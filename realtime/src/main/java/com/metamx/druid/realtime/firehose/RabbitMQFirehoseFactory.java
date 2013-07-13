package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.input.InputRow;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.*;

/**
 * A FirehoseFactory
 */
public class RabbitMQFirehoseFactory implements FirehoseFactory{

  private static final Logger log = new Logger(RabbitMQFirehoseFactory.class);

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final String queue;

  @JsonProperty
  private final String exchange;

  @JsonProperty
  private final String routingKey;

  @JsonProperty
  private final StringInputRowParser parser;

  @JsonCreator
  public RabbitMQFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("queue") String queue,
      @JsonProperty("exchange") String exchange,
      @JsonProperty("routingKey") String routingKey,
      @JsonProperty("parser") StringInputRowParser parser
  )
  {
    this.consumerProps = consumerProps;
    this.queue = queue;
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.parser = parser;

    parser.addDimensionExclusion("queue");
    parser.addDimensionExclusion("exchange");
    parser.addDimensionExclusion("routingKey");
  }

  @Override
  public Firehose connect() throws IOException {

    final ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(consumerProps.getProperty("host", factory.getHost()));
    factory.setUsername(consumerProps.getProperty("username", factory.getUsername()));
    factory.setPassword(consumerProps.getProperty("password", factory.getPassword()));
    factory.setVirtualHost(consumerProps.getProperty("virtualHost", factory.getVirtualHost()));

    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();
    channel.queueDeclare(queue, true, false, false, null);
    channel.queueBind(queue, exchange, routingKey);
    final QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, false, consumer);

    return new Firehose(){

      //private final Connection connection = conn;
      //private final Channel channel = ch;
      //private final QueueingConsumer consumer = qc;

      /**
       * Storing the latest delivery as a member variable should be safe since this will only be run
       * by a single thread.
       */
      private QueueingConsumer.Delivery delivery;

      /**
       * Store the latest delivery tag to be able to commit (acknowledge) the message delivery up to
       * and including this tag. See commit() for more detail.
       */
      private long lastDeliveryTag;

      @Override
      public boolean hasMore() {
        try {
          delivery = consumer.nextDelivery();
          lastDeliveryTag = delivery.getEnvelope().getDeliveryTag();

          log.debug("got new message");
        } catch (InterruptedException e) {
          log.wtf(e, "Don't know if this is supposed to ever happen.");
          return false;
        }

        if(delivery != null){
          return true;
        }

        // Shouldn't ever get here but in case we'll assume there is no more stuff.
        return false;
      }

      @Override
      public InputRow nextRow() {
        log.debug("consuming new message");

        return parser.parse(new String(delivery.getBody()));
      }

      @Override
      public Runnable commit() {

        // This method will be called from the same thread that calls the other methods of
        // this Firehose. However, the returned Runnable will be called by a different thread.
        //
        // It should be (thread) safe to copy the lastDeliveryTag like we do below.
        return new Runnable()
        {
          // Store (copy) the last delivery tag to "become" thread safe.
          final long deliveryTag = lastDeliveryTag;

          @Override
          public void run()
          {
            try {
              log.info("Acknowledging delivery of messages up to tag: " + deliveryTag);

              // Acknowledge all messages up to and including the stored delivery tag.
              channel.basicAck(deliveryTag, true);
            } catch (IOException e) {
              log.error(e, "Unable to acknowledge message reception to message queue.");
            }
          }
        };
      }

      @Override
      public void close() throws IOException {
        channel.close();
        connection.close();
      }
    };
  }
}
