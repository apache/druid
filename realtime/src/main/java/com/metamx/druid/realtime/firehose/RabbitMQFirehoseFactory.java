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

    boolean durable = Boolean.valueOf(consumerProps.getProperty("durable", "false"));
    boolean exclusive = Boolean.valueOf(consumerProps.getProperty("exclusive", "false"));
    boolean autoDelete = Boolean.valueOf(consumerProps.getProperty("autoDelete", "false"));
    boolean autoAck = Boolean.valueOf(consumerProps.getProperty("autoAck", "true"));

    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();
    channel.queueDeclare(queue, durable, exclusive, autoDelete, null);
    channel.queueBind(queue, exchange, routingKey);
    final QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, autoAck, consumer);
    channel.addShutdownListener(new ShutdownListener() {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause) {
        log.warn(cause, "Channel closed!");
        //TODO: should we re-establish the connection here?
      }
    });
    connection.addShutdownListener(new ShutdownListener() {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause) {
        log.warn(cause, "Connection closed!");
        //TODO: should we re-establish the connection here?
      }
    });

    return new Firehose(){

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
        delivery = null;
        try {
          delivery = consumer.nextDelivery();
          lastDeliveryTag = delivery.getEnvelope().getDeliveryTag();

          log.debug("got new message");
        } catch (InterruptedException e) {
          //TODO: I'm not exactly sure how we should react to this.
          // Does it mean that delivery will be null and we should handle that
          // as if there are no more messages (return false)?
          log.wtf(e, "Don't know if this is supposed to ever happen.");
        }

        if(delivery != null){
          // If delivery is non-null, we report that there is something more to process.
          return true;
        }

        // This means that delivery is null so we have nothing more to process.
        return false;
      }

      @Override
      public InputRow nextRow() {
        if(delivery == null){
          //Just making sure.
          log.wtf("I have nothing in delivery. Method hasMore() should have returned false.");
          return null;
        }

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
        log.info("Closing connection to RabbitMQ");
        channel.close();
        connection.close();
      }
    };
  }
}
