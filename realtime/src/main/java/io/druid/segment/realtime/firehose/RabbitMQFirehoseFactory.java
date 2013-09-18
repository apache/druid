/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.StringInputRowParser;

import java.io.IOException;

/**
 * A FirehoseFactory for RabbitMQ.
 * <p/>
 * It will receive it's configuration through the realtime.spec file and expects to find a
 * consumerProps element in the firehose definition with values for a number of configuration options.
 * Below is a complete example for a RabbitMQ firehose configuration with some explanation. Options
 * that have defaults can be skipped but options with no defaults must be specified with the exception
 * of the URI property. If the URI property is set, it will override any other property that was also
 * set.
 * <p/>
 * File: <em>realtime.spec</em>
 * <pre>
 *   "firehose" : {
 *     "type" : "rabbitmq",
 *     "connection" : {
  *       "host": "localhost",                 # The hostname of the RabbitMQ broker to connect to. Default: 'localhost'
  *       "port": "5672",                      # The port number to connect to on the RabbitMQ broker. Default: '5672'
  *       "username": "test-dude",             # The username to use to connect to RabbitMQ. Default: 'guest'
  *       "password": "test-word",             # The password to use to connect to RabbitMQ. Default: 'guest'
  *       "virtualHost": "test-vhost",         # The virtual host to connect to. Default: '/'
  *       "uri": "amqp://mqserver:1234/vhost", # The URI string to use to connect to RabbitMQ. No default and not needed
  *     },
  *     "config" : {
  *       "exchange": "test-exchange",         # The exchange to connect to. No default
  *       "queue" : "druidtest",               # The queue to connect to or create. No default
  *       "routingKey": "#",                   # The routing key to use to bind the queue to the exchange. No default
  *       "durable": "true",                   # Whether the queue should be durable. Default: 'false'
  *       "exclusive": "false",                # Whether the queue should be exclusive. Default: 'false'
  *       "autoDelete": "false"                # Whether the queue should auto-delete on disconnect. Default: 'false'
  *     },
 *     "parser" : {
 *       "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
 *       "data" : { "format" : "json" },
 *       "dimensionExclusions" : ["wp"]
 *     }
 *   },
 * </pre>
 * <p/>
 * <b>Limitations:</b> This implementation will not attempt to reconnect to the MQ broker if the
 * connection to it is lost. Furthermore it does not support any automatic failover on high availability
 * RabbitMQ clusters. This is not supported by the underlying AMQP client library and while the behavior
 * could be "faked" to some extent we haven't implemented that yet. However, if a policy is defined in
 * the RabbitMQ cluster that sets the "ha-mode" and "ha-sync-mode" properly on the queue that this
 * Firehose connects to, messages should survive an MQ broker node failure and be delivered once a
 * connection to another node is set up.
 * <p/>
 * For more information on RabbitMQ high availability please see:
 * <a href="http://www.rabbitmq.com/ha.html">http://www.rabbitmq.com/ha.html</a>.
 */
public class RabbitMQFirehoseFactory implements FirehoseFactory
{
  private static final Logger log = new Logger(RabbitMQFirehoseFactory.class);

  @JsonProperty
  private final RabbitMQFirehoseConfig config;

  @JsonProperty
  private final StringInputRowParser parser;

  @JsonProperty
  private final ConnectionFactory connectionFactory;

  @JsonCreator
  public RabbitMQFirehoseFactory(
      @JsonProperty("connection") JacksonifiedConnectionFactory connectionFactory,
      @JsonProperty("config") RabbitMQFirehoseConfig config,
      @JsonProperty("parser") StringInputRowParser parser
  )
  {
    this.connectionFactory = connectionFactory;
    this.config = config;
    this.parser = parser;
  }

  @Override
  public Firehose connect() throws IOException
  {
    String queue = config.getQueue();
    String exchange = config.getExchange();
    String routingKey = config.getRoutingKey();

    boolean durable = config.isDurable();
    boolean exclusive = config.isExclusive();
    boolean autoDelete = config.isAutoDelete();

    final Connection connection = connectionFactory.newConnection();
    connection.addShutdownListener(new ShutdownListener()
    {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause)
      {
        log.warn(cause, "Connection closed!");
        //FUTURE: we could try to re-establish the connection here. Not done in this version though.
      }
    });

    final Channel channel = connection.createChannel();
    channel.queueDeclare(queue, durable, exclusive, autoDelete, null);
    channel.queueBind(queue, exchange, routingKey);
    channel.addShutdownListener(new ShutdownListener()
    {
      @Override
      public void shutdownCompleted(ShutdownSignalException cause)
      {
        log.warn(cause, "Channel closed!");
        //FUTURE: we could try to re-establish the connection here. Not done in this version though.
      }
    });

    // We create a QueueingConsumer that will not auto-acknowledge messages since that
    // happens on commit().
    final QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, false, consumer);

    return new Firehose()
    {
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
      public boolean hasMore()
      {
        delivery = null;
        try {
          // Wait for the next delivery. This will block until something is available.
          delivery = consumer.nextDelivery();
          if (delivery != null) {
            lastDeliveryTag = delivery.getEnvelope().getDeliveryTag();
            // If delivery is non-null, we report that there is something more to process.
            return true;
          }
        }
        catch (InterruptedException e) {
          // A little unclear on how we should handle this.

          // At any rate, we're in an unknown state now so let's log something and return false.
          log.wtf(e, "Got interrupted while waiting for next delivery. Doubt this should ever happen.");
        }

        // This means that delivery is null or we caught the exception above so we report that we have
        // nothing more to process.
        return false;
      }

      @Override
      public InputRow nextRow()
      {
        if (delivery == null) {
          //Just making sure.
          log.wtf("I have nothing in delivery. Method hasMore() should have returned false.");
          return null;
        }

        return parser.parse(new String(delivery.getBody()));
      }

      @Override
      public Runnable commit()
      {
        // This method will be called from the same thread that calls the other methods of
        // this Firehose. However, the returned Runnable will be called by a different thread.
        //
        // It should be (thread) safe to copy the lastDeliveryTag like we do below and then
        // acknowledge values up to and including that value.
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
            }
            catch (IOException e) {
              log.error(e, "Unable to acknowledge message reception to message queue.");
            }
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        log.info("Closing connection to RabbitMQ");
        channel.close();
        connection.close();
      }
    };
  }
}
