/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.rabbitmq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.retry.RetryPolicy;
import net.jodah.lyra.util.Duration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
 *       "autoDelete": "false",               # Whether the queue should auto-delete on disconnect. Default: 'false'
 *
 *       "maxRetries": "10",                  # The max number of reconnection retry attempts
 *       "retryIntervalSeconds": "1",         # The reconnection interval
 *       "maxDurationSeconds": "300"          # The max duration of trying to reconnect
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
public class RabbitMQFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(RabbitMQFirehoseFactory.class);

  private final RabbitMQFirehoseConfig config;
  private final JacksonifiedConnectionFactory connectionFactory;

  @JsonCreator
  public RabbitMQFirehoseFactory(
      @JsonProperty("connection") JacksonifiedConnectionFactory connectionFactory,
      @JsonProperty("config") RabbitMQFirehoseConfig config,
      // See https://github.com/druid-io/druid/pull/1922
      @JsonProperty("connectionFactory") JacksonifiedConnectionFactory connectionFactoryCOMPAT
  ) throws Exception
  {
    this.connectionFactory = connectionFactory == null
                             ? connectionFactoryCOMPAT == null ? JacksonifiedConnectionFactory.makeDefaultConnectionFactory() : connectionFactoryCOMPAT
                             : connectionFactory;
    this.config = config == null ? RabbitMQFirehoseConfig.makeDefaultConfig() : config;

  }

  @JsonProperty
  public RabbitMQFirehoseConfig getConfig()
  {
    return config;
  }

  @JsonProperty("connection")
  public JacksonifiedConnectionFactory getConnectionFactory()
  {
    return connectionFactory;
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser, File temporaryDirectory) throws IOException
  {
    ConnectionOptions lyraOptions = new ConnectionOptions(this.connectionFactory);
    Config lyraConfig = new Config()
        .withRecoveryPolicy(
            new RetryPolicy()
                .withMaxRetries(config.getMaxRetries())
                .withRetryInterval(Duration.seconds(config.getRetryIntervalSeconds()))
                .withMaxDuration(Duration.seconds(config.getMaxDurationSeconds()))
        );

    String queue = config.getQueue();
    String exchange = config.getExchange();
    String routingKey = config.getRoutingKey();

    boolean durable = config.isDurable();
    boolean exclusive = config.isExclusive();
    boolean autoDelete = config.isAutoDelete();

    final Connection connection = Connections.create(lyraOptions, lyraConfig);

    connection.addShutdownListener(
        new ShutdownListener()
        {
          @Override
          public void shutdownCompleted(ShutdownSignalException cause)
          {
            log.warn(cause, "Connection closed!");
          }
        }
    );

    final Channel channel = connection.createChannel();
    channel.queueDeclare(queue, durable, exclusive, autoDelete, null);
    channel.queueBind(queue, exchange, routingKey);
    channel.addShutdownListener(
        new ShutdownListener()
        {
          @Override
          public void shutdownCompleted(ShutdownSignalException cause)
          {
            log.warn(cause, "Channel closed!");
          }
        }
    );

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
      private Delivery delivery;

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

        return firehoseParser.parse(ByteBuffer.wrap(delivery.getBody()));
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

  private static class QueueingConsumer extends DefaultConsumer
  {
    private final BlockingQueue<Delivery> _queue;

    public QueueingConsumer(Channel ch)
    {
      this(ch, new LinkedBlockingQueue<Delivery>());
    }

    public QueueingConsumer(Channel ch, BlockingQueue<Delivery> q)
    {
      super(ch);
      this._queue = q;
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig)
    {
      _queue.clear();
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException
    {
      _queue.clear();
    }

    @Override
    public void handleDelivery(
        String consumerTag,
        Envelope envelope,
        AMQP.BasicProperties properties,
        byte[] body
    )
        throws IOException
    {
      this._queue.add(new Delivery(envelope, properties, body));
    }

    public Delivery nextDelivery()
        throws InterruptedException, ShutdownSignalException, ConsumerCancelledException
    {
      return _queue.take();
    }
  }
}
