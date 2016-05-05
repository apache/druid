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

package io.druid.server.namespace;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.KafkaExtractionNamespace;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

/**
 *
 */
public class KafkaExtractionManager
{
  private static final Logger log = new Logger(KafkaExtractionManager.class);

  private final Properties kafkaProperties = new Properties();
  private final ConcurrentMap<String, String> namespaceVersionMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicLong> topicEvents = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ListenableFuture<?>> futures = new ConcurrentHashMap<>();
  private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("kafka-rename-consumer-%d")
              .setDaemon(true)
              .setPriority(Thread.MIN_PRIORITY)
              .build()
      )
  );
  private final AtomicInteger backgroundTaskCount = new AtomicInteger(0);

  // Bindings in KafkaExtractionNamespaceModule
  @Inject
  public KafkaExtractionManager(
      final Properties kafkaProperties
  )
  {
    if (kafkaProperties.containsKey("group.id")) {
      throw new IAE(
          "Cannot set kafka property [group.id]. Property is randomly generated for you. Found [%s]",
          kafkaProperties.getProperty("group.id")
      );
    }
    if (kafkaProperties.containsKey("auto.offset.reset")) {
      throw new IAE(
          "Cannot set kafka property [auto.offset.reset]. Property will be forced to [smallest]. Found [%s]",
          kafkaProperties.getProperty("auto.offset.reset")
      );
    }
    this.kafkaProperties.putAll(kafkaProperties);
    if (!this.kafkaProperties.containsKey("zookeeper.connect")) {
      this.kafkaProperties.put("zookeeper.connect", "localhost:2181/kafka");
    }
    // Enable publish-subscribe
    this.kafkaProperties.setProperty("auto.offset.reset", "smallest");
  }

  boolean supports(Properties otherProperties)
  {
    if (otherProperties.containsKey("zookeeper.connect")) {
      return this.kafkaProperties.getProperty("zookeeper.connect")
                                 .equals(otherProperties.getProperty("zookeeper.connect"));
    }
    return false;
  }

  public long getBackgroundTaskCount()
  {
    return backgroundTaskCount.get();
  }

  private static final Decoder<String> defaultStringDecoder = new Decoder<String>()
  {
    @Override
    public String fromBytes(byte[] bytes)
    {
      return StringUtils.fromUtf8(bytes);
    }
  };

  public long getNumEvents(String namespace)
  {
    if (namespace == null) {
      return 0L;
    } else {
      final AtomicLong eventCounter = topicEvents.get(namespace);
      if (eventCounter != null) {
        return eventCounter.get();
      } else {
        return 0L;
      }
    }
  }

  public void addListener(final String id, final KafkaExtractionNamespace kafkaNamespace, final Map<String, String> map)
  {
    final String topic = kafkaNamespace.getKafkaTopic();
    final ListenableFuture<?> future = executorService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            final Properties privateProperties = new Properties();
            privateProperties.putAll(kafkaProperties);
            privateProperties.setProperty("group.id", UUID.randomUUID().toString());
            ConsumerConnector consumerConnector = new kafka.javaapi.consumer.ZookeeperConsumerConnector(
                new ConsumerConfig(
                    privateProperties
                )
            );
            List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreamsByFilter(
                new Whitelist(Pattern.quote(topic)), 1, defaultStringDecoder, defaultStringDecoder
            );

            if (streams == null || streams.isEmpty()) {
              throw new IAE("Topic [%s] had no streams", topic);
            }
            if (streams.size() > 1) {
              throw new ISE("Topic [%s] has %d streams! expected 1", topic, streams.size());
            }
            backgroundTaskCount.incrementAndGet();
            final KafkaStream<String, String> kafkaStream = streams.get(0);
            final ConsumerIterator<String, String> it = kafkaStream.iterator();
            log.info("Listening to topic [%s] for namespace [%s]", topic, id);
            AtomicLong eventCounter = topicEvents.get(id);
            if (eventCounter == null) {
              topicEvents.putIfAbsent(id, new AtomicLong(0L));
              eventCounter = topicEvents.get(id);
            }
            while (it.hasNext()) {
              final MessageAndMetadata<String, String> messageAndMetadata = it.next();
              final String key = messageAndMetadata.key();
              final String message = messageAndMetadata.message();
              if (key == null || message == null) {
                log.error("Bad key/message from topic [%s]: [%s]", topic, messageAndMetadata);
                continue;
              }
              map.put(key, message);
              namespaceVersionMap.put(id, Long.toString(eventCounter.incrementAndGet()));
              log.debug("Placed key[%s] val[%s]", key, message);
            }
          }
        }
    );
    futures.putIfAbsent(id, future);
    Futures.addCallback(
        future, new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            topicEvents.remove(id);
            futures.remove(id);
          }

          @Override
          public void onFailure(Throwable t)
          {
            topicEvents.remove(id);
            futures.remove(id);
            if (t instanceof java.util.concurrent.CancellationException) {
              log.warn("Cancelled rename task for topic [%s]", topic);
            } else {
              Throwables.propagate(t);
            }
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }

  public void removeListener(final String id)
  {
    Future future = futures.remove(id);
    future.cancel(true);
  }

  public void start()
  {
    // NO-OP
    // all consumers are started through KafkaExtractionNamespaceFactory.getCachePopulator
  }

  public void stop()
  {
    executorService.shutdown();
    Futures.allAsList(futures.values()).cancel(true);
  }
}
