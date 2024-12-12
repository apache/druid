/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@JsonTypeName("kafka")
public class KafkaLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger LOG = new Logger(KafkaLookupExtractorFactory.class);
  private final ListeningExecutorService executorService;
  private final AtomicLong doubleEventCount = new AtomicLong(0L);
  private final NamespaceExtractionCacheManager cacheManager;
  private final String factoryId;
  private final AtomicReference<Map<String, String>> mapRef = new AtomicReference<>(null);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private CacheHandler cacheHandler;

  private volatile ListenableFuture<?> future = null;

  @JsonProperty
  private final String kafkaTopic;

  @JsonProperty
  private final Map<String, String> kafkaProperties;

  @JsonProperty
  private final long connectTimeout;

  @JsonProperty
  private final boolean injective;

  @JsonCreator
  public KafkaLookupExtractorFactory(
      @JacksonInject NamespaceExtractionCacheManager cacheManager,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("kafkaProperties") final Map<String, String> kafkaProperties,
      @JsonProperty("connectTimeout") @Min(0) long connectTimeout,
      @JsonProperty("injective") boolean injective
  )
  {
    this.kafkaTopic = Preconditions.checkNotNull(kafkaTopic, "kafkaTopic required");
    this.kafkaProperties = Preconditions.checkNotNull(kafkaProperties, "kafkaProperties required");
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(
        "kafka-factory-" + StringUtils.encodeForFormat(kafkaTopic) + "-%s",
        Thread.MIN_PRIORITY
    ));
    this.cacheManager = cacheManager;
    this.connectTimeout = connectTimeout;
    this.injective = injective;
    this.factoryId = "kafka-factory-" + kafkaTopic + UUID.randomUUID();
  }

  public KafkaLookupExtractorFactory(
      NamespaceExtractionCacheManager cacheManager,
      String kafkaTopic,
      Map<String, String> kafkaProperties
  )
  {
    this(cacheManager, kafkaTopic, kafkaProperties, 0, false);
  }

  public String getKafkaTopic()
  {
    return kafkaTopic;
  }

  public Map<String, String> getKafkaProperties()
  {
    return kafkaProperties;
  }

  public long getConnectTimeout()
  {
    return connectTimeout;
  }

  public boolean isInjective()
  {
    return injective;
  }

  @Override
  public boolean start()
  {
    synchronized (started) {
      if (started.get()) {
        LOG.warn("Already started, not starting again");
        return true;
      }
      if (executorService.isShutdown()) {
        LOG.warn("Already shut down, not starting again");
        return false;
      }
      verifyKafkaProperties();

      final String topic = getKafkaTopic();
      LOG.debug("About to listen to topic [%s] with group.id [%s]", topic, factoryId);
      // this creates a ConcurrentMap
      cacheHandler = cacheManager.createCache();
      final Map<String, String> map = cacheHandler.getCache();
      mapRef.set(map);


      final CountDownLatch startingReads = new CountDownLatch(1);

      final ListenableFuture<?> future = executorService.submit(() -> {
        final Consumer<String, String> consumer = getConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        try {
          while (!executorService.isShutdown()) {
            try {
              if (executorService.isShutdown()) {
                break;
              }
              final ConsumerRecords<String, String> records = consumer.poll(1000);
              startingReads.countDown();

              for (final ConsumerRecord<String, String> record : records) {
                final String key = record.key();
                final String message = record.value();
                if (key == null) {
                  LOG.error("Bad key from topic [%s]: [%s]", topic, record);
                  continue;
                }
                if (message == null) {
                  LOG.trace("Removed key[%s] val[%s]", key, message);
                  doubleEventCount.incrementAndGet();
                  map.remove(key);
                  doubleEventCount.incrementAndGet();
                  continue;
                }
                doubleEventCount.incrementAndGet();
                map.put(key, message);
                doubleEventCount.incrementAndGet();
                LOG.trace("Placed key[%s] val[%s]", key, message);
              }
            }
            catch (Exception e) {
              LOG.error(e, "Error reading stream for topic [%s]", topic);
            }
          }
        }
        finally {
          consumer.close();
        }
      });
      Futures.addCallback(
          future,
          new FutureCallback<Object>()
          {
            @Override
            public void onSuccess(Object result)
            {
              LOG.debug("Success listening to [%s]", topic);
            }

            @Override
            public void onFailure(Throwable t)
            {
              if (t instanceof CancellationException) {
                LOG.debug("Topic [%s] cancelled", topic);
              } else {
                LOG.error(t, "Error in listening to [%s]", topic);
              }
            }
          },
          Execs.directExecutor()
      );
      this.future = future;
      final Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        while (!startingReads.await(100, TimeUnit.MILLISECONDS) && connectTimeout > 0L) {
          // Don't return until we have actually connected
          if (future.isDone()) {
            future.get();
          } else {
            if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > connectTimeout) {
              throw new TimeoutException("Failed to connect to kafka in sufficient time");
            }
          }
        }
      }
      catch (InterruptedException | ExecutionException | TimeoutException e) {
        executorService.shutdown();
        future.cancel(true);
        LOG.error(e, "Failed to start kafka extraction factory");
        cacheHandler.close();
        return false;
      }

      started.set(true);
      return true;
    }
  }

  @Override
  public boolean close()
  {
    synchronized (started) {
      if (!started.get() || executorService.isShutdown()) {
        LOG.info("Already shutdown, ignoring");
        return !started.get();
      }
      started.set(false);
      executorService.shutdown();

      final ListenableFuture<?> future = this.future;
      if (future != null) {
        future.cancel(true);
      }
      cacheHandler.close();
      return true;
    }
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    if (this == other) {
      return false;
    }

    if (other == null || getClass() != other.getClass()) {
      return true;
    }

    final KafkaLookupExtractorFactory that = (KafkaLookupExtractorFactory) other;

    return !(getKafkaTopic().equals(that.getKafkaTopic())
             && getKafkaProperties().equals(that.getKafkaProperties())
             && getConnectTimeout() == that.getConnectTimeout()
             && isInjective() == that.isInjective());
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return new KafkaLookupExtractorIntrospectionHandler(this);
  }

  @Override
  public void awaitInitialization()
  {
    // Kafka lookup do not need await on initialization as it is realtime kafka lookups.
  }

  @Override
  public boolean isInitialized()
  {
    return true;
  }

  @Override
  public LookupExtractor get()
  {
    final Map<String, String> map = Preconditions.checkNotNull(mapRef.get(), "Not started");
    final long startCount = doubleEventCount.get();
    return new MapLookupExtractor(map, isInjective())
    {
      @Override
      public byte[] getCacheKey()
      {
        final byte[] idutf8 = StringUtils.toUtf8(factoryId);
        // If the number of things added has not changed during the course of this extractor's life, we can cache it
        if (startCount == doubleEventCount.get()) {
          return ByteBuffer
              .allocate(idutf8.length + 1 + Long.BYTES)
              .put(idutf8)
              .put((byte) 0xFF)
              .putLong(startCount)
              .array();
        } else {
          // If the number of things added HAS changed during the course of this extractor's life, we CANNOT cache
          final byte[] scrambler = StringUtils.toUtf8(UUID.randomUUID().toString());
          return ByteBuffer
              .allocate(idutf8.length + 1 + scrambler.length + 1)
              .put(idutf8)
              .put((byte) 0xFF)
              .put(scrambler)
              .put((byte) 0xFF)
              .array();
        }
      }
    };
  }

  public long getCompletedEventCount()
  {
    return doubleEventCount.get() >> 1;
  }

  // Used in tests
  NamespaceExtractionCacheManager getCacheManager()
  {
    return cacheManager;
  }

  AtomicReference<Map<String, String>> getMapRef()
  {
    return mapRef;
  }

  AtomicLong getDoubleEventCount()
  {
    return doubleEventCount;
  }

  ListenableFuture<?> getFuture()
  {
    return future;
  }

  /**
   * Check that the user has not set forbidden Kafka consumer props
   *
   * Some consumer properties must be set in order to guarantee that
   * the consumer will consume the entire topic from the beginning.
   * Otherwise, lookup data may not be loaded completely.
   */
  private void verifyKafkaProperties()
  {
    if (kafkaProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      throw new IAE(
              "Cannot set kafka property [group.id]. Property is randomly generated for you. Found [%s]",
              kafkaProperties.get(ConsumerConfig.GROUP_ID_CONFIG)
      );
    }
    if (kafkaProperties.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      throw new IAE(
              "Cannot set kafka property [auto.offset.reset]. Property will be forced to [earliest]. Found [%s]",
              kafkaProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      );
    }
    if (kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) &&
          !kafkaProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equals("false")) {
      throw new IAE(
          "Cannot set kafka property [enable.auto.commit]. Property will be forced to [false]. Found [%s]",
          kafkaProperties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
      );
    }
    Preconditions.checkNotNull(
            kafkaProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
            "bootstrap.servers required property"
    );
  }

  // Overridden in tests
  Consumer<String, String> getConsumer()
  {
    // Workaround for Kafka String Serializer could not be found
    // Adopted from org.apache.druid.indexing.kafka.KafkaRecordSupplier#getKafkaConsumer
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    final Properties properties = getConsumerProperties();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Nonnull
  private Properties getConsumerProperties()
  {
    final Properties properties = new Properties();
    properties.putAll(kafkaProperties);
    // Set the consumer to consume everything and never commit offsets
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, factoryId);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return properties;
  }
}
