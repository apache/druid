/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import java.nio.ByteBuffer;
import java.util.List;
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
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

@JsonTypeName("kafka")
public class KafkaLookupExtractorFactory implements LookupExtractorFactory
{
  private static final Logger LOG = new Logger(KafkaLookupExtractorFactory.class);
  static final Decoder<String> DEFAULT_STRING_DECODER = new Decoder<String>()
  {
    @Override
    public String fromBytes(byte[] bytes)
    {
      return StringUtils.fromUtf8(bytes);
    }
  };

  private final ListeningExecutorService executorService;
  private final AtomicLong doubleEventCount = new AtomicLong(0L);
  private final NamespaceExtractionCacheManager cacheManager;
  private final String factoryId = UUID.randomUUID().toString();
  private final AtomicReference<Map<String, String>> mapRef = new AtomicReference<>(null);
  private final AtomicBoolean started = new AtomicBoolean(false);

  private volatile ListenableFuture<?> future = null;

  @JsonProperty
  private final String kafkaTopic;

  @JsonProperty
  private final Map<String, String> kafkaProperties;

  @JsonProperty
  private final long connectTimeout;

  @JsonProperty
  private final boolean isOneToOne;

  @JsonCreator
  public KafkaLookupExtractorFactory(
      @JacksonInject NamespaceExtractionCacheManager cacheManager,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("kafkaProperties") final Map<String, String> kafkaProperties,
      @JsonProperty("connectTimeout") @Min(0) long connectTimeout,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.kafkaTopic = Preconditions.checkNotNull(kafkaTopic, "kafkaTopic required");
    this.kafkaProperties = Preconditions.checkNotNull(kafkaProperties, "kafkaProperties required");
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(
        "kafka-factory-" + kafkaTopic + "-%s",
        Thread.MIN_PRIORITY
    ));
    this.cacheManager = cacheManager;
    this.connectTimeout = connectTimeout;
    this.isOneToOne = isOneToOne;
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

  public boolean isOneToOne()
  {
    return isOneToOne;
  }

  @Override
  public boolean start()
  {
    synchronized (started) {
      if (started.get()) {
        LOG.warn("Already started, not starting again");
        return started.get();
      }
      if (executorService.isShutdown()) {
        LOG.warn("Already shut down, not starting again");
        return false;
      }
      final Properties kafkaProperties = new Properties();
      kafkaProperties.putAll(getKafkaProperties());
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
      Preconditions.checkNotNull(
          kafkaProperties.getProperty("zookeeper.connect"),
          "zookeeper.connect required property"
      );

      kafkaProperties.setProperty("group.id", factoryId);
      final String topic = getKafkaTopic();
      LOG.debug("About to listen to topic [%s] with group.id [%s]", topic, factoryId);
      final Map<String, String> map = cacheManager.getCacheMap(factoryId);
      mapRef.set(map);
      // Enable publish-subscribe
      kafkaProperties.setProperty("auto.offset.reset", "smallest");

      final CountDownLatch startingReads = new CountDownLatch(1);

      final ListenableFuture<?> future = executorService.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              while (!executorService.isShutdown() && !Thread.currentThread().isInterrupted()) {
                final ConsumerConnector consumerConnector = buildConnector(kafkaProperties);
                try {
                  final List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreamsByFilter(
                      new Whitelist(Pattern.quote(topic)), 1, DEFAULT_STRING_DECODER, DEFAULT_STRING_DECODER
                  );

                  if (streams == null || streams.isEmpty()) {
                    throw new IAE("Topic [%s] had no streams", topic);
                  }
                  if (streams.size() > 1) {
                    throw new ISE("Topic [%s] has %d streams! expected 1", topic, streams.size());
                  }
                  final KafkaStream<String, String> kafkaStream = streams.get(0);

                  startingReads.countDown();

                  for (final MessageAndMetadata<String, String> messageAndMetadata : kafkaStream) {
                    final String key = messageAndMetadata.key();
                    final String message = messageAndMetadata.message();
                    if (key == null || message == null) {
                      LOG.error("Bad key/message from topic [%s]: [%s]", topic, messageAndMetadata);
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
                finally {
                  consumerConnector.shutdown();
                }
              }
            }
          }
      );
      Futures.addCallback(
          future, new FutureCallback<Object>()
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
          MoreExecutors.sameThreadExecutor()
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
        if (!future.isDone() && !future.cancel(true) && !future.isDone()) {
          LOG.warn("Could not cancel kafka listening thread");
        }
        LOG.error(e, "Failed to start kafka extraction factory");
        cacheManager.delete(factoryId);
        return false;
      }

      started.set(true);
      return true;
    }
  }

  // Overriden in tests
  ConsumerConnector buildConnector(Properties properties)
  {
    return new kafka.javaapi.consumer.ZookeeperConsumerConnector(
        new ConsumerConfig(properties)
    );
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
      executorService.shutdownNow();
      final ListenableFuture<?> future = this.future;
      if (future != null) {
        if (!future.isDone() && !future.cancel(true) && !future.isDone()) {
          LOG.error("Error cancelling future for topic [%s]", getKafkaTopic());
          return false;
        }
      }
      if (!cacheManager.delete(factoryId)) {
        LOG.error("Error removing [%s] for topic [%s] from cache", factoryId, getKafkaTopic());
        return false;
      }
      return true;
    }
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    if (this == other) {
      return false;
    }

    if (other == null) {
      return false;
    }

    if (getClass() != other.getClass()) {
      return true;
    }

    final KafkaLookupExtractorFactory that = (KafkaLookupExtractorFactory) other;

    return !(getKafkaTopic().equals(that.getKafkaTopic())
             && getKafkaProperties().equals(that.getKafkaProperties())
             && getConnectTimeout() == that.getConnectTimeout()
             && isOneToOne() == that.isOneToOne()
    );
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return new KafkaLookupExtractorIntrospectionHandler();
  }

  @Override
  public LookupExtractor get()
  {
    final Map<String, String> map = Preconditions.checkNotNull(mapRef.get(), "Not started");
    final long startCount = doubleEventCount.get();
    return new MapLookupExtractor(map, isOneToOne())
    {
      @Override
      public byte[] getCacheKey()
      {
        final byte[] idutf8 = StringUtils.toUtf8(factoryId);
        // If the number of things added has not changed during the course of this extractor's life, we can cache it
        if (startCount == doubleEventCount.get()) {
          return ByteBuffer
              .allocate(idutf8.length + 1 + Longs.BYTES)
              .put(idutf8)
              .put((byte) 0xFF)
              .putLong(startCount)
              .array();
        } else {
          // If the number of things added HAS changed during the coruse of this extractor's life, we CANNOT cache
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


  class KafkaLookupExtractorIntrospectionHandler implements LookupIntrospectHandler
  {
    @GET
    public Response getActive()
    {
      final ListenableFuture<?> future = getFuture();
      if (future != null && !future.isDone()) {
        return Response.ok().build();
      } else {
        return Response.status(Response.Status.GONE).build();
      }
    }
  }
}
