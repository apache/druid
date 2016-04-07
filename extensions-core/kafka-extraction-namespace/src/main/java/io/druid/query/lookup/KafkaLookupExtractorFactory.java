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
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

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

  private final Object startStopLock = new Object();
  private final ListeningExecutorService executorService;
  private final AtomicLong doubleEventCount = new AtomicLong(0L);
  private final NamespaceExtractionCacheManager cacheManager;
  private final String factoryId = UUID.randomUUID().toString();
  private final AtomicReference<Map<String, String>> mapRef = new AtomicReference<>(null);

  private AtomicBoolean started = new AtomicBoolean(false);
  private ListenableFuture<?> future = null;
  private ConsumerConnector consumerConnector;

  @JsonProperty
  private final String kafkaTopic;

  @JsonProperty
  private final Map<String, String> kafkaProperties;

  @JsonCreator
  public KafkaLookupExtractorFactory(
      @JacksonInject NamespaceExtractionCacheManager cacheManager,
      @NotNull @JsonProperty(value = "kafkaTopic", required = true) final String kafkaTopic,
      @NotNull @JsonProperty(value = "kafkaProperties", required = true) final Map<String, String> kafkaProperties
  )
  {
    this.kafkaTopic = Preconditions.checkNotNull(kafkaTopic, "kafkaTopic required");
    this.kafkaProperties = Preconditions.checkNotNull(kafkaProperties, "kafkaProperties required");
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(
        "kafka-factory-" + kafkaTopic + "-%s",
        Thread.MIN_PRIORITY
    ));
    this.cacheManager = cacheManager;
  }

  public String getKafkaTopic()
  {
    return kafkaTopic;
  }

  public Map<String, String> getKafkaProperties()
  {
    return kafkaProperties;
  }

  @Override
  public boolean start()
  {
    synchronized (startStopLock) {
      if (started.get()) {
        LOG.warn("Already started, not starting again");
        return false;
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

      consumerConnector = buildConnector(kafkaProperties);

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
      final ConsumerIterator<String, String> it = kafkaStream.iterator();

      final ListenableFuture<?> future = executorService.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              while (it.hasNext()) {
                final MessageAndMetadata<String, String> messageAndMetadata = it.next();
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

  ;

  @Override
  public boolean close()
  {
    synchronized (startStopLock) {
      if (!started.get() || executorService.isShutdown()) {
        LOG.info("Already shutdown, ignoring");
        return false;
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
      if (consumerConnector != null) {
        consumerConnector.shutdown();
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

    if (other == null || getClass() != other.getClass()) {
      return true;
    }

    final KafkaLookupExtractorFactory that = (KafkaLookupExtractorFactory) other;

    return !(getKafkaTopic().equals(that.getKafkaTopic()) && getKafkaProperties().equals(that.getKafkaProperties()));
  }

  @Override
  public LookupExtractor get()
  {
    final Map<String, String> map = Preconditions.checkNotNull(mapRef.get(), "Not started");
    final long startCount = doubleEventCount.get();
    return new MapLookupExtractor(map, false)
    {
      @Override
      public byte[] getCacheKey()
      {
        final byte[] idutf8 = StringUtils.toUtf8(factoryId);
        if (startCount == doubleEventCount.get()) {
          return ByteBuffer
              .allocate(idutf8.length + 1 + Longs.BYTES)
              .put(idutf8)
              .put((byte) 0xFF)
              .putLong(startCount)
              .array();
        } else {
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
}
