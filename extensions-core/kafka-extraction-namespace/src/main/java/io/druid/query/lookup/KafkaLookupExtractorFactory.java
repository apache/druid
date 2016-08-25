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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service.State;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  private final ListeningExecutorService executorService;
  private final AtomicLong doubleEventCount = new AtomicLong(0L);
  private final NamespaceExtractionCacheManager cacheManager;
  private final String factoryId = UUID.randomUUID().toString();
  private final ConsumerConnectorFactory consumerConnectorFactory;
  private final AtomicReference<State> currentState = new AtomicReference<>(State.NEW);
  private final CountDownLatch startingReads = new CountDownLatch(1);

  private volatile ListenableFuture<?> future = null;
  private volatile Map<String, String> map = null;

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
    this(cacheManager, kafkaTopic, kafkaProperties, connectTimeout, injective, new ConsumerConnectorFactory.Zookeeper());
  }

  KafkaLookupExtractorFactory(
          NamespaceExtractionCacheManager cacheManager,
          final String kafkaTopic,
          final Map<String, String> kafkaProperties,
          long connectTimeout,
          boolean injective,
          ConsumerConnectorFactory consumerConnectorFactory
  )
  {
    this.kafkaTopic = Preconditions.checkNotNull(kafkaTopic, "kafkaTopic required");
    this.kafkaProperties = Preconditions.checkNotNull(kafkaProperties, "kafkaProperties required");
    this.cacheManager = cacheManager;
    this.connectTimeout = connectTimeout;
    this.injective = injective;
    this.consumerConnectorFactory = consumerConnectorFactory;
    this.executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(
            "kafka-factory-" + kafkaTopic + "-%s",
            Thread.MIN_PRIORITY
    ));
  }

  void init() {
    map = cacheManager.getCacheMap(factoryId);
  }

  @Override
  public boolean start()
  {

    if(!advanceState(State.NEW, State.STARTING)) {
      LOG.warn("Already started, not starting again. Current state: [%s]", currentState.get());
      return currentState.get().equals(State.RUNNING);
    }

    // TODO: if current state is STARTING, block until a terminal state is reached?

    final Properties kafkaProperties = buildProperties();
    LOG.debug("About to listen to topic [%s] with group.id [%s]", kafkaTopic, factoryId);

    init();

    future = executorService.submit(makeRunnable(kafkaProperties));

    if (!awaitStart()) {
      currentState.set(State.FAILED);
      return false;
    }

    advanceState(State.STARTING, State.RUNNING);

    return true;
  }

  private boolean awaitStart() {
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
    return true;
  }

  private Runnable makeRunnable(final Properties kafkaProperties) {
    return new Runnable()
    {
      @Override
      public void run()
      {
        while (!shouldStop()) {
          // TODO: why is a new connector created and destroyed on every iteration?
          final ConsumerConnector consumerConnector = consumerConnectorFactory.buildConnector(kafkaProperties);
          try {
            final List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreamsByFilter(
                new Whitelist(Pattern.quote(kafkaTopic)), 1, DEFAULT_STRING_DECODER, DEFAULT_STRING_DECODER
            );

            if (streams == null || streams.isEmpty()) {
              throw new IAE("Topic [%s] had no streams", kafkaTopic);
            }
            if (streams.size() > 1) {
              throw new ISE("Topic [%s] has %d streams! expected 1", kafkaTopic, streams.size());
            }
            final KafkaStream<String, String> kafkaStream = streams.get(0);

            // FIXME: Why is this done on every iteration?
            startingReads.countDown();

            for (final MessageAndMetadata<String, String> messageAndMetadata : kafkaStream) {
              final String key = messageAndMetadata.key();
              final String message = messageAndMetadata.message();
              if (key == null || message == null) {
                LOG.error("Bad key/message from topic [%s]: [%s]", kafkaTopic, messageAndMetadata);
                continue;
              }
              onNewEntry(key, message);
            }
          }
          catch (Exception e) {
            LOG.error(e, "Error reading stream for topic [%s]", kafkaTopic);
          } finally {
            Thread.interrupted();
            consumerConnector.shutdown();
          }
        }
      }
    };
  }

  void onNewEntry(String key, String message) {
    doubleEventCount.incrementAndGet();
    map.put(key, message);
    doubleEventCount.incrementAndGet();
    LOG.trace("Placed key[%s] val[%s]", key, message);
  }

  private Properties buildProperties() {
    final Properties kafkaProperties = new Properties();
    kafkaProperties.putAll(this.kafkaProperties);
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

    // Enable publish-subscribe
    kafkaProperties.setProperty("auto.offset.reset", "smallest");
    return kafkaProperties;
  }


  @Override
  public boolean close()
  {
    if(!advanceState(State.RUNNING, State.STOPPING)) {
      LOG.info("Already shutdown, ignoring. Current state: [%s]", currentState);
      return currentState.get().equals(State.TERMINATED);
    }

    // TODO: if current state is STOPPING, block until a terminal state is reached?
    // Is this method ever invoked by multiple threads simultaneously?


    executorService.shutdown();

    if (future != null) {
      if (!future.isDone() && !future.cancel(true) && !future.isDone()) {
        LOG.error("Error cancelling future for topic [%s]", kafkaTopic);
        advanceState(State.STOPPING, State.FAILED);
        return false;
      }
    }


    if (!cacheManager.delete(factoryId)) {
      LOG.error("Error removing [%s] for topic [%s] from cache", factoryId, kafkaTopic);
      advanceState(State.STOPPING, State.FAILED);
      return false;
    }

    advanceState(State.STOPPING, State.TERMINATED);

    return true;
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

    return !(kafkaTopic.equals(that.kafkaTopic)
             && kafkaProperties.equals(that.kafkaProperties)
             && connectTimeout == that.connectTimeout
             && injective == that.injective
    );
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return new KafkaLookupExtractorIntrospectionHandler(this);
  }

  @Override
  public LookupExtractor get()
  {
    final Map<String, String> map = Preconditions.checkNotNull(this.map, "Not started");
    final long startCount = doubleEventCount.get();
    return new MapLookupExtractor(map, injective)
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

  private boolean advanceState(State expectedCurrentState, State newState) {
    boolean success = currentState.compareAndSet(expectedCurrentState, newState);

    if(success)
    {
      synchronized (currentState)
      {
        currentState.notifyAll();
      }
    }

    return success;
  }

  private boolean awaitState(State expectedState, long timeout) throws InterruptedException {
    final long waitStart = System.currentTimeMillis();

    while(!currentState.get().equals(expectedState) && System.currentTimeMillis() - waitStart < timeout)
    {
      synchronized (currentState)
      {
        currentState.wait(timeout);
      }
    }

    return currentState.get().equals(expectedState);
  }

  private boolean shouldStop() {
    State state = currentState.get();

    return state.equals(State.STOPPING) || state.equals(State.TERMINATED) || state.equals(State.FAILED);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KafkaLookupExtractorFactory that = (KafkaLookupExtractorFactory) o;

    if (connectTimeout != that.connectTimeout) return false;
    if (injective != that.injective) return false;
    if (!kafkaTopic.equals(that.kafkaTopic)) return false;
    return kafkaProperties.equals(that.kafkaProperties);

  }

  @Override
  public int hashCode() {
    int result = kafkaTopic.hashCode();
    result = 31 * result + kafkaProperties.hashCode();
    result = 31 * result + (int) (connectTimeout ^ (connectTimeout >>> 32));
    result = 31 * result + (injective ? 1 : 0);
    return result;
  }

  public long getCompletedEventCount()
  {
    return doubleEventCount.get() >> 1;
  }

  public State getState() {
    return currentState.get();
  }

  interface ConsumerConnectorFactory
  {
    ConsumerConnector buildConnector(Properties properties);

    class Zookeeper implements ConsumerConnectorFactory
    {
      public ConsumerConnector buildConnector(Properties properties)
      {
        return new kafka.javaapi.consumer.ZookeeperConsumerConnector(
                new ConsumerConfig(properties)
        );
      }
    }
  }

}
