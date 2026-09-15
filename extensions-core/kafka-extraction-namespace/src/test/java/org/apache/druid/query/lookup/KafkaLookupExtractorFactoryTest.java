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

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.server.lookup.namespace.cache.MockNamespaceExtractionCacheManager;
import org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaLookupExtractorFactoryTest
{
  private static final String TOPIC = "some_topic";
  private static final Map<String, String> DEFAULT_PROPERTIES = ImmutableMap.of(
      "some.property", "some.value"
  );
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final NamespaceExtractionCacheManager cacheManager = MockNamespaceExtractionCacheManager.createMockNamespaceExtractionCacheManager();

  @BeforeEach
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues()
    {
      @Override
      public Object findInjectableValue(
          Object valueId,
          DeserializationContext ctxt,
          BeanProperty forProperty,
          Object beanInstance
      )
      {
        if ("org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager".equals(valueId)) {
          return cacheManager;
        } else {
          return null;
        }
      }
    });
  }

  @Test
  public void testSimpleSerDe() throws Exception
  {
    final KafkaLookupExtractorFactory expected = new KafkaLookupExtractorFactory(null, TOPIC, DEFAULT_PROPERTIES);
    final KafkaLookupExtractorFactory result = mapper.readValue(
        mapper.writeValueAsString(expected),
        KafkaLookupExtractorFactory.class
    );
    result.awaitInitialization();
    Assertions.assertEquals(expected.getKafkaTopic(), result.getKafkaTopic());
    Assertions.assertEquals(expected.getKafkaProperties(), result.getKafkaProperties());
    Assertions.assertEquals(cacheManager, result.getCacheManager());
    Assertions.assertEquals(0, expected.getCompletedEventCount());
    Assertions.assertEquals(0, result.getCompletedEventCount());
    Assertions.assertTrue(result.isInitialized());
  }

  @Test
  public void testCacheKeyScramblesOnNewData()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );
    factory.getMapRef().set(ImmutableMap.of());
    final AtomicLong events = factory.getDoubleEventCount();

    final LookupExtractor extractor = factory.get();

    final Set<List<Byte>> byteArrays = Sets.newHashSetWithExpectedSize(n);
    for (int i = 0; i < n; ++i) {
      final List<Byte> myKey = Bytes.asList(extractor.getCacheKey());
      Assertions.assertFalse(byteArrays.contains(myKey));
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assertions.assertEquals(n, byteArrays.size());
  }

  @Test
  public void testCacheKeyScramblesDifferentStarts()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );
    factory.getMapRef().set(ImmutableMap.of());
    final AtomicLong events = factory.getDoubleEventCount();

    final Set<List<Byte>> byteArrays = Sets.newHashSetWithExpectedSize(n);
    for (int i = 0; i < n; ++i) {
      final LookupExtractor extractor = factory.get();
      final List<Byte> myKey = Bytes.asList(extractor.getCacheKey());
      Assertions.assertFalse(byteArrays.contains(myKey));
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assertions.assertEquals(n, byteArrays.size());
  }

  @Test
  public void testCacheKeySameOnNoChange()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );
    factory.getMapRef().set(ImmutableMap.of());

    final LookupExtractor extractor = factory.get();

    final byte[] baseKey = extractor.getCacheKey();
    for (int i = 0; i < n; ++i) {
      Assertions.assertArrayEquals(baseKey, factory.get().getCacheKey());
    }
  }

  @Test
  public void testCacheKeyDifferentForTopics()
  {
    final KafkaLookupExtractorFactory factory1 = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );
    factory1.getMapRef().set(ImmutableMap.of());
    //noinspection StringConcatenationMissingWhitespace
    final KafkaLookupExtractorFactory factory2 = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC + "b",
        DEFAULT_PROPERTIES
    );
    factory2.getMapRef().set(ImmutableMap.of());

    Assertions.assertFalse(Arrays.equals(factory1.get().getCacheKey(), factory2.get().getCacheKey()));
  }

  @Test
  public void testReplaces()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );

    Assertions.assertTrue(factory.replaces(null));

    Assertions.assertTrue(factory.replaces(new MapLookupExtractorFactory(ImmutableMap.of(), false)));
    Assertions.assertFalse(factory.replaces(factory));
    Assertions.assertFalse(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    )));

    //noinspection StringConcatenationMissingWhitespace
    Assertions.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC + "b",
        DEFAULT_PROPERTIES
    )));

    Assertions.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("some.property", "some.other.value")
    )));

    Assertions.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("some.other.property", "some.value")
    )));

    Assertions.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES,
        1,
        false
    )));

    Assertions.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES,
        0,
        true
    )));
  }

  @Test
  public void testStopWithoutStart()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );
    Assertions.assertTrue(factory.close());
  }

  @Test
  public void testStartStop()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.replay(cacheManager);

    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("bootstrap.servers", "localhost"),
        10_000L,
        false
    )
    {
      @Override
      Consumer<String, String> getConsumer()
      {
        return kafkaConsumer;
      }
    };

    Assertions.assertTrue(factory.start());
    Assertions.assertTrue(factory.close());
    Assertions.assertTrue(factory.getFuture().isDone());
    EasyMock.verify(cacheManager);
  }


  @Test
  public void testStartFailsFromTimeout()
  {
    EasyMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("bootstrap.servers", "localhost"),
        1,
        false
    )
    {
      @Override
      Consumer getConsumer()
      {
        // Lock up
        try {
          Thread.currentThread().join();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        throw new RuntimeException("shouldn't make it here");
      }
    };
    Assertions.assertFalse(factory.start());
    Assertions.assertTrue(factory.getFuture().isDone());
    Assertions.assertTrue(factory.getFuture().isCancelled());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartStopStart()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("bootstrap.servers", "localhost")
    )
    {
      @Override
      Consumer<String, String> getConsumer()
      {
        return kafkaConsumer;
      }
    };
    Assertions.assertTrue(factory.start());
    Assertions.assertTrue(factory.close());
    Assertions.assertFalse(factory.start());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartStartStopStop()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("bootstrap.servers", "localhost"),
        10_000L,
        false
    )
    {
      @Override
      Consumer<String, String> getConsumer()
      {
        return kafkaConsumer;
      }
    };
    Assertions.assertTrue(factory.start());
    Assertions.assertTrue(factory.start());
    Assertions.assertTrue(factory.close());
    Assertions.assertTrue(factory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnMissingConnect()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of()
    );
    Assertions.assertThrows(
        NullPointerException.class,
        () -> factory.start(),
        "bootstrap.servers required property"
    );
    Assertions.assertTrue(factory.close());
  }

  @Test
  public void testStartFailsOnGroupID()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("group.id", "make me fail")
    );
    Assertions.assertThrows(
        IAE.class,
        () -> factory.start(),
        "Cannot set kafka property [group.id]. Property is randomly generated for you. Found"
    );
    Assertions.assertTrue(factory.close());
  }

  @Test
  public void testStartFailsOnAutoOffset()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("auto.offset.reset", "make me fail")
    );
    Assertions.assertThrows(
        IAE.class,
        () -> factory.start(),
        "Cannot set kafka property [auto.offset.reset]. Property will be forced to [smallest]. Found "
    );
    Assertions.assertTrue(factory.close());
  }

  @Test
  public void testStartFailsOnAutoCommit()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("enable.auto.commit", "true")
    );
    Assertions.assertThrows(
        IAE.class,
        () -> factory.start(),
        "Cannot set kafka property [enable.auto.commit]. Property will be forced to [false]. Found [true]"
    );
    Assertions.assertTrue(factory.close());
  }

  @Test
  public void testFailsGetNotStarted()
  {
    Assertions.assertThrows(NullPointerException.class, () -> new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    ).get(), "Not started");
  }

  @Test
  public void testSerDe() throws Exception
  {
    final NamespaceExtractionCacheManager cacheManager = EasyMock.createStrictMock(NamespaceExtractionCacheManager.class);
    final String kafkaTopic = "some_topic";
    final Map<String, String> kafkaProperties = ImmutableMap.of("some_key", "some_value");
    final long connectTimeout = 999;
    final boolean injective = true;
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        kafkaTopic,
        kafkaProperties,
        connectTimeout,
        injective
    );
    final KafkaLookupExtractorFactory otherFactory = mapper.readValue(
        mapper.writeValueAsString(factory),
        KafkaLookupExtractorFactory.class
    );
    Assertions.assertEquals(kafkaTopic, otherFactory.getKafkaTopic());
    Assertions.assertEquals(kafkaProperties, otherFactory.getKafkaProperties());
    Assertions.assertEquals(connectTimeout, otherFactory.getConnectTimeout());
    Assertions.assertEquals(injective, otherFactory.isInjective());
  }
}
