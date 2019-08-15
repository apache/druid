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
import com.google.common.primitives.Bytes;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    NamespaceExtractionCacheManager.class,
    CacheHandler.class
})
@PowerMockIgnore({
    "javax.management.*",
    "javax.net.ssl.*",
    "org.apache.logging.*",
    "org.slf4j.*",
    "com.sun.*",
    "javax.script.*",
    "jdk.*"
})
public class KafkaLookupExtractorFactoryTest
{
  private static final String TOPIC = "some_topic";
  private static final Map<String, String> DEFAULT_PROPERTIES = ImmutableMap.of(
      "some.property", "some.value"
  );
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final NamespaceExtractionCacheManager cacheManager = PowerMock.createStrictMock(
      NamespaceExtractionCacheManager.class);
  private final CacheHandler cacheHandler = PowerMock.createStrictMock(CacheHandler.class);


  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
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
    Assert.assertEquals(expected.getKafkaTopic(), result.getKafkaTopic());
    Assert.assertEquals(expected.getKafkaProperties(), result.getKafkaProperties());
    Assert.assertEquals(cacheManager, result.getCacheManager());
    Assert.assertEquals(0, expected.getCompletedEventCount());
    Assert.assertEquals(0, result.getCompletedEventCount());
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

    final Set<List<Byte>> byteArrays = new HashSet<>(n);
    for (int i = 0; i < n; ++i) {
      final List<Byte> myKey = Bytes.asList(extractor.getCacheKey());
      Assert.assertFalse(byteArrays.contains(myKey));
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assert.assertEquals(n, byteArrays.size());
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

    final Set<List<Byte>> byteArrays = new HashSet<>(n);
    for (int i = 0; i < n; ++i) {
      final LookupExtractor extractor = factory.get();
      final List<Byte> myKey = Bytes.asList(extractor.getCacheKey());
      Assert.assertFalse(byteArrays.contains(myKey));
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assert.assertEquals(n, byteArrays.size());
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
      Assert.assertArrayEquals(baseKey, factory.get().getCacheKey());
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

    Assert.assertFalse(Arrays.equals(factory1.get().getCacheKey(), factory2.get().getCacheKey()));
  }

  @Test
  public void testReplaces()
  {
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    );

    Assert.assertTrue(factory.replaces(null));

    Assert.assertTrue(factory.replaces(new MapLookupExtractorFactory(ImmutableMap.of(), false)));
    Assert.assertFalse(factory.replaces(factory));
    Assert.assertFalse(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    )));

    //noinspection StringConcatenationMissingWhitespace
    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC + "b",
        DEFAULT_PROPERTIES
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("some.property", "some.other.value")
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("some.other.property", "some.value")
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES,
        1,
        false
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
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
    Assert.assertTrue(factory.close());
  }

  @Test
  public void testStartStop()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall();

    PowerMock.replay(cacheManager, cacheHandler);

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

    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertTrue(factory.getFuture().isDone());
    PowerMock.verify(cacheManager, cacheHandler);
  }


  @Test
  public void testStartFailsFromTimeout()
  {
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall();
    PowerMock.replay(cacheManager, cacheHandler);
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
    Assert.assertFalse(factory.start());
    Assert.assertTrue(factory.getFuture().isDone());
    Assert.assertTrue(factory.getFuture().isCancelled());
    PowerMock.verify(cacheManager, cacheHandler);
  }

  @Test
  public void testStartStopStart()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall().once();
    PowerMock.replay(cacheManager, cacheHandler);
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
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertFalse(factory.start());
    PowerMock.verify(cacheManager, cacheHandler);
  }

  @Test
  public void testStartStartStopStop()
  {
    Consumer<String, String> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall().once();
    PowerMock.replay(cacheManager, cacheHandler);
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
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertTrue(factory.close());
    PowerMock.verify(cacheManager, cacheHandler);
  }

  @Test
  public void testStartFailsOnMissingConnect()
  {
    expectedException.expectMessage("bootstrap.servers required property");
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    PowerMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of()
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    PowerMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnGroupID()
  {
    expectedException.expectMessage(
        "Cannot set kafka property [group.id]. Property is randomly generated for you. Found");
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>());
    cacheHandler.close();
    PowerMock.replay(cacheManager, cacheHandler);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("group.id", "make me fail")
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    PowerMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnAutoOffset()
  {
    expectedException.expectMessage(
        "Cannot set kafka property [auto.offset.reset]. Property will be forced to [smallest]. Found ");
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall();
    PowerMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("auto.offset.reset", "make me fail")
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    PowerMock.verify(cacheManager);
  }

  @Test
  public void testFailsGetNotStarted()
  {
    expectedException.expectMessage("Not started");
    new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    ).get();
  }

  @Test
  public void testSerDe() throws Exception
  {
    final NamespaceExtractionCacheManager cacheManager = PowerMock.createStrictMock(NamespaceExtractionCacheManager.class);
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
    Assert.assertEquals(kafkaTopic, otherFactory.getKafkaTopic());
    Assert.assertEquals(kafkaProperties, otherFactory.getKafkaProperties());
    Assert.assertEquals(connectTimeout, otherFactory.getConnectTimeout());
    Assert.assertEquals(injective, otherFactory.isInjective());
  }

  private IAnswer<Boolean> getBlockingAnswer()
  {
    return () -> {
      Thread.sleep(60000);
      Assert.fail("Test failed to complete within 60000ms");

      return false;
    };
  }
}
