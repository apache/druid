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

package io.druid.query.lookup;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Bytes;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.server.lookup.namespace.cache.CacheHandler;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.druid.query.lookup.KafkaLookupExtractorFactory.DEFAULT_STRING_DECODER;

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
  private final NamespaceExtractionCacheManager cacheManager = PowerMock.createStrictMock(NamespaceExtractionCacheManager.class);
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
          Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
      )
      {
        if ("io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager".equals(valueId)) {
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
    factory.getMapRef().set(ImmutableMap.<String, String>of());

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
    factory1.getMapRef().set(ImmutableMap.<String, String>of());
    final KafkaLookupExtractorFactory factory2 = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC + "b",
        DEFAULT_PROPERTIES
    );
    factory2.getMapRef().set(ImmutableMap.<String, String>of());

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

    Assert.assertTrue(factory.replaces(new MapLookupExtractorFactory(ImmutableMap.<String, String>of(), false)));
    Assert.assertFalse(factory.replaces(factory));
    Assert.assertFalse(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES
    )));

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
    final KafkaStream<String, String> kafkaStream = PowerMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = PowerMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = PowerMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andAnswer(getBlockingAnswer()).anyTimes();
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<String, String>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall();

    final AtomicBoolean threadWasInterrupted = new AtomicBoolean(false);
    consumerConnector.shutdown();
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
    {
      @Override
      public Object answer() throws Throwable
      {
        threadWasInterrupted.set(Thread.currentThread().isInterrupted());
        return null;
      }
    }).times(2);

    PowerMock.replay(cacheManager, cacheHandler, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"),
        10_000L,
        false
    )
    {
      @Override
      ConsumerConnector buildConnector(Properties properties)
      {
        return consumerConnector;
      }
    };

    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertTrue(factory.getFuture().isDone());
    Assert.assertFalse(threadWasInterrupted.get());

    PowerMock.verify(cacheManager, cacheHandler);
  }


  @Test
  public void testStartFailsFromTimeout() throws Exception
  {
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<String, String>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall();
    PowerMock.replay(cacheManager, cacheHandler);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"),
        1,
        false
    )
    {
      @Override
      ConsumerConnector buildConnector(Properties properties)
      {
        // Lock up
        try {
          Thread.currentThread().join();
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
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
    final KafkaStream<String, String> kafkaStream = PowerMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = PowerMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = PowerMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andAnswer(getBlockingAnswer()).anyTimes();
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<String, String>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall().once();
    consumerConnector.shutdown();
    EasyMock.expectLastCall().times(2);
    PowerMock.replay(cacheManager, cacheHandler, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost")
    )
    {
      @Override
      ConsumerConnector buildConnector(Properties properties)
      {
        return consumerConnector;
      }
    };
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertFalse(factory.start());
    PowerMock.verify(cacheManager, cacheHandler);
  }

  @Test
  public void testStartStartStop()
  {
    final KafkaStream<String, String> kafkaStream = PowerMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = PowerMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = PowerMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andAnswer(getBlockingAnswer()).anyTimes();
    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();
    EasyMock.expect(cacheHandler.getCache()).andReturn(new ConcurrentHashMap<String, String>()).once();
    cacheHandler.close();
    EasyMock.expectLastCall().once();
    consumerConnector.shutdown();
    EasyMock.expectLastCall().times(3);
    PowerMock.replay(cacheManager, cacheHandler, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"),
        10_000L,
        false
    )
    {
      @Override
      ConsumerConnector buildConnector(Properties properties)
      {
        return consumerConnector;
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
    expectedException.expectMessage("zookeeper.connect required property");
    PowerMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.<String, String>of()
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
    PowerMock.replay(cacheManager);
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

  @Test
  public void testDefaultDecoder()
  {
    final String str = "some string";
    Assert.assertEquals(str, DEFAULT_STRING_DECODER.fromBytes(StringUtils.toUtf8(str)));
  }

  private IAnswer<Boolean> getBlockingAnswer()
  {
    return new IAnswer<Boolean>()
    {
      @Override
      public Boolean answer() throws Throwable
      {
        Thread.sleep(60000);
        Assert.fail("Test failed to complete within 60000ms");

        return false;
      }
    };
  }
}
