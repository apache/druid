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

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.StringUtils;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.lookup.KafkaLookupExtractorFactory.ConsumerConnectorFactory;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.druid.query.lookup.KafkaLookupExtractorFactory.DEFAULT_STRING_DECODER;

public class KafkaLookupExtractorFactoryTest
{
  private static final String TOPIC = "some_topic";
  private static final Map<String, String> DEFAULT_PROPERTIES = ImmutableMap.of(
      "some.property", "some.value"
  );
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final NamespaceExtractionCacheManager cacheManager = EasyMock.createStrictMock(
          NamespaceExtractionCacheManager.class
  );

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
    final KafkaLookupExtractorFactory expected = makeLookupFactory();

    final KafkaLookupExtractorFactory result = mapper.readValue(
        mapper.writeValueAsString(expected),
        KafkaLookupExtractorFactory.class
    );

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCacheKeyScramblesOnNewData()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager);

    mockCacheManager();
    factory.init();

    final AtomicLong events = factory.getDoubleEventCount();

    final LookupExtractor extractor = factory.get();

    final List<byte[]> byteArrays = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      final byte[] myKey = extractor.getCacheKey();
      // Not terribly efficient.. but who cares
      for (byte[] byteArray : byteArrays) {
        Assert.assertFalse(Arrays.equals(byteArray, myKey));
      }
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assert.assertEquals(n, byteArrays.size());
  }

  @Test
  public void testCacheKeyScramblesDifferentStarts()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager);
    mockCacheManager();
    factory.init();

    final AtomicLong events = factory.getDoubleEventCount();

    final List<byte[]> byteArrays = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      final LookupExtractor extractor = factory.get();
      final byte[] myKey = extractor.getCacheKey();
      // Not terribly efficient.. but who cares
      for (byte[] byteArray : byteArrays) {
        Assert.assertFalse(Arrays.equals(byteArray, myKey));
      }
      byteArrays.add(myKey);
      events.incrementAndGet();
    }
    Assert.assertEquals(n, byteArrays.size());
  }

  @Test
  public void testCacheKeySameOnNoChange()
  {
    final int n = 1000;
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager);

    mockCacheManager();
    factory.init();

    final LookupExtractor extractor = factory.get();

    final byte[] baseKey = extractor.getCacheKey();
    for (int i = 0; i < n; ++i) {
      Assert.assertArrayEquals(baseKey, factory.get().getCacheKey());
    }
  }

  @Test
  public void testCacheKeyDifferentForTopics()
  {
    final KafkaLookupExtractorFactory factory1 = makeLookupFactory(cacheManager);
    final KafkaLookupExtractorFactory factory2 = makeLookupFactory(cacheManager, TOPIC + 'b');

    mockCacheManager();
    factory1.init(); factory2.init();

    Assert.assertFalse(Arrays.equals(factory1.get().getCacheKey(), factory2.get().getCacheKey()));
  }

  @Test
  public void testReplaces()
  {
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager);

    Assert.assertTrue(factory.replaces(null));

    Assert.assertTrue(factory.replaces(new MapLookupExtractorFactory(ImmutableMap.<String, String>of(), false)));
    Assert.assertFalse(factory.replaces(factory));
    Assert.assertFalse(factory.replaces(makeLookupFactory(cacheManager)));

    Assert.assertTrue(factory.replaces(makeLookupFactory(cacheManager, TOPIC + 'b')));

    Assert.assertTrue(factory.replaces(makeLookupFactory(
            cacheManager, ImmutableMap.of("some.property", "some.other.value")
    )));

    Assert.assertTrue(factory.replaces(makeLookupFactory(
        cacheManager, ImmutableMap.of("some.other.property", "some.value")
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES,
        1,
        false,
        NOOP_CONNECTION_FACTORY
    )));

    Assert.assertTrue(factory.replaces(new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        DEFAULT_PROPERTIES,
        0,
        true,
        NOOP_CONNECTION_FACTORY
    )));
  }

  @Test
  public void testStopWithoutStart()
  {
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager);
    Assert.assertTrue(factory.close());
  }

  @Test
  public void testStartStop()
  {
    final KafkaStream<String, String> kafkaStream = EasyMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = EasyMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = EasyMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andReturn(false).anyTimes();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(true).once();
    consumerConnector.shutdown();
    EasyMock.expectLastCall().once();
    EasyMock.replay(cacheManager, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"),
        10_000L,
        false,
        predefinedConsumerConnectorFactory(consumerConnector)
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertTrue(factory.getFuture().isDone());
    EasyMock.verify(cacheManager);
  }


  @Test
  public void testStartFailsFromTimeout() throws Exception
  {
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(true).once();
    EasyMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
            cacheManager,
            TOPIC,
            ImmutableMap.of("zookeeper.connect", "localhost"),
            1,
            false,
            new ConsumerConnectorFactory() {
              @Override
              public ConsumerConnector buildConnector(Properties properties)
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
            }
    );
    Assert.assertFalse(factory.start());
    Assert.assertTrue(factory.getFuture().isDone());
    Assert.assertTrue(factory.getFuture().isCancelled());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStopDeleteError()
  {
    final KafkaStream<String, String> kafkaStream = EasyMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = EasyMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = EasyMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andReturn(false).anyTimes();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(false).once();

    EasyMock.replay(cacheManager, kafkaStream, consumerConnector, consumerIterator);

    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager, TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"), 0, false,
        predefinedConsumerConnectorFactory(consumerConnector)
    );

    Assert.assertTrue(factory.start());
    Assert.assertFalse(factory.close());
    EasyMock.verify(cacheManager, kafkaStream, consumerConnector, consumerIterator);
  }


  @Test
  public void testStartStopStart()
  {
    final KafkaStream<String, String> kafkaStream = EasyMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = EasyMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = EasyMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andReturn(false).anyTimes();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(true).once();
    consumerConnector.shutdown();
    EasyMock.expectLastCall().once();
    EasyMock.replay(cacheManager, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager, TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"), 0, false,
        predefinedConsumerConnectorFactory(consumerConnector)
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertFalse(factory.start());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartStartStop()
  {
    final KafkaStream<String, String> kafkaStream = EasyMock.createStrictMock(KafkaStream.class);
    final ConsumerIterator<String, String> consumerIterator = EasyMock.createStrictMock(ConsumerIterator.class);
    final ConsumerConnector consumerConnector = EasyMock.createStrictMock(ConsumerConnector.class);
    EasyMock.expect(consumerConnector.createMessageStreamsByFilter(
        EasyMock.anyObject(TopicFilter.class),
        EasyMock.anyInt(),
        EasyMock.eq(
            DEFAULT_STRING_DECODER),
        EasyMock.eq(DEFAULT_STRING_DECODER)
    )).andReturn(ImmutableList.of(kafkaStream)).once();
    EasyMock.expect(kafkaStream.iterator()).andReturn(consumerIterator).anyTimes();
    EasyMock.expect(consumerIterator.hasNext()).andReturn(false).anyTimes();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(true).once();
    consumerConnector.shutdown();
    EasyMock.expectLastCall().once();
    EasyMock.replay(cacheManager, kafkaStream, consumerConnector, consumerIterator);
    final KafkaLookupExtractorFactory factory = new KafkaLookupExtractorFactory(
        cacheManager,
        TOPIC,
        ImmutableMap.of("zookeeper.connect", "localhost"),
        10_000L,
        false, predefinedConsumerConnectorFactory(consumerConnector)
    );
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    Assert.assertTrue(factory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnMissingConnect()
  {
    expectedException.expectMessage("zookeeper.connect required property");
    EasyMock.replay(cacheManager);
    final KafkaLookupExtractorFactory factory = makeLookupFactory(cacheManager, ImmutableMap.<String, String>of());
    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnGroupID()
  {
    expectedException.expectMessage(
        "Cannot set kafka property [group.id]. Property is randomly generated for you. Found");
    EasyMock.replay(cacheManager);

    final KafkaLookupExtractorFactory factory = makeLookupFactory(
            cacheManager, ImmutableMap.of("group.id", "make me fail")
    );

    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartFailsOnAutoOffset()
  {
    expectedException.expectMessage(
        "Cannot set kafka property [auto.offset.reset]. Property will be forced to [smallest]. Found ");
    EasyMock.replay(cacheManager);

    final KafkaLookupExtractorFactory factory = makeLookupFactory(
        cacheManager, ImmutableMap.of("auto.offset.reset", "make me fail")
    );

    Assert.assertTrue(factory.start());
    Assert.assertTrue(factory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testFailsGetNotStarted()
  {
    expectedException.expectMessage("Not started");
    makeLookupFactory(cacheManager).get();
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

    Assert.assertEquals(factory, otherFactory);
  }

  @Test
  public void testDefaultDecoder()
  {
    final String str = "some string";
    Assert.assertEquals(str, DEFAULT_STRING_DECODER.fromBytes(StringUtils.toUtf8(str)));
  }

  private void mockCacheManager() {
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .anyTimes();

    EasyMock.replay(cacheManager);
  }

  private static KafkaLookupExtractorFactory makeLookupFactory() {
    return makeLookupFactory(null);
  }

  private static KafkaLookupExtractorFactory makeLookupFactory(NamespaceExtractionCacheManager cacheManager) {
    return makeLookupFactory(cacheManager, TOPIC);
  }

  private static KafkaLookupExtractorFactory makeLookupFactory(NamespaceExtractionCacheManager cacheManager, String topicName) {
    return new KafkaLookupExtractorFactory(
            cacheManager, topicName, DEFAULT_PROPERTIES, 0, false, NOOP_CONNECTION_FACTORY
    );
  }

  private static KafkaLookupExtractorFactory makeLookupFactory(
          NamespaceExtractionCacheManager cacheManager, Map<String, String> kafkaProperties) {
    return new KafkaLookupExtractorFactory(
            cacheManager, TOPIC, kafkaProperties, 0, false, NOOP_CONNECTION_FACTORY
    );
  }

  static ConsumerConnectorFactory NOOP_CONNECTION_FACTORY = new ConsumerConnectorFactory()
  {
    @Override
    public ConsumerConnector buildConnector(Properties properties) {
      throw new UnsupportedOperationException("Not expected to be invoked");
    }
  };

  static ConsumerConnectorFactory predefinedConsumerConnectorFactory(final ConsumerConnector consumerConnector)
  {
    return new ConsumerConnectorFactory() {
      @Override
      public ConsumerConnector buildConnector(Properties properties) {
        return consumerConnector;
      }
    };
  }
}
