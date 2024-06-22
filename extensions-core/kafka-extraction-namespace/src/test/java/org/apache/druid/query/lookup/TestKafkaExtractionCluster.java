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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionModule;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Some;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class TestKafkaExtractionCluster
{
  private static final Logger log = new Logger(TestKafkaExtractionCluster.class);
  private static final String TOPIC_NAME = "testTopic";
  private static final Map<String, String> KAFKA_PROPERTIES = new HashMap<>();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Closer closer = Closer.create();

  private TestingCluster zkServer;
  private KafkaServer kafkaServer;
  private Injector injector;
  private ObjectMapper mapper;
  private KafkaLookupExtractorFactory factory;

  private static List<ProducerRecord<byte[], byte[]>> generateRecords()
  {
    return ImmutableList.of(
            new ProducerRecord<>(TOPIC_NAME, 0,
                    StringUtils.toUtf8("abcdefg"),
                    StringUtils.toUtf8("abcdefg")));
  }

  @Before
  public void setUp() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();
    closer.register(() -> {
      zkServer.stop();
    });

    kafkaServer = new KafkaServer(
          getBrokerProperties(),
          Time.SYSTEM,
          Some.apply(StringUtils.format("TestingBroker[%d]-", 1)),
          false);

    kafkaServer.startup();
    closer.register(() -> {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    });
    log.info("---------------------------Started Kafka Broker ---------------------------");

    log.info("---------------------------Publish Messages to topic-----------------------");
    publishRecordsToKafka();

    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              }
            },
            // These injections fail under IntelliJ but are required for maven
            new NamespaceExtractionModule(),
            new KafkaExtractionNamespaceModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);

    log.info("--------------------------- placed default item via producer ---------------------------");
    final Map<String, String> consumerProperties = getConsumerProperties();

    final KafkaLookupExtractorFactory kafkaLookupExtractorFactory = new KafkaLookupExtractorFactory(
        null,
        TOPIC_NAME,
        consumerProperties
    );

    factory = (KafkaLookupExtractorFactory) mapper.readValue(
        mapper.writeValueAsString(kafkaLookupExtractorFactory),
        LookupExtractorFactory.class
    );
    Assert.assertEquals(kafkaLookupExtractorFactory.getKafkaTopic(), factory.getKafkaTopic());
    Assert.assertEquals(kafkaLookupExtractorFactory.getKafkaProperties(), factory.getKafkaProperties());
    factory.start();
    closer.register(() -> factory.close());
    log.info("--------------------------- started rename manager ---------------------------");
  }

  @Nonnull
  private Map<String, String> getConsumerProperties()
  {
    final Map<String, String> props = new HashMap<>(KAFKA_PROPERTIES);
    int port = kafkaServer.advertisedListeners().apply(0).port();
    props.put("bootstrap.servers", StringUtils.format("127.0.0.1:%d", port));
    return props;
  }

  private void publishRecordsToKafka()
  {
    final Properties kafkaProducerProperties = makeProducerProperties();

    try (final Producer<byte[], byte[]> producer = new KafkaProducer(kafkaProducerProperties)) {
      generateRecords().forEach(producer::send);
    }
  }

  @Nonnull
  private KafkaConfig getBrokerProperties() throws IOException
  {
    final Properties serverProperties = new Properties();
    serverProperties.putAll(KAFKA_PROPERTIES);
    serverProperties.put("broker.id", "0");
    serverProperties.put("zookeeper.connect", zkServer.getConnectString());
    serverProperties.put("port", String.valueOf(ThreadLocalRandom.current().nextInt(9999) + 10000));
    serverProperties.put("auto.create.topics.enable", "true");
    serverProperties.put("log.dir", temporaryFolder.newFolder().getAbsolutePath());
    serverProperties.put("num.partitions", "1");
    serverProperties.put("offsets.topic.replication.factor", "1");
    serverProperties.put("default.replication.factor", "1");
    serverProperties.put("log.cleaner.enable", "true");
    serverProperties.put("advertised.host.name", "localhost");
    serverProperties.put("zookeeper.session.timeout.ms", "30000");
    serverProperties.put("zookeeper.sync.time.ms", "200");
    return new KafkaConfig(serverProperties);
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  private Properties makeProducerProperties()
  {
    final Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.putAll(KAFKA_PROPERTIES);
    int port = kafkaServer.advertisedListeners().apply(0).port();
    kafkaProducerProperties.put("bootstrap.servers", StringUtils.format("127.0.0.1:%d", port));
    kafkaProducerProperties.put("key.serializer", ByteArraySerializer.class.getName());
    kafkaProducerProperties.put("value.serializer", ByteArraySerializer.class.getName());
    kafkaProducerProperties.put("acks", "all");
    KAFKA_PROPERTIES.put("request.required.acks", "1");
    return kafkaProducerProperties;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void checkServer() throws Exception
  {
    try (Admin adminClient = Admin.create((Map) getConsumerProperties())) {
      if (adminClient.describeCluster().controller().get() == null) {
        throw new ISE("server is not active!");
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testSimpleLookup() throws Exception
  {
    try (final Producer<byte[], byte[]> producer = new KafkaProducer(makeProducerProperties())) {
      checkServer();

      assertUpdated(null, "foo");
      assertReverseUpdated(ImmutableList.of(), "foo");

      long events = factory.getCompletedEventCount();

      log.info("-------------------------     Sending foo bar     -------------------------------");
      producer.send(new ProducerRecord<>(TOPIC_NAME, StringUtils.toUtf8("foo"), StringUtils.toUtf8("bar")));

      long start = System.currentTimeMillis();
      while (events == factory.getCompletedEventCount()) {
        Thread.sleep(100);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking foo bar     -------------------------------");
      assertUpdated("bar", "foo");
      assertReverseUpdated(Collections.singletonList("foo"), "bar");
      assertUpdated(null, "baz");

      checkServer();
      events = factory.getCompletedEventCount();

      log.info("-------------------------     Sending baz bat     -------------------------------");
      producer.send(new ProducerRecord<>(TOPIC_NAME, StringUtils.toUtf8("baz"), StringUtils.toUtf8("bat")));
      while (events == factory.getCompletedEventCount()) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking baz bat     -------------------------------");
      Assert.assertEquals("bat", factory.get().apply("baz"));
      Assert.assertEquals(
          Collections.singletonList("baz"),
          Lists.newArrayList(factory.get().unapplyAll(Collections.singleton("bat")))
      );
    }
  }

  @Test(timeout = 60_000L)
  public void testLookupWithTombstone() throws Exception
  {
    try (final Producer<byte[], byte[]> producer = new KafkaProducer(makeProducerProperties())) {
      checkServer();

      assertUpdated(null, "foo");
      assertReverseUpdated(ImmutableList.of(), "foo");

      long events = factory.getCompletedEventCount();

      log.info("-------------------------     Sending foo bar     -------------------------------");
      producer.send(new ProducerRecord<>(TOPIC_NAME, StringUtils.toUtf8("foo"), StringUtils.toUtf8("bar")));

      long start = System.currentTimeMillis();
      while (events == factory.getCompletedEventCount()) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking foo bar     -------------------------------");
      assertUpdated("bar", "foo");
      assertReverseUpdated(Collections.singletonList("foo"), "bar");

      checkServer();
      events = factory.getCompletedEventCount();

      log.info("-----------------------     Sending foo tombstone     -----------------------------");
      producer.send(new ProducerRecord<>(TOPIC_NAME, StringUtils.toUtf8("foo"), null));
      while (events == factory.getCompletedEventCount()) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-----------------------     Checking foo removed     -----------------------------");
      assertUpdated(null, "foo");
      assertReverseUpdated(ImmutableList.of(), "foo");
    }
  }

  @Test(timeout = 60_000L)
  public void testLookupWithInitTombstone() throws Exception
  {
    try (final Producer<byte[], byte[]> producer = new KafkaProducer(makeProducerProperties())) {
      checkServer();

      assertUpdated(null, "foo");
      assertReverseUpdated(ImmutableList.of(), "foo");

      long events = factory.getCompletedEventCount();

      long start = System.currentTimeMillis();

      log.info("-----------------------     Sending foo tombstone     -----------------------------");
      producer.send(new ProducerRecord<>(TOPIC_NAME, StringUtils.toUtf8("foo"), null));
      while (events == factory.getCompletedEventCount()) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-----------------------     Checking foo removed     -----------------------------");
      assertUpdated(null, "foo");
      assertReverseUpdated(ImmutableList.of(), "foo");
    }
  }

  private void assertUpdated(
      String expected,
      String key
  )
      throws InterruptedException
  {
    final LookupExtractor extractor = factory.get();
    if (expected == null) {
      while (extractor.apply(key) != null) {
        Thread.sleep(100);
      }
    } else {
      while (!expected.equals(extractor.apply(key))) {
        Thread.sleep(100);
      }
    }

    Assert.assertEquals("update check", expected, extractor.apply(key));
  }

  private void assertReverseUpdated(
      List<String> expected,
      String key
  )
      throws InterruptedException
  {
    final LookupExtractor extractor = factory.get();

    while (!expected.equals(Lists.newArrayList(extractor.unapplyAll(Collections.singleton(key))))) {
      Thread.sleep(100);
    }

    Assert.assertEquals("update check", expected, Lists.newArrayList(extractor.unapplyAll(Collections.singleton(key))));
  }
}
