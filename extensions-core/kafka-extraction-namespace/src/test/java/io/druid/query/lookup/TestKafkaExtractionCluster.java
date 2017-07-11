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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.lookup.namespace.NamespaceExtractionModule;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
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
  private static final String topicName = "testTopic";
  private static final Map<String, String> kafkaProperties = new HashMap<>();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Closer closer = Closer.create();

  private KafkaServer kafkaServer;
  private KafkaConfig kafkaConfig;
  private TestingServer zkTestServer;
  private ZkClient zkClient;
  private Injector injector;
  private ObjectMapper mapper;
  private KafkaLookupExtractorFactory factory;

  @Before
  public void setUp() throws Exception
  {
    zkTestServer = new TestingServer(-1, temporaryFolder.newFolder(), true);
    zkTestServer.start();

    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        zkTestServer.stop();
      }
    });

    zkClient = new ZkClient(
        zkTestServer.getConnectString(),
        10000,
        10000,
        ZKStringSerializer$.MODULE$
    );
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        zkClient.close();
      }
    });
    if (!zkClient.exists("/kafka")) {
      zkClient.create("/kafka", null, CreateMode.PERSISTENT);
    }

    log.info("---------------------------Started ZK---------------------------");

    final String zkKafkaPath = "/kafka";

    final Properties serverProperties = new Properties();
    serverProperties.putAll(kafkaProperties);
    serverProperties.put("broker.id", "0");
    serverProperties.put("log.dir", temporaryFolder.newFolder().getAbsolutePath());
    serverProperties.put("log.cleaner.enable", "true");
    serverProperties.put("host.name", "127.0.0.1");
    serverProperties.put("zookeeper.connect", zkTestServer.getConnectString() + zkKafkaPath);
    serverProperties.put("zookeeper.session.timeout.ms", "10000");
    serverProperties.put("zookeeper.sync.time.ms", "200");
    serverProperties.put("port", String.valueOf(ThreadLocalRandom.current().nextInt(9999) + 10000));

    kafkaConfig = new KafkaConfig(serverProperties);

    final long time = DateTime.parse("2015-01-01").getMillis();
    kafkaServer = new KafkaServer(
        kafkaConfig,
        new Time()
        {

          @Override
          public long milliseconds()
          {
            return time;
          }

          @Override
          public long nanoseconds()
          {
            return milliseconds() * 1_000_000;
          }

          @Override
          public void sleep(long ms)
          {
            try {
              Thread.sleep(ms);
            }
            catch (InterruptedException e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
    kafkaServer.startup();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
      }
    });

    int sleepCount = 0;

    while (!kafkaServer.kafkaController().isActive()) {
      Thread.sleep(100);
      if (++sleepCount > 10) {
        throw new InterruptedException("Controller took to long to awaken");
      }
    }

    log.info("---------------------------Started Kafka Server---------------------------");

    final ZkClient zkClient = new ZkClient(
        zkTestServer.getConnectString() + zkKafkaPath, 10000, 10000,
        ZKStringSerializer$.MODULE$
    );

    try (final AutoCloseable autoCloseable = new AutoCloseable()
    {
      @Override
      public void close() throws Exception
      {
        if (zkClient.exists(zkKafkaPath)) {
          try {
            zkClient.deleteRecursive(zkKafkaPath);
          }
          catch (ZkException ex) {
            log.warn(ex, "error deleting %s zk node", zkKafkaPath);
          }
        }
        zkClient.close();
      }
    }) {
      final Properties topicProperties = new Properties();
      topicProperties.put("cleanup.policy", "compact");
      if (!AdminUtils.topicExists(zkClient, topicName)) {
        AdminUtils.createTopic(zkClient, topicName, 1, 1, topicProperties);
      }

      log.info("---------------------------Created topic---------------------------");

      Assert.assertTrue(AdminUtils.topicExists(zkClient, topicName));
    }

    final Properties kafkaProducerProperties = makeProducerProperties();
    final Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(kafkaProducerProperties));
    try (final AutoCloseable autoCloseable = new AutoCloseable()
    {
      @Override
      public void close() throws Exception
      {
        producer.close();
      }
    }) {
      producer.send(
          new KeyedMessage<>(
              topicName,
              StringUtils.toUtf8("abcdefg"),
              StringUtils.toUtf8("abcdefg")
          )
      );
    }

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
    final Map<String, String> consumerProperties = new HashMap<>(kafkaProperties);
    consumerProperties.put("zookeeper.connect", zkTestServer.getConnectString() + zkKafkaPath);
    consumerProperties.put("zookeeper.session.timeout.ms", "10000");
    consumerProperties.put("zookeeper.sync.time.ms", "200");

    final KafkaLookupExtractorFactory kafkaLookupExtractorFactory = new KafkaLookupExtractorFactory(
        null,
        topicName,
        consumerProperties
    );

    factory = (KafkaLookupExtractorFactory) mapper.readValue(
        mapper.writeValueAsString(kafkaLookupExtractorFactory),
        LookupExtractorFactory.class
    );
    Assert.assertEquals(kafkaLookupExtractorFactory.getKafkaTopic(), factory.getKafkaTopic());
    Assert.assertEquals(kafkaLookupExtractorFactory.getKafkaProperties(), factory.getKafkaProperties());
    factory.start();
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        factory.close();
      }
    });
    log.info("--------------------------- started rename manager ---------------------------");
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  private final Properties makeProducerProperties()
  {
    final Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.putAll(kafkaProperties);
    kafkaProducerProperties.put(
        "metadata.broker.list",
        StringUtils.format("127.0.0.1:%d", kafkaServer.socketServer().port())
    );
    kafkaProperties.put("request.required.acks", "1");
    return kafkaProducerProperties;
  }

  private void checkServer()
  {
    if (!kafkaServer.apis().controller().isActive()) {
      throw new ISE("server is not active!");
    }
  }

  @Test(timeout = 60_000L)
  public void testSimpleRename() throws InterruptedException
  {
    final Properties kafkaProducerProperties = makeProducerProperties();
    final Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(kafkaProducerProperties));
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        producer.close();
      }
    });
    checkServer();

    assertUpdated(null, "foo");
    assertReverseUpdated(ImmutableList.<String>of(), "foo");

    long events = factory.getCompletedEventCount();

    log.info("-------------------------     Sending foo bar     -------------------------------");
    producer.send(new KeyedMessage<>(topicName, StringUtils.toUtf8("foo"), StringUtils.toUtf8("bar")));

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
    producer.send(new KeyedMessage<>(topicName, StringUtils.toUtf8("baz"), StringUtils.toUtf8("bat")));
    while (events == factory.getCompletedEventCount()) {
      Thread.sleep(10);
      if (System.currentTimeMillis() > start + 60_000) {
        throw new ISE("Took too long to update event");
      }
    }

    log.info("-------------------------     Checking baz bat     -------------------------------");
    Assert.assertEquals("bat", factory.get().apply("baz"));
    Assert.assertEquals(Collections.singletonList("baz"), factory.get().unapply("bat"));
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

    while (!expected.equals(extractor.unapply(key))) {
      Thread.sleep(100);
    }

    Assert.assertEquals("update check", expected, extractor.unapply(key));
  }
}
