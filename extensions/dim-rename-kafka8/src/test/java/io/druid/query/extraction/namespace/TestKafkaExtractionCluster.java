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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.namespace.KafkaExtractionManager;
import io.druid.server.namespace.KafkaExtractionNamespaceFactory;
import io.druid.server.namespace.KafkaExtractionNamespaceModule;
import io.druid.server.namespace.NamespacedExtractionModule;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.CreateMode;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class TestKafkaExtractionCluster
{
  private static final Logger log = new Logger(TestKafkaExtractionCluster.class);
  private static KafkaServer kafkaServer;
  private static Properties kafkaProperties = new Properties();
  private static KafkaConfig kafkaConfig;
  private static final String topicName = "testTopic";
  private static final String namespace = "testNamespace";
  private static TestingServer zkTestServer;
  private static KafkaExtractionManager renameManager;
  private static final ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();

  private static final String[] defaultZNodes = new String[]{"kafka"};
  private static final Lifecycle lifecycle = new Lifecycle();
  private static final NamespaceExtractionCacheManager extractionCacheManager = new OnHeapNamespaceExtractionCacheManager(
      lifecycle
  );

  @AfterClass
  public static void tearDownStatic()
  {
    lifecycle.stop();
  }

  @BeforeClass
  public static void setupStatic() throws Exception
  {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setTarget("System.out");
    consoleAppender.setFollow(true);
    consoleAppender.activateOptions();
    consoleAppender.setName("console");
    consoleAppender.setThreshold(Level.INFO);
    consoleAppender.setLayout(new PatternLayout("%d{ISO8601} %p [%t] %c - %m%n"));
    BasicConfigurator.configure(
        consoleAppender
    );
    log.info("---------------------------Set console appender---------------------------");
    zkTestServer = new TestingServer(-1, new File(tmpDir.getAbsolutePath() + "/zk"), true);
    {
      ZkClient zkClient = new ZkClient(
          zkTestServer.getConnectString(),
          10000,
          10000,
          ZKStringSerializer$.MODULE$
      );
      for (String znode : defaultZNodes) {
        final String zNode = String.format("/%s", znode);
        if (zkClient.exists(zNode)) {
          zkClient.deleteRecursive(zNode);
        }
        zkClient.create(zNode, null, CreateMode.PERSISTENT);
      }
      zkClient.close();
    }

    log.info("---------------------------Started ZK---------------------------");
    kafkaProperties.put("broker.id", "0");
    kafkaProperties.put("log.dir", tmpDir.getAbsolutePath() + "/log");
    kafkaProperties.put("zookeeper.connect", zkTestServer.getConnectString() + "/kafka");
    kafkaProperties.put("log.cleaner.enable", "true");
    kafkaProperties.put("request.required.acks", "1");
    kafkaProperties.put("host", "127.0.0.1");
    kafkaProperties.put("host.name", "127.0.0.1");

    kafkaProperties.put("zookeeper.session.timeout.ms", "10000");
    kafkaProperties.put("zookeeper.sync.time.ms", "200");
    kafkaProperties.put("auto.commit.interval.ms", "1000");

    kafkaConfig = new KafkaConfig(kafkaProperties);

    final long time = DateTime.parse("2015-01-01").getMillis();
    kafkaServer = new KafkaServer(
        kafkaConfig, new Time()
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
    kafkaProperties.put(
        "metadata.broker.list",
        String.format("127.0.0.1:%d", kafkaServer.socketServer().port())
    );
    int sleepCount = 0;
    while (!kafkaServer.kafkaController().isActive()) {
      Thread.sleep(10);
      if (++sleepCount > 100) {
        throw new InterruptedException("Controller took to long to awaken");
      }
    }

    log.info("---------------------------Started Kafka Server---------------------------");

    ZkClient zkClient = new ZkClient(
        kafkaProperties.getProperty("zookeeper.connect"), 10000, 10000,
        ZKStringSerializer$.MODULE$
    );
    try {
      final Properties topicProperties = new Properties();
      topicProperties.put("cleanup.policy", "compact");
      if (!AdminUtils.topicExists(zkClient, topicName)) {
        AdminUtils.createTopic(zkClient, topicName, 1, 1, topicProperties);
      }

      log.info("---------------------------Created topic---------------------------");

      Assert.assertTrue(AdminUtils.topicExists(zkClient, topicName));
    }
    finally {
      zkClient.close();
    }
    fnCache.clear();
    renameManager = new KafkaExtractionManager(
        kafkaProperties,
        extractionCacheManager,
        fnCache
    );
    Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProperties));
    try {
      producer.send(
          new KeyedMessage<byte[], byte[]>(
              topicName,
              StringUtils.toUtf8("abcdefg"),
              StringUtils.toUtf8("abcdefg")
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      producer.close();
    }
    log.info("--------------------------- placed default item via producer ---------------------------");
    renameManager.start();
    renameManager.addListener(new KafkaExtractionNamespace("testTopic", "testTopic"));
    long start = System.currentTimeMillis();
    while (renameManager.getBackgroundTaskCount() < 1) {
      Thread.sleep(10); // wait for map populator to start up
      if (System.currentTimeMillis() > start + 60_000) {
        throw new ISE("renameManager took too long to start");
      }
    }
    log.info("--------------------------- started rename manager ---------------------------");
  }

  @AfterClass
  public static void closeStatic() throws IOException
  {
    if (null != renameManager) {
      renameManager.stop();
    }

    if (null != kafkaServer) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }

    if (null != zkTestServer) {
      zkTestServer.stop();
    }
  }

  private static void checkServer()
  {
    if (!kafkaServer.apis().controller().isActive()) {
      throw new ISE("server is not active!");
    }
  }

  @Test
  public void testSimpleRename() throws InterruptedException
  {
    final Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProperties));
    try {
      checkServer();

      KafkaExtractionNamespace extractionNamespace = new KafkaExtractionNamespace(namespace, topicName);
      final KafkaExtractionNamespaceFactory factory = new KafkaExtractionNamespaceFactory(extractionCacheManager, renameManager);
      final Function<String, String> fn = factory.build(extractionNamespace);
      Assert.assertEquals(null, fn.apply("foo"));

      long events = renameManager.getNumEvents();

      producer.send(new KeyedMessage<byte[], byte[]>(topicName, StringUtils.toUtf8("foo"), StringUtils.toUtf8("bar")));

      long start = System.currentTimeMillis();
      while (fn.apply("foo") == null) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking foo bar     -------------------------------");
      Assert.assertEquals("bar", fn.apply("foo"));
      Assert.assertEquals(null, fn.apply("baz"));

      checkServer();
      events = renameManager.getNumEvents();
      producer.send(new KeyedMessage<byte[], byte[]>(topicName, StringUtils.toUtf8("baz"), StringUtils.toUtf8("bat")));
      while (events == renameManager.getNumEvents()) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }
      Assert.assertEquals("bat", fn.apply("baz"));
    }
    finally {
      producer.close();
    }
  }
}
