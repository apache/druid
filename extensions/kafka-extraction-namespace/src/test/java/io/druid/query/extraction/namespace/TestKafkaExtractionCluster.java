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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import io.druid.initialization.Initialization;
import io.druid.server.namespace.KafkaExtractionManager;
import io.druid.server.namespace.KafkaExtractionNamespaceFactory;
import io.druid.server.namespace.KafkaExtractionNamespaceModule;
import io.druid.server.namespace.NamespacedExtractionModule;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
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

  private static final Lifecycle lifecycle = new Lifecycle();
  private static NamespaceExtractionCacheManager extractionCacheManager;
  private static ZkClient zkClient = null;
  private static File tmpDir = Files.createTempDir();
  private static Injector injector;



  public static class KafkaFactoryProvider implements Provider<ExtractionNamespaceFunctionFactory<?>>
  {
    private final KafkaExtractionManager kafkaExtractionManager;
    @Inject
    public KafkaFactoryProvider(
        KafkaExtractionManager kafkaExtractionManager
    ){
      this.kafkaExtractionManager = kafkaExtractionManager;
    }
    @Override
    public ExtractionNamespaceFunctionFactory<?> get()
    {
      return new KafkaExtractionNamespaceFactory(kafkaExtractionManager);
    }
  }

  @BeforeClass
  public static void setupStatic() throws Exception
  {
    zkTestServer = new TestingServer(-1, new File(tmpDir.getAbsolutePath() + "/zk"), true);
    zkClient = new ZkClient(
        zkTestServer.getConnectString(),
        10000,
        10000,
        ZKStringSerializer$.MODULE$
    );
    if (!zkClient.exists("/kafka")) {
      zkClient.create("/kafka", null, CreateMode.PERSISTENT);
    }


    log.info("---------------------------Started ZK---------------------------");


    final Properties serverProperties = new Properties();
    serverProperties.putAll(kafkaProperties);
    serverProperties.put("broker.id", "0");
    serverProperties.put("log.dir", tmpDir.getAbsolutePath() + "/log");
    serverProperties.put("log.cleaner.enable", "true");
    serverProperties.put("host.name", "127.0.0.1");
    serverProperties.put("zookeeper.connect", zkTestServer.getConnectString() + "/kafka");
    serverProperties.put("zookeeper.session.timeout.ms", "10000");
    serverProperties.put("zookeeper.sync.time.ms", "200");

    kafkaConfig = new KafkaConfig(serverProperties);

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
    int sleepCount = 0;
    while (!kafkaServer.kafkaController().isActive()) {
      Thread.sleep(10);
      if (++sleepCount > 100) {
        throw new InterruptedException("Controller took to long to awaken");
      }
    }

    log.info("---------------------------Started Kafka Server---------------------------");

    ZkClient zkClient = new ZkClient(
        zkTestServer.getConnectString() + "/kafka", 10000, 10000,
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
    final Properties kafkaProducerProperties = makeProducerProperties();
    Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProducerProperties));
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

    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.<Module>of()
        ), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            }, new NamespacedExtractionModule(),
            new KafkaExtractionNamespaceModule()
            {
              @Override
              public Properties getProperties(
                  @Json ObjectMapper mapper,
                  Properties systemProperties
              )
              {
                final Properties consumerProperties = new Properties(kafkaProperties);
                consumerProperties.put("zookeeper.connect", zkTestServer.getConnectString() + "/kafka");
                consumerProperties.put("zookeeper.session.timeout.ms", "10000");
                consumerProperties.put("zookeeper.sync.time.ms", "200");
                return consumerProperties;
              }
            }
        )
    );
    renameManager = injector.getInstance(KafkaExtractionManager.class);

    log.info("--------------------------- placed default item via producer ---------------------------");
    extractionCacheManager = injector.getInstance(NamespaceExtractionCacheManager.class);
    extractionCacheManager.schedule(
        new KafkaExtractionNamespace(topicName, namespace)
    );
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
    lifecycle.stop();
    if (null != renameManager) {
      renameManager.stop();
    }

    if (null != kafkaServer) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }

    if (null != zkClient) {
      if (zkClient.exists("/kafka")) {
        try {
          zkClient.deleteRecursive("/kafka");
        }catch(org.I0Itec.zkclient.exception.ZkException ex){
          log.warn(ex, "error deleting /kafka zk node");
        }
      }
      zkClient.close();
    }
    if (null != zkTestServer) {
      zkTestServer.stop();
    }
    if(tmpDir.exists()){
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  private static final Properties makeProducerProperties(){
    final Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.putAll(kafkaProperties);
    kafkaProducerProperties.put(
        "metadata.broker.list",
        String.format("127.0.0.1:%d", kafkaServer.socketServer().port())
    );
    kafkaProperties.put("request.required.acks", "1");
    return kafkaProducerProperties;
  }

  private static void checkServer()
  {
    if (!kafkaServer.apis().controller().isActive()) {
      throw new ISE("server is not active!");
    }
  }

  //@Test(timeout = 5_000)
  @Test
  public void testSimpleRename() throws InterruptedException
  {
    final Properties kafkaProducerProperties = makeProducerProperties();
    final Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(new ProducerConfig(kafkaProducerProperties));
    try {
      checkServer();
      final ConcurrentMap<String, Function<String, String>> fnFn = injector.getInstance(Key.get(new TypeLiteral<ConcurrentMap<String, Function<String, String>>>(){}, Names.named("namespaceExtractionFunctionCache")));
      KafkaExtractionNamespace extractionNamespace = new KafkaExtractionNamespace(topicName, namespace);

      Assert.assertEquals(null, fnFn.get(extractionNamespace.getNamespace()).apply("foo"));

      long events = renameManager.getNumEvents(namespace);

      log.info("-------------------------     Sending foo bar     -------------------------------");
      producer.send(new KeyedMessage<byte[], byte[]>(topicName, StringUtils.toUtf8("foo"), StringUtils.toUtf8("bar")));

      long start = System.currentTimeMillis();
      while (events == renameManager.getNumEvents(namespace)) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking foo bar     -------------------------------");
      Assert.assertEquals("bar", fnFn.get(extractionNamespace.getNamespace()).apply("foo"));
      Assert.assertEquals(null, fnFn.get(extractionNamespace.getNamespace()).apply("baz"));

      checkServer();
      events = renameManager.getNumEvents(namespace);

      log.info("-------------------------     Sending baz bat     -------------------------------");
      producer.send(new KeyedMessage<byte[], byte[]>(topicName, StringUtils.toUtf8("baz"), StringUtils.toUtf8("bat")));
      while (events == renameManager.getNumEvents(namespace)) {
        Thread.sleep(10);
        if (System.currentTimeMillis() > start + 60_000) {
          throw new ISE("Took too long to update event");
        }
      }

      log.info("-------------------------     Checking baz bat     -------------------------------");
      Assert.assertEquals("bat", fnFn.get(extractionNamespace.getNamespace()).apply("baz"));
    }
    finally {
      producer.close();
    }
  }
}
