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

package org.apache.druid.indexing.kafka.test;

import com.google.common.collect.ImmutableMap;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import scala.Some;
import scala.collection.immutable.List$;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestBroker implements Closeable
{
  private static final Random RANDOM = ThreadLocalRandom.current();
  private final String zookeeperConnect;
  private final File directory;
  private final boolean directoryCleanup;
  private final int id;
  private final Map<String, Object> brokerProps;

  private volatile KafkaServer server;

  public TestBroker(
      String zookeeperConnect,
      @Nullable File directory,
      int id,
      Map<String, Object> brokerProps
  )
  {
    this.zookeeperConnect = zookeeperConnect;
    this.directory = directory == null ? FileUtils.createTempDir() : directory;
    this.directoryCleanup = directory == null;
    this.id = id;
    this.brokerProps = brokerProps == null ? ImmutableMap.of() : brokerProps;
  }

  public void start()
  {
    final Properties props = new Properties();
    props.setProperty("zookeeper.connect", zookeeperConnect);
    props.setProperty("zookeeper.session.timeout.ms", "30000");
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("log.dirs", directory.toString());
    props.setProperty("broker.id", String.valueOf(id));
    props.setProperty("port", String.valueOf(ThreadLocalRandom.current().nextInt(9999) + 10000));
    props.setProperty("advertised.host.name", "localhost");
    props.setProperty("transaction.state.log.replication.factor", "1");
    props.setProperty("offsets.topic.replication.factor", "1");
    props.setProperty("transaction.state.log.min.isr", "1");
    props.putAll(brokerProps);

    final KafkaConfig config = new KafkaConfig(props);

    server = new KafkaServer(
        config,
        Time.SYSTEM,
        Some.apply(StringUtils.format("TestingBroker[%d]-", id)),
        List$.MODULE$.empty()
    );
    server.startup();
  }

  public int getPort()
  {
    return server.socketServer().config().port();
  }

  public KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer<>(producerProperties());
  }

  public Admin newAdminClient()
  {
    return Admin.create(adminClientProperties());
  }

  Map<String, Object> adminClientProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    commonClientProperties(props);
    return props;
  }

  public KafkaConsumer<byte[], byte[]> newConsumer()
  {
    return new KafkaConsumer<>(consumerProperties());
  }

  public Map<String, Object> producerProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    commonClientProperties(props);
    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("transactional.id", String.valueOf(RANDOM.nextInt()));
    return props;
  }

  void commonClientProperties(Map<String, Object> props)
  {
    props.put("bootstrap.servers", StringUtils.format("localhost:%d", getPort()));
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = KafkaConsumerConfigs.getConsumerProperties();
    props.put("bootstrap.servers", StringUtils.format("localhost:%d", getPort()));
    return props;
  }

  @Override
  public void close() throws IOException
  {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
    if (directoryCleanup) {
      FileUtils.deleteDirectory(directory);
    }
  }
}
