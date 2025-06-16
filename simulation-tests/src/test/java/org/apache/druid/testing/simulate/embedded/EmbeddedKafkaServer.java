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

package org.apache.druid.testing.simulate.embedded;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import scala.Some;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class EmbeddedKafkaServer extends ExternalResource
{
  private static final Random RANDOM = ThreadLocalRandom.current();

  private final EmbeddedZookeeper zk;
  private final TemporaryFolder tempDir;
  private final Map<String, String> brokerProperties;

  private volatile KafkaServer server;

  EmbeddedKafkaServer(
      EmbeddedZookeeper zk,
      TemporaryFolder tempDir,
      Map<String, String> brokerProperties
  )
  {
    this.zk = zk;
    this.tempDir = tempDir;
    this.brokerProperties = brokerProperties == null ? Map.of() : brokerProperties;
  }

  @Override
  protected void before() throws IOException
  {
    final Properties props = new Properties();
    props.setProperty("zookeeper.connect", zk.getConnectString());
    props.setProperty("zookeeper.session.timeout.ms", "30000");
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("log.dirs", tempDir.newFolder().toString());
    props.setProperty("broker.id", String.valueOf(1));
    props.setProperty("port", String.valueOf(ThreadLocalRandom.current().nextInt(9999) + 10000));
    props.setProperty("advertised.host.name", "localhost");
    props.setProperty("transaction.state.log.replication.factor", "1");
    props.setProperty("offsets.topic.replication.factor", "1");
    props.setProperty("transaction.state.log.min.isr", "1");
    props.putAll(brokerProperties);

    final KafkaConfig config = new KafkaConfig(props);

    server = new KafkaServer(
        config,
        Time.SYSTEM,
        Some.apply(StringUtils.format("EmbeddedKafka[1]-")),
        false
    );
    server.startup();
  }

  @Override
  protected void after()
  {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
  }

  public int getPort()
  {
    return server.advertisedListeners().apply(0).port();
  }

  public KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer<>(producerProperties());
  }

  public Admin newAdminClient()
  {
    return Admin.create(adminClientProperties());
  }

  private Map<String, Object> adminClientProperties()
  {
    final Map<String, Object> props = new HashMap<>();
    commonClientProperties(props);
    return props;
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

  private void commonClientProperties(Map<String, Object> props)
  {
    props.put("bootstrap.servers", StringUtils.format("localhost:%d", getPort()));
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = KafkaConsumerConfigs.getConsumerProperties();
    props.put("bootstrap.servers", StringUtils.format("localhost:%d", getPort()));
    return props;
  }
}
