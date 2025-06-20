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

package org.apache.druid.indexing.kafka.simulate;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.simulate.EmbeddedResource;
import org.apache.druid.testing.simulate.EmbeddedZookeeper;
import org.apache.druid.testing.simulate.TestFolder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import scala.Some;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Embedded Kafka server used in Druid simulation tests.
 * Contains some logic common with {@link org.apache.druid.indexing.kafka.test.TestBroker}.
 */
public class EmbeddedKafkaServer implements EmbeddedResource
{
  private static final Random RANDOM = ThreadLocalRandom.current();

  private final EmbeddedZookeeper zk;
  private final TestFolder testFolder;
  private final Map<String, String> brokerProperties;

  private volatile KafkaServer server;
  private volatile String bootstrapServerUrl;

  public EmbeddedKafkaServer(
      EmbeddedZookeeper zk,
      TestFolder testFolder,
      Map<String, String> brokerProperties
  )
  {
    this.zk = zk;
    this.testFolder = testFolder;
    this.brokerProperties = brokerProperties == null ? Map.of() : brokerProperties;
  }

  @Override
  public void start() throws IOException
  {
    final Properties props = new Properties();
    props.setProperty("zookeeper.connect", zk.getConnectString());
    props.setProperty("zookeeper.session.timeout.ms", "30000");
    props.setProperty("zookeeper.connection.timeout.ms", "30000");
    props.setProperty("log.dirs", testFolder.newFolder().toString());
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
    bootstrapServerUrl = StringUtils.format("localhost:%d", getPort());
  }

  @Override
  public void stop()
  {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = new HashMap<>(KafkaConsumerConfigs.getConsumerProperties());
    props.put("bootstrap.servers", bootstrapServerUrl);
    return props;
  }

  public void createTopicWithPartitions(String topicName, int numPartitions)
  {
    try (Admin admin = newAdminClient()) {
      admin.createTopics(
          List.of(new NewTopic(topicName, numPartitions, (short) 1))
      ).all().get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteTopic(String topicName)
  {
    try (Admin admin = newAdminClient()) {
      admin.deleteTopics(List.of(topicName)).all().get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: are headers needed in the records?
  public void produceRecordsToTopic(List<ProducerRecord<byte[], byte[]>> records)
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = newProducer()) {
      kafkaProducer.initTransactions();
      kafkaProducer.beginTransaction();
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record);
      }
      kafkaProducer.commitTransaction();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int getPort()
  {
    return server.advertisedListeners().apply(0).port();
  }

  private KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer<>(producerProperties());
  }

  private Admin newAdminClient()
  {
    return Admin.create(commonClientProperties());
  }

  private Map<String, Object> producerProperties()
  {
    final Map<String, Object> props = new HashMap<>(commonClientProperties());

    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("transactional.id", String.valueOf(RANDOM.nextInt()));

    return props;
  }

  private Map<String, Object> commonClientProperties()
  {
    return Map.of("bootstrap.servers", bootstrapServerUrl);
  }
}
