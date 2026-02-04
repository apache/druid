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

import org.apache.druid.indexing.kafka.KafkaConsumerConfigs;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Kafka container for use in embedded tests.
 * <p>
 * {@link #KAFKA_IMAGE} can be overriden via system property to use a different Kafka Docker image.
 * </p>
 */
public class KafkaResource extends TestcontainerResource<KafkaContainer>
{
  /**
   * Kafka Docker image used in embedded tests. The image name is
   * read from the system property {@code druid.testing.kafka.image} and
   * defaults to {@code apache/kafka}. Environments that cannot run that
   * image should set the system property to {@code apache/kafka-native}.
   */
  private static final String KAFKA_IMAGE = System.getProperty("druid.testing.kafka.image", "apache/kafka:4.1.1");

  private EmbeddedDruidCluster cluster;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  @Override
  protected KafkaContainer createContainer()
  {
    // The result of getBootstrapServers() is the first entry in KafkaContainer.advertisedListeners.
    // Override getBootstrapServers() to ensure that both DruidContainers and
    // EmbeddedDruidServers can connect to the Kafka brokers.
    return new KafkaContainer(KAFKA_IMAGE) {
      @Override
      public String getBootstrapServers()
      {
        return cluster.getEmbeddedHostname().useInHostAndPort(super.getBootstrapServers());
      }
    };
  }

  public String getBootstrapServerUrl()
  {
    ensureRunning();
    return getContainer().getBootstrapServers();
  }

  public Map<String, Object> consumerProperties()
  {
    final Map<String, Object> props = new HashMap<>(KafkaConsumerConfigs.getConsumerProperties());
    props.put("bootstrap.servers", getBootstrapServerUrl());
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

  public Set<String> listTopics()
  {
    try (Admin admin = newAdminClient()) {
      return admin.listTopics().names().get();
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

  /**
   * Increases the number of partitions in the given Kakfa topic. The topic must
   * already exist. This method waits until the increase in the partition count
   * has started (but not necessarily finished).
   */
  public void increasePartitionsInTopic(String topic, int newPartitionCount)
  {
    try (Admin admin = newAdminClient()) {
      final CreatePartitionsResult result = admin.createPartitions(
          Map.of(topic, NewPartitions.increaseTo(newPartitionCount))
      );

      // Wait for the partitioning to start
      result.values().get(topic).get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void produceRecordsToTopic(
      List<ProducerRecord<byte[], byte[]>> records,
      Map<String, Object> extraProducerProperties
  )
  {
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = newProducer(extraProducerProperties)) {
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

  /**
   * Produces records to a topic of this embedded Kafka server.
   */
  public void produceRecordsToTopic(List<ProducerRecord<byte[], byte[]>> records)
  {
    produceRecordsToTopic(records, null);
  }

  /**
   * Produces records to a topic of this embedded Kafka server without using
   * Kafka transactions.
   */
  public void produceRecordsWithoutTransaction(List<ProducerRecord<byte[], byte[]>> records)
  {
    final Map<String, Object> props = producerProperties();
    props.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(props)) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, Object> producerProperties()
  {
    final Map<String, Object> props = new HashMap<>(commonClientProperties());

    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    props.put("transactional.id", String.valueOf(ThreadLocalRandom.current().nextInt()));

    return props;
  }

  public Admin newAdminClient()
  {
    return Admin.create(commonClientProperties());
  }

  @Override
  public String toString()
  {
    return "KafkaResource";
  }

  private KafkaProducer<byte[], byte[]> newProducer(Map<String, Object> extraProperties)
  {
    final Map<String, Object> producerProperties = new HashMap<>(producerProperties());
    if (extraProperties != null) {
      producerProperties.putAll(extraProperties);
    }
    return new KafkaProducer<>(producerProperties);
  }

  private Map<String, Object> commonClientProperties()
  {
    return Map.of("bootstrap.servers", getBootstrapServerUrl());
  }
}
