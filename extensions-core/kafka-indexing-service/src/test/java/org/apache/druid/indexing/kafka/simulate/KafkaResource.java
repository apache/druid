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
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.KafkaContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Kafka container for use in embedded tests.
 */
public class KafkaResource extends TestcontainerResource<KafkaContainer>
{
  public KafkaResource()
  {
    super();
  }

  private static final String KAFKA_IMAGE = "apache/kafka-native:4.0.0";

  @Override
  protected KafkaContainer createContainer()
  {
    return new KafkaContainer(KAFKA_IMAGE);
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
   * Produces records to a topic of this embedded Kafka server.
   */
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

  private KafkaProducer<byte[], byte[]> newProducer()
  {
    return new KafkaProducer<>(producerProperties());
  }

  private Admin newAdminClient()
  {
    return Admin.create(commonClientProperties());
  }

  private Map<String, Object> commonClientProperties()
  {
    return Map.of("bootstrap.servers", getBootstrapServerUrl());
  }
}
