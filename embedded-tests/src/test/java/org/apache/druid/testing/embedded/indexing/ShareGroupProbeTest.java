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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Diagnostic probe: bypasses Druid entirely and uses {@link KafkaShareConsumer}
 * directly against the testcontainers broker to verify share groups work.
 */
public class ShareGroupProbeTest
{
  private ShareGroupKafkaResource kafkaServer;

  @BeforeEach
  public void setUp()
  {
    kafkaServer = new ShareGroupKafkaResource();
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    cluster.addResource(kafkaServer);
    kafkaServer.beforeStart(cluster);
    kafkaServer.start();
    kafkaServer.onStarted(cluster);
  }

  @AfterEach
  public void tearDown()
  {
    if (kafkaServer != null) {
      kafkaServer.stop();
    }
  }

  @Test
  public void probe_mimicDruid_pollLoop100ms_areConsumed() throws Exception
  {
    final String topic = "probe_topic_mimic_" + System.currentTimeMillis();
    final String groupId = "probe_group_mimic_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 2);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    // Build props EXACTLY like Druid's KafkaShareGroupRecordSupplier does:
    // - start from kafkaServer.consumerProperties() (KafkaConsumerConfigs.getConsumerProperties + bootstrap)
    // - sanitize (strips enable.auto.commit/auto.offset.reset/group.instance.id)
    // - set group.id and share.acknowledgement.mode
    final Map<String, Object> raw = new HashMap<>(kafkaServer.consumerProperties());
    final Map<String, Object> sanitized =
        org.apache.druid.indexing.kafka.ShareGroupConsumerProperties.sanitize(raw);
    final Properties props = new Properties();
    for (Map.Entry<String, Object> e : sanitized.entrySet()) {
      props.setProperty(e.getKey(), String.valueOf(e.getValue()));
    }
    props.setProperty("group.id", groupId);
    props.setProperty("share.acknowledgement.mode", "explicit");

    final AtomicInteger received = new AtomicInteger();

    Thread producerThread = new Thread(() -> {
      try {
        Thread.sleep(3_000);
        final java.util.ArrayList<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> records =
            new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
          records.add(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, i % 2, null, ("v-" + i).getBytes()));
        }
        kafkaServer.produceRecordsToTopic(records);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "probe-producer");
    producerThread.start();

    try (KafkaShareConsumer<byte[], byte[]> consumer =
             new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(topic));

      final long deadlineMs = System.currentTimeMillis() + 30_000;
      while (received.get() < 10 && System.currentTimeMillis() < deadlineMs) {
        final ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<byte[], byte[]> r : polled) {
          received.incrementAndGet();
          consumer.acknowledge(r, AcknowledgeType.ACCEPT);
        }
        consumer.commitSync();
      }
    }

    producerThread.join(2_000);
    Assertions.assertEquals(10, received.get(), "Expected 10 records via Druid-mimicking probe");
  }

  @Test
  public void probe_noWarmup_transactionalProducer_areConsumed() throws Exception
  {
    final String topic = "probe_topic_nowarmup_" + System.currentTimeMillis();
    final String groupId = "probe_group_nowarmup_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 2);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    final Properties props = new Properties();
    props.put("bootstrap.servers", kafkaServer.getBootstrapServerUrl());
    props.put("group.id", groupId);
    props.put("share.acknowledgement.mode", "explicit");

    final AtomicInteger received = new AtomicInteger();

    try (KafkaShareConsumer<byte[], byte[]> consumer =
             new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(topic));

      Thread.sleep(3_000);

      final java.util.ArrayList<org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>> records =
          new java.util.ArrayList<>();
      for (int i = 0; i < 10; i++) {
        records.add(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, i % 2, null, ("value-" + i).getBytes()));
      }
      kafkaServer.produceRecordsToTopic(records);

      final long deadlineMs = System.currentTimeMillis() + 30_000;
      while (received.get() < 10 && System.currentTimeMillis() < deadlineMs) {
        final ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<byte[], byte[]> r : polled) {
          received.incrementAndGet();
          consumer.acknowledge(r, AcknowledgeType.ACCEPT);
        }
        consumer.commitSync();
      }
    }

    Assertions.assertEquals(10, received.get(), "Expected 10 records via KafkaShareConsumer (no warmup, transactional producer)");
  }

  @Test
  public void probe_recordsProducedAfterSubscribe_areConsumed() throws Exception
  {
    final String topic = "probe_topic_" + System.currentTimeMillis();
    final String groupId = "probe_group_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 2);

    try (Admin admin = kafkaServer.newAdminClient()) {
      final ConfigResource brokerCfg = new ConfigResource(ConfigResource.Type.BROKER, "1");
      final DescribeConfigsResult res = admin.describeConfigs(List.of(brokerCfg));
      for (ConfigEntry e : res.all().get().get(brokerCfg).entries()) {
        if (e.name().startsWith("group.share")) {
          System.out.println("BROKER_CFG: " + e.name() + "=" + e.value() + " source=" + e.source());
        }
      }
    }

    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

    try (Admin admin = kafkaServer.newAdminClient()) {
      final ConfigResource grp = new ConfigResource(ConfigResource.Type.GROUP, groupId);
      final DescribeConfigsResult res = admin.describeConfigs(List.of(grp));
      for (ConfigEntry e : res.all().get().get(grp).entries()) {
        if (e.name().startsWith("share")) {
          System.out.println("GROUP_CFG: " + e.name() + "=" + e.value() + " source=" + e.source());
        }
      }
    }

    final Properties props = new Properties();
    props.put("bootstrap.servers", kafkaServer.getBootstrapServerUrl());
    props.put("group.id", groupId);
    props.put("share.acknowledgement.mode", "explicit");

    final AtomicInteger received = new AtomicInteger();

    try (KafkaShareConsumer<byte[], byte[]> consumer =
             new KafkaShareConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(topic));

      final long subscribeAt = System.currentTimeMillis();
      while (System.currentTimeMillis() - subscribeAt < 3_000) {
        consumer.poll(Duration.ofMillis(200));
      }

      final Map<String, Object> producerProps = new HashMap<>();
      producerProps.put("bootstrap.servers", kafkaServer.getBootstrapServerUrl());
      producerProps.put("key.serializer", ByteArraySerializer.class.getName());
      producerProps.put("value.serializer", ByteArraySerializer.class.getName());
      producerProps.put("acks", "all");

      try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
        for (int i = 0; i < 10; i++) {
          producer.send(new ProducerRecord<>(topic, i % 2, null, ("value-" + i).getBytes()));
        }
        producer.flush();
      }

      final long deadlineMs = System.currentTimeMillis() + 30_000;
      while (received.get() < 10 && System.currentTimeMillis() < deadlineMs) {
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<byte[], byte[]> r : records) {
          received.incrementAndGet();
          consumer.acknowledge(r, AcknowledgeType.ACCEPT);
        }
        consumer.commitSync();
      }
    }

    Assertions.assertEquals(10, received.get(), "Expected 10 records via KafkaShareConsumer");
  }
}
