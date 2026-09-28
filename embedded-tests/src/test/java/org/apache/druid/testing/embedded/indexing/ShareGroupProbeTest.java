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
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    kafkaServer = new ShareGroupKafkaResource(5_000L);
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

    // Mirror Druid's KafkaShareGroupRecordSupplier props pipeline.
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
          records.add(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, i % 2, null, ("v-" + i).getBytes(StandardCharsets.UTF_8)));
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
        records.add(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, i % 2, null, ("value-" + i).getBytes(StandardCharsets.UTF_8)));
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
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");

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
          producer.send(new ProducerRecord<>(topic, i % 2, null, ("value-" + i).getBytes(StandardCharsets.UTF_8)));
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

  @Test
  public void probe_renewedRecord_reappearsWithoutNewDelivery() throws Exception
  {
    final String topic = "probe_renew_" + System.currentTimeMillis();
    final String groupId = "probe_renew_group_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");
    kafkaServer.produceRecordsToTopic(List.of(
        new ProducerRecord<>(topic, 0, null, "renew-value".getBytes(StandardCharsets.UTF_8))
    ));

    try (KafkaShareConsumer<byte[], byte[]> consumer = new KafkaShareConsumer<>(
        shareConsumerProperties(groupId),
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
    )) {
      consumer.subscribe(List.of(topic));
      final ConsumerRecord<byte[], byte[]> initial = pollSingleRecord(consumer, 20_000L);
      final short initialDeliveryCount = initial.deliveryCount().orElseThrow();

      ConsumerRecord<byte[], byte[]> current = initial;
      for (int i = 0; i < 3; i++) {
        consumer.acknowledge(current, AcknowledgeType.RENEW);
        assertCommitSucceeded(consumer.commitSync());

        current = pollSingleRecord(consumer, 15_000L);
        Assertions.assertEquals(initial.topic(), current.topic());
        Assertions.assertEquals(initial.partition(), current.partition());
        Assertions.assertEquals(initial.offset(), current.offset());
        Assertions.assertEquals(initialDeliveryCount, current.deliveryCount().orElseThrow());
      }

      consumer.acknowledge(current, AcknowledgeType.ACCEPT);
      assertCommitSucceeded(consumer.commitSync());
    }
  }

  @Test
  public void probe_expiredAcquisition_isRedeliveredToAnotherConsumer() throws Exception
  {
    final String topic = "probe_expiry_" + System.currentTimeMillis();
    final String groupId = "probe_expiry_group_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");
    kafkaServer.produceRecordsToTopic(List.of(
        new ProducerRecord<>(topic, 0, null, "expiry-value".getBytes(StandardCharsets.UTF_8))
    ));

    try (
        KafkaShareConsumer<byte[], byte[]> first = new KafkaShareConsumer<>(
            shareConsumerProperties(groupId),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        );
        KafkaShareConsumer<byte[], byte[]> second = new KafkaShareConsumer<>(
            shareConsumerProperties(groupId),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )
    ) {
      first.subscribe(List.of(topic));
      second.subscribe(List.of(topic));

      final ConsumerRecord<byte[], byte[]> acquired = pollSingleRecord(first, 20_000L);
      final int lockDurationMs = first.acquisitionLockTimeoutMs().orElseThrow();

      final ConsumerRecords<byte[], byte[]> beforeExpiry = second.poll(Duration.ofMillis(lockDurationMs / 2L));
      Assertions.assertTrue(beforeExpiry.isEmpty());

      final ConsumerRecord<byte[], byte[]> redelivered = pollSingleRecord(second, lockDurationMs + 10_000L);
      Assertions.assertEquals(acquired.topic(), redelivered.topic());
      Assertions.assertEquals(acquired.partition(), redelivered.partition());
      Assertions.assertEquals(acquired.offset(), redelivered.offset());
      Assertions.assertEquals((short) 2, redelivered.deliveryCount().orElseThrow());

      second.acknowledge(redelivered, AcknowledgeType.ACCEPT);
      assertCommitSucceeded(second.commitSync());
    }
  }

  @Test
  public void probe_releasedRecord_isImmediatelyRedelivered() throws Exception
  {
    final String topic = "probe_release_" + System.currentTimeMillis();
    final String groupId = "probe_release_group_" + System.currentTimeMillis();

    kafkaServer.createTopicWithPartitions(topic, 1);
    kafkaServer.setShareGroupAutoOffsetReset(groupId, "earliest");
    kafkaServer.produceRecordsToTopic(List.of(
        new ProducerRecord<>(topic, 0, null, "release-value".getBytes(StandardCharsets.UTF_8))
    ));

    try (
        KafkaShareConsumer<byte[], byte[]> first = new KafkaShareConsumer<>(
            shareConsumerProperties(groupId),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        );
        KafkaShareConsumer<byte[], byte[]> second = new KafkaShareConsumer<>(
            shareConsumerProperties(groupId),
            new ByteArrayDeserializer(),
            new ByteArrayDeserializer()
        )
    ) {
      first.subscribe(List.of(topic));
      second.subscribe(List.of(topic));

      final ConsumerRecord<byte[], byte[]> acquired = pollSingleRecord(first, 20_000L);
      first.acknowledge(acquired, AcknowledgeType.RELEASE);
      assertCommitSucceeded(first.commitSync());

      final ConsumerRecord<byte[], byte[]> redelivered = pollSingleRecord(second, 15_000L);
      Assertions.assertEquals(acquired.topic(), redelivered.topic());
      Assertions.assertEquals(acquired.partition(), redelivered.partition());
      Assertions.assertEquals(acquired.offset(), redelivered.offset());
      Assertions.assertEquals((short) 2, redelivered.deliveryCount().orElseThrow());

      second.acknowledge(redelivered, AcknowledgeType.ACCEPT);
      assertCommitSucceeded(second.commitSync());
    }
  }

  private Properties shareConsumerProperties(String groupId)
  {
    final Properties props = new Properties();
    props.put("bootstrap.servers", kafkaServer.getBootstrapServerUrl());
    props.put("group.id", groupId);
    props.put("share.acknowledgement.mode", "explicit");
    props.put("share.acquire.mode", "record_limit");
    props.put("max.poll.records", "1");
    return props;
  }

  private ConsumerRecord<byte[], byte[]> pollSingleRecord(
      KafkaShareConsumer<byte[], byte[]> consumer,
      long timeoutMs
  )
  {
    final long deadlineMs = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadlineMs) {
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250L));
      if (!records.isEmpty()) {
        Assertions.assertEquals(1, records.count());
        return records.iterator().next();
      }
    }
    Assertions.fail("Expected one share-group record within " + timeoutMs + " ms");
    return null;
  }

  private void assertCommitSucceeded(Map<?, ? extends Optional<?>> result)
  {
    Assertions.assertTrue(result.values().stream().allMatch(Optional::isEmpty), result.toString());
  }
}
