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

package org.apache.druid.indexing.kafka;

import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KafkaShareGroupRecordSupplier} using a mocked
 * KafkaShareConsumer. These tests verify the wrapping logic without
 * requiring a real Kafka broker.
 */
public class KafkaShareGroupRecordSupplierTest
{
  private KafkaShareConsumer<byte[], byte[]> mockConsumer;
  private KafkaShareGroupRecordSupplier supplier;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp()
  {
    mockConsumer = mock(KafkaShareConsumer.class);
    supplier = new KafkaShareGroupRecordSupplier(mockConsumer);
  }

  @After
  public void tearDown()
  {
    supplier.close();
  }

  @Test
  public void testSubscribeAndSubscription()
  {
    final Set<String> topics = Set.of("topic-a", "topic-b");
    when(mockConsumer.subscription()).thenReturn(topics);

    supplier.subscribe(topics);
    verify(mockConsumer).subscribe(topics);

    Assert.assertEquals(topics, supplier.subscription());
  }

  @Test
  public void testUnsubscribe()
  {
    supplier.unsubscribe();
    verify(mockConsumer).unsubscribe();
  }

  @Test
  public void testPollWrapsRecords()
  {
    final ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(
        "test-topic", 0, 100L, "key1".getBytes(), "value1".getBytes()
    );
    final ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(
        "test-topic", 1, 200L, "key2".getBytes(), "value2".getBytes()
    );

    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordMap = new HashMap<>();
    recordMap.put(new TopicPartition("test-topic", 0), List.of(record1));
    recordMap.put(new TopicPartition("test-topic", 1), List.of(record2));
    final ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordMap);

    when(mockConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> result =
        supplier.poll(1000);

    Assert.assertEquals(2, result.size());

    // Verify first record
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> polled1 =
        result.stream().filter(r -> r.getSequenceNumber() == 100L).findFirst().orElse(null);
    Assert.assertNotNull(polled1);
    Assert.assertEquals("test-topic", polled1.getStream());
    Assert.assertEquals(0, polled1.getPartitionId().partition());
    Assert.assertNotNull(polled1.getData());
    Assert.assertEquals(1, polled1.getData().size());

    // Verify second record
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> polled2 =
        result.stream().filter(r -> r.getSequenceNumber() == 200L).findFirst().orElse(null);
    Assert.assertNotNull(polled2);
    Assert.assertEquals(1, polled2.getPartitionId().partition());
  }

  @Test
  public void testPollWithNullValue()
  {
    final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "test-topic", 0, 50L, "key".getBytes(), null
    );
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordMap = new HashMap<>();
    recordMap.put(new TopicPartition("test-topic", 0), List.of(record));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(recordMap));

    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> result =
        supplier.poll(1000);

    Assert.assertEquals(1, result.size());
    Assert.assertNull(result.get(0).getData());
  }

  @Test
  public void testPollReturnsEmptyOnTimeout()
  {
    when(mockConsumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> result =
        supplier.poll(100);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testAcknowledgeDefaultAccept()
  {
    final KafkaTopicPartition partition = new KafkaTopicPartition(true, "test-topic", 0);
    supplier.acknowledge(partition, 42L);

    final ArgumentCaptor<ConsumerRecord> recordCaptor = ArgumentCaptor.forClass(ConsumerRecord.class);
    final ArgumentCaptor<org.apache.kafka.clients.consumer.AcknowledgeType> typeCaptor =
        ArgumentCaptor.forClass(org.apache.kafka.clients.consumer.AcknowledgeType.class);

    verify(mockConsumer).acknowledge(recordCaptor.capture(), typeCaptor.capture());
    Assert.assertEquals(42L, recordCaptor.getValue().offset());
    Assert.assertEquals("test-topic", recordCaptor.getValue().topic());
    Assert.assertEquals(0, recordCaptor.getValue().partition());
    Assert.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT, typeCaptor.getValue());
  }

  @Test
  public void testAcknowledgeWithRelease()
  {
    final KafkaTopicPartition partition = new KafkaTopicPartition(true, "test-topic", 0);
    supplier.acknowledge(partition, 10L, AcknowledgeType.RELEASE);

    final ArgumentCaptor<org.apache.kafka.clients.consumer.AcknowledgeType> typeCaptor =
        ArgumentCaptor.forClass(org.apache.kafka.clients.consumer.AcknowledgeType.class);
    verify(mockConsumer).acknowledge(any(ConsumerRecord.class), typeCaptor.capture());
    Assert.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE, typeCaptor.getValue());
  }

  @Test
  public void testAcknowledgeWithReject()
  {
    final KafkaTopicPartition partition = new KafkaTopicPartition(true, "test-topic", 0);
    supplier.acknowledge(partition, 10L, AcknowledgeType.REJECT);

    final ArgumentCaptor<org.apache.kafka.clients.consumer.AcknowledgeType> typeCaptor =
        ArgumentCaptor.forClass(org.apache.kafka.clients.consumer.AcknowledgeType.class);
    verify(mockConsumer).acknowledge(any(ConsumerRecord.class), typeCaptor.capture());
    Assert.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.REJECT, typeCaptor.getValue());
  }

  @Test
  public void testAcknowledgeBatch()
  {
    final KafkaTopicPartition p0 = new KafkaTopicPartition(true, "test-topic", 0);
    final KafkaTopicPartition p1 = new KafkaTopicPartition(true, "test-topic", 1);

    final Map<KafkaTopicPartition, java.util.Collection<Long>> offsets = new HashMap<>();
    offsets.put(p0, Arrays.asList(1L, 2L, 3L));
    offsets.put(p1, Arrays.asList(10L, 11L));

    supplier.acknowledge(offsets, AcknowledgeType.ACCEPT);

    // 3 + 2 = 5 individual acknowledge calls
    verify(mockConsumer, Mockito.times(5)).acknowledge(
        any(ConsumerRecord.class),
        eq(org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT)
    );
  }

  @Test
  public void testCommitSync()
  {
    final TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    final Map<TopicIdPartition, Optional<KafkaException>> kafkaResult = new HashMap<>();
    kafkaResult.put(tip, Optional.empty());

    when(mockConsumer.commitSync()).thenReturn(kafkaResult);

    final Map<KafkaTopicPartition, Optional<Exception>> result = supplier.commitSync();
    Assert.assertEquals(1, result.size());

    final Map.Entry<KafkaTopicPartition, Optional<Exception>> entry = result.entrySet().iterator().next();
    Assert.assertEquals("test-topic", entry.getKey().topic());
    Assert.assertEquals(0, entry.getKey().partition());
    Assert.assertFalse(entry.getValue().isPresent());
  }

  @Test
  public void testCommitSyncWithError()
  {
    final TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    final KafkaException error = new KafkaException("commit failed");
    final Map<TopicIdPartition, Optional<KafkaException>> kafkaResult = new HashMap<>();
    kafkaResult.put(tip, Optional.of(error));

    when(mockConsumer.commitSync()).thenReturn(kafkaResult);

    final Map<KafkaTopicPartition, Optional<Exception>> result = supplier.commitSync();
    Assert.assertEquals(1, result.size());

    final Optional<Exception> maybeError = result.values().iterator().next();
    Assert.assertTrue(maybeError.isPresent());
    Assert.assertEquals("commit failed", maybeError.get().getMessage());
  }

  @Test
  public void testCloseIsIdempotent()
  {
    supplier.close();
    supplier.close();
    verify(mockConsumer, Mockito.times(1)).close();
  }
}
