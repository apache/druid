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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ShareGroupAcquisitionRegistryTest
{
  private static final long LOCK_TIMEOUT_MS = 10_000;
  private static final long MAX_POLL_INTERVAL_MS = 60_000;

  private final AtomicLong nanoTime = new AtomicLong();

  @Test
  public void testFirstAppearanceReservesCapacity()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);

    Assertions.assertEquals(
        ShareGroupAcquisitionRegistry.Observation.ADDED,
        registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS)
    );
    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
    final ShareGroupAcquisitionRegistry.Snapshot snapshot = registry.get(identity(record)).orElseThrow();
    Assertions.assertSame(record, snapshot.getLatestAppearance());
    Assertions.assertEquals(ShareGroupAcquisitionRegistry.State.NEW, snapshot.getState());
    Assertions.assertEquals(40, snapshot.getEstimatedBytes());
    Assertions.assertEquals(0, snapshot.getFirstAcquiredNanos());
  }

  @Test
  public void testRepeatedAppearanceRefreshesOneEntryAndOneAttempt()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> first = record("topic", 0, 1);
    registry.observe(first, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(first));

    OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> latest = first;
    for (int i = 0; i < 10; i++) {
      nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
      latest = record("topic", 0, 1);
      Assertions.assertEquals(
          ShareGroupAcquisitionRegistry.Observation.REFRESHED,
          registry.observe(latest, 99, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS)
      );
    }

    final ShareGroupAcquisitionRegistry.Snapshot snapshot = registry.get(identity(first)).orElseThrow();
    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
    Assertions.assertSame(latest, snapshot.getLatestAppearance());
    Assertions.assertEquals(attempt.getAttemptId(), snapshot.getAttemptId());
    Assertions.assertEquals(ShareGroupAcquisitionRegistry.State.UPLOADING, snapshot.getState());
    Assertions.assertEquals(TimeUnit.SECONDS.toNanos(1), snapshot.getLastAppearanceNanos());
  }

  @Test
  public void testRecordLimitReturnsSaturatedWithoutGrowingRegistry()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    registry.observe(record("topic", 0, 1), 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);

    Assertions.assertEquals(
        ShareGroupAcquisitionRegistry.Observation.SATURATED,
        registry.observe(record("topic", 0, 2), 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS)
    );
    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
  }

  @Test
  public void testByteLimitReturnsSaturatedWithoutGrowingRegistry()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 60);
    registry.observe(record("topic", 0, 1), 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);

    Assertions.assertEquals(
        ShareGroupAcquisitionRegistry.Observation.SATURATED,
        registry.observe(record("topic", 0, 2), 21, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS)
    );
    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
  }

  @Test
  public void testSuccessfulCompletionTransitionsToDurable()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(record));

    Assertions.assertTrue(registry.markDurable(attempt));
    Assertions.assertEquals(
        AcknowledgeType.ACCEPT,
        registry.acknowledgementFor(identity(record), false).orElseThrow()
    );
    registry.acknowledgementAssigned(identity(record), AcknowledgeType.ACCEPT);
    Assertions.assertEquals(
        ShareGroupAcquisitionRegistry.State.ACCEPT_PENDING,
        registry.get(identity(record)).orElseThrow().getState()
    );
  }

  @Test
  public void testFailureTransitionsToReleasePending()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(record));

    Assertions.assertTrue(registry.markFailed(attempt));
    Assertions.assertEquals(
        AcknowledgeType.RELEASE,
        registry.acknowledgementFor(identity(record), false).orElseThrow()
    );
  }

  @Test
  public void testStaleCompletionCannotFinishNewAttempt()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    final ShareGroupRecordIdentity identity = identity(record);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt staleAttempt = registry.startUpload(identity);
    registry.markFailed(staleAttempt);
    registry.acknowledgementAssigned(identity, AcknowledgeType.RELEASE);
    registry.acknowledgementsFlushed(Map.of(record.getPartitionId(), Optional.empty()));

    registry.observe(record("topic", 0, 1), 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt currentAttempt = registry.startUpload(identity);

    Assertions.assertFalse(registry.markDurable(staleAttempt));
    Assertions.assertTrue(registry.markDurable(currentAttempt));
  }

  @Test
  public void testRenewalStartsAtConfiguredThreshold()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    registry.startUpload(identity(record));

    nanoTime.set(TimeUnit.MILLISECONDS.toNanos(4_999));
    Assertions.assertFalse(registry.acknowledgementFor(identity(record), false).isPresent());
    nanoTime.set(TimeUnit.MILLISECONDS.toNanos(5_000));
    Assertions.assertEquals(
        AcknowledgeType.RENEW,
        registry.acknowledgementFor(identity(record), false).orElseThrow()
    );
  }

  @Test
  public void testRenewalIsCappedByMaxPollInterval()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, 60_000, 4_000);

    Assertions.assertEquals(
        TimeUnit.MILLISECONDS.toNanos(2_000),
        registry.get(identity(record)).orElseThrow().getRenewAtNanos()
    );
  }

  @Test
  public void testUnknownLockDurationRenewsImmediately()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, -1, MAX_POLL_INTERVAL_MS);
    registry.startUpload(identity(record));

    Assertions.assertEquals(
        AcknowledgeType.RENEW,
        registry.acknowledgementFor(identity(record), false).orElseThrow()
    );
  }

  @Test
  public void testStoppingReleasesNonDurableRecord()
  {
    final ShareGroupAcquisitionRegistry registry = registry(1, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    registry.startUpload(identity(record));

    Assertions.assertEquals(
        AcknowledgeType.RELEASE,
        registry.acknowledgementFor(identity(record), true).orElseThrow()
    );
  }

  @Test
  public void testSuccessfulTerminalFlushReleasesCapacity()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(record));
    registry.markDurable(attempt);
    registry.acknowledgementAssigned(identity(record), AcknowledgeType.ACCEPT);

    registry.acknowledgementsFlushed(Map.of(record.getPartitionId(), Optional.empty()));

    Assertions.assertEquals(0, registry.size());
    Assertions.assertEquals(0, registry.retainedBytes());
  }

  @Test
  public void testFailedTerminalFlushRetainsRecord()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(record));
    registry.markDurable(attempt);
    registry.acknowledgementAssigned(identity(record), AcknowledgeType.ACCEPT);

    registry.acknowledgementsFlushed(
        Map.of(record.getPartitionId(), Optional.of(new IllegalStateException("failed")))
    );

    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
  }

  @Test
  public void testMissingTerminalFlushResultRetainsRecord()
  {
    final ShareGroupAcquisitionRegistry registry = registry(2, 100);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 0, 1);
    registry.observe(record, 40, LOCK_TIMEOUT_MS, MAX_POLL_INTERVAL_MS);
    final ShareGroupAcquisitionRegistry.UploadAttempt attempt = registry.startUpload(identity(record));
    registry.markDurable(attempt);
    registry.acknowledgementAssigned(identity(record), AcknowledgeType.ACCEPT);

    registry.acknowledgementsFlushed(Map.of());

    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(40, registry.retainedBytes());
  }

  @Test
  public void testSingleTopicIdentityUsesRecordStream()
  {
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record("topic", 3, 7);

    final ShareGroupRecordIdentity identity = ShareGroupRecordIdentity.from(record);

    Assertions.assertEquals("topic", identity.getTopic());
    Assertions.assertEquals(3, identity.getPartition());
    Assertions.assertEquals(7, identity.getOffset());
  }

  private ShareGroupAcquisitionRegistry registry(int maxRecords, long maxBytes)
  {
    return new ShareGroupAcquisitionRegistry(maxRecords, maxBytes, 0.5, nanoTime::get);
  }

  private static ShareGroupRecordIdentity identity(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record
  )
  {
    return ShareGroupRecordIdentity.from(record);
  }

  private static OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record(
      String topic,
      int partition,
      long offset
  )
  {
    final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
        topic,
        partition,
        offset,
        null,
        new byte[]{1}
    );
    return new OrderedPartitionableRecord<>(
        topic,
        new KafkaTopicPartition(false, null, partition),
        offset,
        List.of(new KafkaRecordEntity(consumerRecord))
    );
  }
}
