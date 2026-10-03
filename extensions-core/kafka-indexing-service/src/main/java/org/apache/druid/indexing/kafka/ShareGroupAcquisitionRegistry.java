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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

final class ShareGroupAcquisitionRegistry
{
  enum Observation
  {
    ADDED,
    REFRESHED,
    SATURATED
  }

  enum State
  {
    NEW,
    UPLOADING,
    DURABLE,
    ACCEPT_PENDING,
    RELEASE_PENDING
  }

  static final class UploadAttempt
  {
    private final ShareGroupRecordIdentity identity;
    private final long attemptId;

    private UploadAttempt(ShareGroupRecordIdentity identity, long attemptId)
    {
      this.identity = identity;
      this.attemptId = attemptId;
    }

    ShareGroupRecordIdentity getIdentity()
    {
      return identity;
    }

    long getAttemptId()
    {
      return attemptId;
    }
  }

  static final class Snapshot
  {
    private final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> latestAppearance;
    private final State state;
    private final long estimatedBytes;
    private final long attemptId;
    private final long firstAcquiredNanos;
    private final long lastAppearanceNanos;
    private final long renewAtNanos;

    private Snapshot(Entry entry)
    {
      this.latestAppearance = entry.latestAppearance;
      this.state = entry.state;
      this.estimatedBytes = entry.estimatedBytes;
      this.attemptId = entry.attemptId;
      this.firstAcquiredNanos = entry.firstAcquiredNanos;
      this.lastAppearanceNanos = entry.lastAppearanceNanos;
      this.renewAtNanos = entry.renewAtNanos;
    }

    OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> getLatestAppearance()
    {
      return latestAppearance;
    }

    State getState()
    {
      return state;
    }

    long getEstimatedBytes()
    {
      return estimatedBytes;
    }

    long getAttemptId()
    {
      return attemptId;
    }

    long getFirstAcquiredNanos()
    {
      return firstAcquiredNanos;
    }

    long getLastAppearanceNanos()
    {
      return lastAppearanceNanos;
    }

    long getRenewAtNanos()
    {
      return renewAtNanos;
    }
  }

  private static final class Entry
  {
    private OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> latestAppearance;
    private final long estimatedBytes;
    private final long firstAcquiredNanos;
    private long lastAppearanceNanos;
    private long renewAtNanos;
    private State state = State.NEW;
    private long attemptId;

    private Entry(
        OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> latestAppearance,
        long estimatedBytes,
        long nowNanos,
        long renewAtNanos
    )
    {
      this.latestAppearance = latestAppearance;
      this.estimatedBytes = estimatedBytes;
      this.firstAcquiredNanos = nowNanos;
      this.lastAppearanceNanos = nowNanos;
      this.renewAtNanos = renewAtNanos;
    }
  }

  private final int maxRecords;
  private final long maxBytes;
  private final double renewalFraction;
  private final LongSupplier nanoTime;
  private final Map<ShareGroupRecordIdentity, Entry> entries = new HashMap<>();
  private long retainedBytes;
  private long nextAttemptId;

  ShareGroupAcquisitionRegistry(
      int maxRecords,
      long maxBytes,
      double renewalFraction,
      LongSupplier nanoTime
  )
  {
    Preconditions.checkArgument(maxRecords > 0, "maxRecords must be positive");
    Preconditions.checkArgument(maxBytes > 0, "maxBytes must be positive");
    Preconditions.checkArgument(
        renewalFraction > 0 && renewalFraction <= 1,
        "renewalFraction must be in (0, 1]"
    );
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
    this.renewalFraction = renewalFraction;
    this.nanoTime = Preconditions.checkNotNull(nanoTime, "nanoTime");
  }

  Observation observe(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record,
      long estimatedBytes,
      long acquisitionLockTimeoutMs,
      long maxPollIntervalMs
  )
  {
    Preconditions.checkArgument(estimatedBytes >= 0, "estimatedBytes must not be negative");
    Preconditions.checkArgument(maxPollIntervalMs > 0, "maxPollIntervalMs must be positive");

    final ShareGroupRecordIdentity identity = ShareGroupRecordIdentity.from(record);
    final long nowNanos = nanoTime.getAsLong();
    final long renewAtNanos = renewalDeadline(nowNanos, acquisitionLockTimeoutMs, maxPollIntervalMs);
    final Entry existing = entries.get(identity);
    if (existing != null) {
      existing.latestAppearance = record;
      existing.lastAppearanceNanos = nowNanos;
      existing.renewAtNanos = renewAtNanos;
      return Observation.REFRESHED;
    }

    if (entries.size() >= maxRecords || estimatedBytes > maxBytes - retainedBytes) {
      return Observation.SATURATED;
    }

    entries.put(identity, new Entry(record, estimatedBytes, nowNanos, renewAtNanos));
    retainedBytes += estimatedBytes;
    return Observation.ADDED;
  }

  UploadAttempt startUpload(ShareGroupRecordIdentity identity)
  {
    final Entry entry = requireEntry(identity);
    Preconditions.checkState(entry.state == State.NEW, "Record[%s] is not new", identity);
    entry.state = State.UPLOADING;
    entry.attemptId = ++nextAttemptId;
    return new UploadAttempt(identity, entry.attemptId);
  }

  boolean markDurable(UploadAttempt attempt)
  {
    final Entry entry = entries.get(attempt.identity);
    if (entry == null || entry.state != State.UPLOADING || entry.attemptId != attempt.attemptId) {
      return false;
    }
    entry.state = State.DURABLE;
    return true;
  }

  boolean markFailed(UploadAttempt attempt)
  {
    final Entry entry = entries.get(attempt.identity);
    if (entry == null || entry.state != State.UPLOADING || entry.attemptId != attempt.attemptId) {
      return false;
    }
    entry.state = State.RELEASE_PENDING;
    return true;
  }

  Optional<AcknowledgeType> acknowledgementFor(ShareGroupRecordIdentity identity, boolean stopping)
  {
    final Entry entry = requireEntry(identity);
    if (entry.state == State.DURABLE || entry.state == State.ACCEPT_PENDING) {
      return Optional.of(AcknowledgeType.ACCEPT);
    }
    if (stopping || entry.state == State.RELEASE_PENDING) {
      return Optional.of(AcknowledgeType.RELEASE);
    }
    if (nanoTime.getAsLong() >= entry.renewAtNanos) {
      return Optional.of(AcknowledgeType.RENEW);
    }
    return Optional.empty();
  }

  long nanosUntilAcknowledgement(ShareGroupRecordIdentity identity, boolean stopping)
  {
    if (acknowledgementFor(identity, stopping).isPresent()) {
      return 0;
    }
    return Math.max(0, requireEntry(identity).renewAtNanos - nanoTime.getAsLong());
  }

  void acknowledgementAssigned(ShareGroupRecordIdentity identity, AcknowledgeType type)
  {
    final Entry entry = requireEntry(identity);
    if (type == AcknowledgeType.ACCEPT) {
      Preconditions.checkState(
          entry.state == State.DURABLE || entry.state == State.ACCEPT_PENDING,
          "Record[%s] is not durable",
          identity
      );
      entry.state = State.ACCEPT_PENDING;
    } else if (type == AcknowledgeType.RELEASE) {
      entry.state = State.RELEASE_PENDING;
    } else {
      Preconditions.checkArgument(type == AcknowledgeType.RENEW, "Unsupported acknowledgement[%s]", type);
    }
  }

  void acknowledgementsFlushed(Map<KafkaTopicPartition, Optional<Exception>> results)
  {
    final Iterator<Map.Entry<ShareGroupRecordIdentity, Entry>> iterator = entries.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<ShareGroupRecordIdentity, Entry> registryEntry = iterator.next();
      final State state = registryEntry.getValue().state;
      if (state != State.ACCEPT_PENDING && state != State.RELEASE_PENDING) {
        continue;
      }

      if (acknowledgementSucceeded(results, registryEntry.getKey())) {
        retainedBytes -= registryEntry.getValue().estimatedBytes;
        iterator.remove();
      }
    }
  }

  Optional<Snapshot> get(ShareGroupRecordIdentity identity)
  {
    final Entry entry = entries.get(identity);
    return entry == null ? Optional.empty() : Optional.of(new Snapshot(entry));
  }

  int size()
  {
    return entries.size();
  }

  long retainedBytes()
  {
    return retainedBytes;
  }

  private long renewalDeadline(long nowNanos, long acquisitionLockTimeoutMs, long maxPollIntervalMs)
  {
    if (acquisitionLockTimeoutMs <= 0) {
      return nowNanos;
    }
    final double lockDelayMs = acquisitionLockTimeoutMs * renewalFraction;
    final double pollDelayMs = maxPollIntervalMs * 0.5;
    final long delayNanos = (long) (Math.min(lockDelayMs, pollDelayMs) * 1_000_000L);
    return nowNanos + delayNanos;
  }

  private Entry requireEntry(ShareGroupRecordIdentity identity)
  {
    final Entry entry = entries.get(identity);
    return Preconditions.checkNotNull(entry, "Unknown record[%s]", identity);
  }

  private static boolean acknowledgementSucceeded(
      Map<KafkaTopicPartition, Optional<Exception>> results,
      ShareGroupRecordIdentity identity
  )
  {
    for (Map.Entry<KafkaTopicPartition, Optional<Exception>> result : results.entrySet()) {
      final KafkaTopicPartition partition = result.getKey();
      if (partition.partition() == identity.getPartition()
          && (!partition.topic().isPresent() || partition.topic().get().equals(identity.getTopic()))) {
        return !result.getValue().isPresent();
      }
    }
    return false;
  }
}
