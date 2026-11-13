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
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongFunction;

final class ShareGroupAcquisitionLoop
{
  private static final class StagingCompletion
  {
    private static final StagingCompletion STOP = new StagingCompletion(List.of(), false, true);

    private final List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts;
    private final boolean successful;
    private final boolean stop;

    private StagingCompletion(
        List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts,
        boolean successful,
        boolean stop
    )
    {
      this.attempts = List.copyOf(attempts);
      this.successful = successful;
      this.stop = stop;
    }

    private static StagingCompletion success(List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts)
    {
      return new StagingCompletion(attempts, true, false);
    }

    private static StagingCompletion failure(List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts)
    {
      return new StagingCompletion(attempts, false, false);
    }
  }

  private final String topic;
  private final AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier;
  private final ShareGroupBatchStager batchStager;
  private final Executor stagingExecutor;
  private final ShareGroupAcquisitionRegistry registry;
  private final ToLongFunction<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> byteEstimator;
  private final long pollTimeoutMs;
  private final long maxPollIntervalMs;
  private final BlockingQueue<StagingCompletion> completions = new LinkedBlockingQueue<>();
  private final AtomicBoolean stopRequested = new AtomicBoolean();
  private Thread ownerThread;

  ShareGroupAcquisitionLoop(
      String topic,
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      ShareGroupBatchStager batchStager,
      Executor stagingExecutor,
      ShareGroupAcquisitionRegistry registry,
      ToLongFunction<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> byteEstimator,
      long pollTimeoutMs,
      long maxPollIntervalMs
  )
  {
    Preconditions.checkArgument(pollTimeoutMs >= 0, "pollTimeoutMs must not be negative");
    Preconditions.checkArgument(maxPollIntervalMs > 0, "maxPollIntervalMs must be positive");
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.recordSupplier = Preconditions.checkNotNull(recordSupplier, "recordSupplier");
    this.batchStager = Preconditions.checkNotNull(batchStager, "batchStager");
    this.stagingExecutor = Preconditions.checkNotNull(stagingExecutor, "stagingExecutor");
    this.registry = Preconditions.checkNotNull(registry, "registry");
    this.byteEstimator = Preconditions.checkNotNull(byteEstimator, "byteEstimator");
    this.pollTimeoutMs = pollTimeoutMs;
    this.maxPollIntervalMs = maxPollIntervalMs;
  }

  void run() throws Exception
  {
    checkOwnerThread();
    recordSupplier.subscribe(Set.of(topic));
    while (!stopRequested.get()) {
      try {
        runOnce();
      }
      catch (WakeupException e) {
        if (!stopRequested.get()) {
          throw e;
        }
      }
    }
  }

  void runOnce() throws InterruptedException
  {
    checkOwnerThread();
    drainCompletions();
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records =
        recordSupplier.poll(pollTimeoutMs);
    drainCompletions();
    if (records.isEmpty()) {
      return;
    }

    final long acquisitionLockTimeoutMs = recordSupplier.acquisitionLockTimeoutMs().orElse(-1);
    final Map<ShareGroupRecordIdentity,
        OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> tracked = new LinkedHashMap<>();
    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> saturated =
        new ArrayList<>();
    final Map<KafkaTopicPartition,
        List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>>> newRecordsByPartition =
        new LinkedHashMap<>();

    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
      final ShareGroupRecordIdentity identity = ShareGroupRecordIdentity.from(record);
      final ShareGroupAcquisitionRegistry.Observation observation = registry.observe(
          record,
          byteEstimator.applyAsLong(record),
          acquisitionLockTimeoutMs,
          maxPollIntervalMs
      );
      if (observation == ShareGroupAcquisitionRegistry.Observation.SATURATED) {
        saturated.add(record);
      } else {
        tracked.put(identity, record);
        if (observation == ShareGroupAcquisitionRegistry.Observation.ADDED) {
          newRecordsByPartition.computeIfAbsent(record.getPartitionId(), ignored -> new ArrayList<>()).add(record);
        }
      }
    }

    for (List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> partitionRecords
        : newRecordsByPartition.values()) {
      submit(partitionRecords);
    }

    waitForAcknowledgementDecisions(tracked.keySet());

    boolean terminalAcknowledgementAssigned = false;
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : saturated) {
      recordSupplier.acknowledge(
          record.getPartitionId(),
          record.getSequenceNumber(),
          AcknowledgeType.RELEASE
      );
      terminalAcknowledgementAssigned = true;
    }
    for (Map.Entry<ShareGroupRecordIdentity,
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> trackedRecord : tracked.entrySet()) {
      final AcknowledgeType type = registry.acknowledgementFor(
          trackedRecord.getKey(),
          stopRequested.get()
      ).orElseThrow(() -> new IllegalStateException("No acknowledgement decision for " + trackedRecord.getKey()));
      final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = trackedRecord.getValue();
      recordSupplier.acknowledge(record.getPartitionId(), record.getSequenceNumber(), type);
      registry.acknowledgementAssigned(trackedRecord.getKey(), type);
      terminalAcknowledgementAssigned |= type != AcknowledgeType.RENEW;
    }

    if (terminalAcknowledgementAssigned) {
      registry.acknowledgementsFlushed(recordSupplier.flushAcknowledgementsSync());
    }
  }

  void requestStop()
  {
    stopRequested.set(true);
    completions.offer(StagingCompletion.STOP);
    recordSupplier.wakeup();
  }

  private void submit(
      List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records
  )
  {
    final List<ShareGroupAcquisitionRegistry.UploadAttempt> attempts = new ArrayList<>(records.size());
    for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
      attempts.add(registry.startUpload(ShareGroupRecordIdentity.from(record)));
    }

    final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> immutableRecords =
        List.copyOf(records);
    final Runnable stagingTask = () -> {
      try {
        batchStager.stage(immutableRecords);
        completions.add(StagingCompletion.success(attempts));
      }
      catch (Exception ignored) {
        completions.add(StagingCompletion.failure(attempts));
      }
    };
    try {
      stagingExecutor.execute(stagingTask);
    }
    catch (RejectedExecutionException e) {
      for (ShareGroupAcquisitionRegistry.UploadAttempt attempt : attempts) {
        registry.markFailed(attempt);
      }
    }
  }

  private void waitForAcknowledgementDecisions(Set<ShareGroupRecordIdentity> identities)
      throws InterruptedException
  {
    while (true) {
      drainCompletions();
      boolean ready = true;
      long waitNanos = Long.MAX_VALUE;
      for (ShareGroupRecordIdentity identity : identities) {
        if (!registry.acknowledgementFor(identity, stopRequested.get()).isPresent()) {
          ready = false;
          waitNanos = Math.min(
              waitNanos,
              registry.nanosUntilAcknowledgement(identity, stopRequested.get())
          );
        }
      }
      if (ready) {
        return;
      }

      final StagingCompletion completion = completions.poll(waitNanos, TimeUnit.NANOSECONDS);
      if (completion != null) {
        applyCompletion(completion);
      }
    }
  }

  private void drainCompletions()
  {
    StagingCompletion completion;
    while ((completion = completions.poll()) != null) {
      applyCompletion(completion);
    }
  }

  private void applyCompletion(StagingCompletion completion)
  {
    if (completion.stop) {
      return;
    }
    for (ShareGroupAcquisitionRegistry.UploadAttempt attempt : completion.attempts) {
      if (completion.successful) {
        registry.markDurable(attempt);
      } else {
        registry.markFailed(attempt);
      }
    }
  }

  private void checkOwnerThread()
  {
    if (ownerThread == null) {
      ownerThread = Thread.currentThread();
    } else {
      Preconditions.checkState(
          ownerThread == Thread.currentThread(),
          "Acquisition loop must remain on its owner thread"
      );
    }
  }
}
