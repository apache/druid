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
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ShareGroupAcquisitionLoopTest
{
  @Test
  public void testRepeatedRenewalsStartOneStagingOperation() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.empty());
    for (int i = 0; i < 11; i++) {
      supplier.addPoll(record("topic", 0, 1));
    }
    final RecordingExecutor executor = new RecordingExecutor();
    final AtomicInteger stageCount = new AtomicInteger();
    final ShareGroupAcquisitionLoop loop = loop(
        supplier,
        records -> stageCount.incrementAndGet(),
        executor,
        registry(10, 1_000)
    );

    for (int i = 0; i < 10; i++) {
      loop.runOnce();
    }
    Assertions.assertEquals(1, executor.size());
    Assertions.assertEquals(0, stageCount.get());
    Assertions.assertEquals(10, supplier.acknowledgements.size());
    Assertions.assertTrue(
        supplier.acknowledgements.stream().allMatch(ack -> ack.type == AcknowledgeType.RENEW)
    );

    executor.runNext();
    loop.runOnce();

    Assertions.assertEquals(1, stageCount.get());
    Assertions.assertEquals(AcknowledgeType.ACCEPT, supplier.acknowledgements.get(10).type);
    Assertions.assertEquals(1, supplier.flushCount);
  }

  @Test
  public void testSaturatedRecordIsReleasedWithoutSecondSubmission() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.empty());
    supplier.addPoll(record("topic", 0, 1), record("topic", 0, 2));
    final RecordingExecutor executor = new RecordingExecutor();
    final ShareGroupAcquisitionLoop loop = loop(
        supplier,
        records -> {},
        executor,
        registry(1, 1_000)
    );

    loop.runOnce();

    Assertions.assertEquals(1, executor.size());
    Assertions.assertEquals(AcknowledgeType.RELEASE, supplier.acknowledgement(2).type);
    Assertions.assertEquals(AcknowledgeType.RENEW, supplier.acknowledgement(1).type);
    Assertions.assertEquals(1, supplier.flushCount);
  }

  @Test
  public void testByteSaturationReleasesRecord() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.empty());
    supplier.addPoll(record("topic", 0, 1), record("topic", 0, 2));
    final ShareGroupAcquisitionRegistry registry = registry(2, 1);
    final RecordingExecutor executor = new RecordingExecutor();
    final ShareGroupAcquisitionLoop loop = new ShareGroupAcquisitionLoop(
        "topic",
        supplier,
        records -> {},
        executor,
        registry,
        ignored -> 1,
        1,
        60_000
    );

    loop.runOnce();

    Assertions.assertEquals(1, executor.size());
    Assertions.assertEquals(AcknowledgeType.RELEASE, supplier.acknowledgement(2).type);
  }

  @Test
  public void testExecutorRejectionReleasesRecord() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.of(60_000));
    supplier.addPoll(record("topic", 0, 1));
    final Executor rejectingExecutor = command -> {
      throw new RejectedExecutionException("full");
    };
    final ShareGroupAcquisitionRegistry registry = registry(1, 1_000);
    final ShareGroupAcquisitionLoop loop = loop(supplier, records -> {}, rejectingExecutor, registry);

    loop.runOnce();

    Assertions.assertEquals(AcknowledgeType.RELEASE, supplier.acknowledgement(1).type);
    Assertions.assertEquals(1, supplier.flushCount);
    Assertions.assertEquals(0, registry.size());
  }

  @Test
  public void testWorkerSuccessIsAcceptedOnOwnerThread() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.of(60_000));
    supplier.addPoll(record("topic", 0, 1));
    final AtomicReference<Thread> stagingThread = new AtomicReference<>();
    final ExecutorService stagingExecutor = Executors.newSingleThreadExecutor();
    try {
      final ShareGroupAcquisitionLoop loop = loop(
          supplier,
          records -> stagingThread.set(Thread.currentThread()),
          stagingExecutor,
          registry(1, 1_000)
      );

      loop.runOnce();

      Assertions.assertEquals(AcknowledgeType.ACCEPT, supplier.acknowledgement(1).type);
      Assertions.assertNotSame(stagingThread.get(), supplier.acknowledgement(1).thread);
      Assertions.assertEquals(Set.of(Thread.currentThread()), supplier.consumerThreads);
    }
    finally {
      stagingExecutor.shutdownNow();
    }
  }

  @Test
  public void testWorkerFailureIsReleasedOnOwnerThread() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.of(60_000));
    supplier.addPoll(record("topic", 0, 1));
    final ExecutorService stagingExecutor = Executors.newSingleThreadExecutor();
    try {
      final ShareGroupAcquisitionLoop loop = loop(
          supplier,
          records -> {
            throw new IllegalStateException("upload failed");
          },
          stagingExecutor,
          registry(1, 1_000)
      );

      loop.runOnce();

      Assertions.assertEquals(AcknowledgeType.RELEASE, supplier.acknowledgement(1).type);
      Assertions.assertEquals(Set.of(Thread.currentThread()), supplier.consumerThreads);
    }
    finally {
      stagingExecutor.shutdownNow();
    }
  }

  @Test
  public void testStopDuringUploadReleasesAndFlushes() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.of(60_000));
    supplier.addPoll(record("topic", 0, 1));
    final RecordingExecutor stagingExecutor = new RecordingExecutor();
    final ShareGroupAcquisitionLoop loop = loop(
        supplier,
        records -> {},
        stagingExecutor,
        registry(1, 1_000)
    );
    final ExecutorService ownerExecutor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> run = ownerExecutor.submit(() -> {
        try {
          loop.runOnce();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      Assertions.assertTrue(supplier.pollReturned.await(10, TimeUnit.SECONDS));

      loop.requestStop();
      run.get(10, TimeUnit.SECONDS);

      Assertions.assertEquals(AcknowledgeType.RELEASE, supplier.acknowledgement(1).type);
      Assertions.assertEquals(1, supplier.flushCount);
      Assertions.assertEquals(1, supplier.wakeupCount);
    }
    finally {
      ownerExecutor.shutdownNow();
    }
  }

  @Test
  public void testFailedFlushRetainsTerminalEntry() throws Exception
  {
    final ScriptedSupplier supplier = new ScriptedSupplier(Optional.of(60_000));
    supplier.flushFailure = new IllegalStateException("flush failed");
    supplier.addPoll(record("topic", 0, 1));
    final ShareGroupAcquisitionRegistry registry = registry(1, 1_000);
    final ShareGroupAcquisitionLoop loop = loop(supplier, records -> {}, Runnable::run, registry);

    loop.runOnce();

    Assertions.assertEquals(1, registry.size());
    Assertions.assertEquals(
        ShareGroupAcquisitionRegistry.State.ACCEPT_PENDING,
        registry.get(new ShareGroupRecordIdentity("topic", 0, 1)).orElseThrow().getState()
    );
  }

  private static ShareGroupAcquisitionLoop loop(
      ScriptedSupplier supplier,
      ShareGroupBatchStager stager,
      Executor executor,
      ShareGroupAcquisitionRegistry registry
  )
  {
    return new ShareGroupAcquisitionLoop(
        "topic",
        supplier,
        stager,
        executor,
        registry,
        ignored -> 1,
        1,
        60_000
    );
  }

  private static ShareGroupAcquisitionRegistry registry(int maxRecords, long maxBytes)
  {
    final AtomicLong nanoTime = new AtomicLong();
    return new ShareGroupAcquisitionRegistry(maxRecords, maxBytes, 0.5, nanoTime::get);
  }

  @SafeVarargs
  private static List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records(
      OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>... records
  )
  {
    return List.of(records);
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
        new KafkaTopicPartition(true, topic, partition),
        offset,
        List.of(new KafkaRecordEntity(consumerRecord))
    );
  }

  private static final class RecordingExecutor implements Executor
  {
    private final Deque<Runnable> commands = new ArrayDeque<>();

    @Override
    public void execute(Runnable command)
    {
      commands.add(command);
    }

    int size()
    {
      return commands.size();
    }

    void runNext()
    {
      commands.remove().run();
    }
  }

  private static final class RecordedAcknowledgement
  {
    private final long offset;
    private final AcknowledgeType type;
    private final Thread thread;

    private RecordedAcknowledgement(long offset, AcknowledgeType type, Thread thread)
    {
      this.offset = offset;
      this.type = type;
      this.thread = thread;
    }
  }

  private static final class ScriptedSupplier
      implements AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
  {
    private final Optional<Integer> lockTimeoutMs;
    private final Deque<List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>>> polls =
        new ArrayDeque<>();
    private final List<RecordedAcknowledgement> acknowledgements = new ArrayList<>();
    private final Set<Thread> consumerThreads = new HashSet<>();
    private final CountDownLatch pollReturned = new CountDownLatch(1);
    private Set<String> subscription = Set.of();
    private int flushCount;
    private int wakeupCount;
    private Exception flushFailure;

    private ScriptedSupplier(Optional<Integer> lockTimeoutMs)
    {
      this.lockTimeoutMs = lockTimeoutMs;
    }

    @SafeVarargs
    final void addPoll(OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>... pollRecords)
    {
      polls.add(records(pollRecords));
    }

    RecordedAcknowledgement acknowledgement(long offset)
    {
      return acknowledgements.stream()
                             .filter(acknowledgement -> acknowledgement.offset == offset)
                             .findFirst()
                             .orElseThrow();
    }

    @Override
    public void subscribe(Set<String> topics)
    {
      consumerThreads.add(Thread.currentThread());
      subscription = Set.copyOf(topics);
    }

    @Override
    public void unsubscribe()
    {
      consumerThreads.add(Thread.currentThread());
      subscription = Set.of();
    }

    @Override
    public Set<String> subscription()
    {
      consumerThreads.add(Thread.currentThread());
      return subscription;
    }

    @Override
    public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(long timeoutMs)
    {
      consumerThreads.add(Thread.currentThread());
      final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records =
          polls.isEmpty() ? List.of() : polls.remove();
      pollReturned.countDown();
      return records;
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset)
    {
      acknowledge(partitionId, offset, AcknowledgeType.ACCEPT);
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset, AcknowledgeType type)
    {
      consumerThreads.add(Thread.currentThread());
      acknowledgements.add(new RecordedAcknowledgement(offset, type, Thread.currentThread()));
    }

    @Override
    public void acknowledge(
        Map<KafkaTopicPartition, Collection<Long>> offsets,
        AcknowledgeType type
    )
    {
      for (Map.Entry<KafkaTopicPartition, Collection<Long>> entry : offsets.entrySet()) {
        for (Long offset : entry.getValue()) {
          acknowledge(entry.getKey(), offset, type);
        }
      }
    }

    @Override
    public Map<KafkaTopicPartition, Optional<Exception>> flushAcknowledgementsSync()
    {
      consumerThreads.add(Thread.currentThread());
      flushCount++;
      final Map<KafkaTopicPartition, Optional<Exception>> result = new HashMap<>();
      for (RecordedAcknowledgement acknowledgement : acknowledgements) {
        result.put(
            new KafkaTopicPartition(true, "topic", 0),
            Optional.ofNullable(flushFailure)
        );
      }
      return result;
    }

    @Override
    public Map<KafkaTopicPartition, Optional<Exception>> commitSync()
    {
      return flushAcknowledgementsSync();
    }

    @Override
    public Set<KafkaTopicPartition> getPartitionIds(String stream)
    {
      consumerThreads.add(Thread.currentThread());
      return Set.of();
    }

    @Override
    public void wakeup()
    {
      wakeupCount++;
    }

    @Override
    public Optional<Integer> acquisitionLockTimeoutMs()
    {
      consumerThreads.add(Thread.currentThread());
      return lockTimeoutMs;
    }

    @Override
    public void close()
    {
      consumerThreads.add(Thread.currentThread());
    }
  }
}
