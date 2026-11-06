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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.ClaimShareInboxManifestsAction;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.ShareInboxClaimResult;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ShareGroupIndexTaskRunnerTest
{
  private static final SegmentIdWithShardSpec RESTORED_SEGMENT = new SegmentIdWithShardSpec(
      "test_datasource",
      Intervals.of("2025-01-01/2025-01-02"),
      "version",
      new NumberedShardSpec(3, 4)
  );

  private ObjectMapper mapper;
  private ShareGroupIndexTask task;
  private TaskToolbox toolbox;

  @BeforeEach
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    toolbox = Mockito.mock(TaskToolbox.class);

    final DataSchema dataSchema = DataSchema.builder()
                                            .withDataSource("test_datasource")
                                            .withTimestamp(new TimestampSpec("__time", null, null))
                                            .withDimensions(DimensionsSpec.EMPTY)
                                            .build();
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "test-share-group",
        ImmutableMap.of("bootstrap.servers", "localhost:9092"),
        null,
        null,
        "test-inbox",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
    );
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    task = new ShareGroupIndexTask(
        "task_runner_test",
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );
  }

  @Test
  public void testRequestWakeupIsNullSafeWhenNoActiveSupplier()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    runner.requestWakeup();
  }

  @Test
  public void testStopGracefullyBeforeRunTaskIsSafe()
  {
    Assertions.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assertions.assertTrue(task.isStopRequested());
  }

  @Test
  public void testRunnerAcceptsCustomSupplierFactory()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(
        task,
        toolbox,
        mapper,
        ioConfig -> Mockito.mock(AcknowledgingRecordSupplier.class)
    );
    Assertions.assertNotNull(runner);
    runner.requestWakeup();
  }

  @Test
  public void testInitializeInboxContextResolvesIdentityAndFingerprint()
  {
    final ShareGroupSourceIdentity identity = new ShareGroupSourceIdentity("cluster-id", "topic-id", "test-topic");
    final AtomicInteger resolutions = new AtomicInteger();
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(
        task,
        toolbox,
        mapper,
        null,
        ioConfig -> {
          resolutions.incrementAndGet();
          return identity;
        }
    );

    runner.initializeInboxContext(task.getIOConfig());

    Assertions.assertSame(identity, runner.getSourceIdentity());
    Assertions.assertEquals(64, runner.getSpecFingerprint().length());
    Assertions.assertEquals(1, resolutions.get());
  }

  @Test
  public void testDurableInboxLoopStagesBeforeAcceptAndStopsCleanly() throws Exception
  {
    final DurableInboxFakeSupplier supplier = new DurableInboxFakeSupplier();
    final AtomicInteger staged = new AtomicInteger();
    final ExecutorService stagingExecutor = Execs.directExecutor();
    final ExecutorService ownerExecutor = Executors.newSingleThreadExecutor();
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    try {
      final Future<TaskStatus> status = ownerExecutor.submit(
          () -> runner.runDurableInboxLoop(
              supplier,
              records -> staged.incrementAndGet(),
              stagingExecutor,
              task.getIOConfig()
          )
      );

      Assertions.assertTrue(supplier.accepted.await(10, TimeUnit.SECONDS));
      runner.requestWakeup();

      Assertions.assertTrue(status.get(10, TimeUnit.SECONDS).isSuccess());
      Assertions.assertEquals(1, staged.get());
      Assertions.assertEquals(1, supplier.flushCount.get());
      Assertions.assertTrue(supplier.wakeupCount.get() > 0);
    }
    finally {
      stagingExecutor.shutdownNow();
      ownerExecutor.shutdownNow();
    }
  }

  @Test
  public void testDurableInboxLoopsRunAcquisitionAndProcessorUntilGracefulStop() throws Exception
  {
    final DurableInboxFakeSupplier supplier = new DurableInboxFakeSupplier();
    final ExecutorService stagingExecutor = Execs.directExecutor();
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final ExecutorService ownerExecutor = Executors.newSingleThreadExecutor();
    final ScheduledExecutorService claimExecutor = Mockito.mock(ScheduledExecutorService.class);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(ClaimShareInboxManifestsAction.class)))
           .thenReturn(new ShareInboxClaimResult(List.of()));
    final ShareInboxProcessor processor = inboxProcessor(actionClient, claimExecutor);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    try {
      final Future<TaskStatus> status = ownerExecutor.submit(
          () -> runner.runDurableInboxLoops(
              supplier,
              records -> {},
              stagingExecutor,
              processor,
              processorExecutor,
              task.getIOConfig()
          )
      );

      Assertions.assertTrue(supplier.accepted.await(10, TimeUnit.SECONDS));
      runner.requestWakeup();

      Assertions.assertTrue(status.get(10, TimeUnit.SECONDS).isSuccess());
      Mockito.verify(actionClient, Mockito.atLeastOnce()).submit(Mockito.any(ClaimShareInboxManifestsAction.class));
    }
    finally {
      stagingExecutor.shutdownNow();
      processorExecutor.shutdownNow();
      ownerExecutor.shutdownNow();
    }
  }

  @Test
  public void testDurableInboxProcessorFailureStopsAcquisitionAndFailsTask() throws Exception
  {
    final DurableInboxFakeSupplier supplier = new DurableInboxFakeSupplier();
    final ExecutorService stagingExecutor = Execs.directExecutor();
    final ExecutorService processorExecutor = Executors.newSingleThreadExecutor();
    final ScheduledExecutorService claimExecutor = Mockito.mock(ScheduledExecutorService.class);
    final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(actionClient.submit(Mockito.any(ClaimShareInboxManifestsAction.class)))
           .thenThrow(new java.io.IOException("processor failure"));
    final ShareInboxProcessor processor = inboxProcessor(actionClient, claimExecutor);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    try {
      final java.io.IOException failure = Assertions.assertThrows(
          java.io.IOException.class,
          () -> runner.runDurableInboxLoops(
              supplier,
              records -> {},
              stagingExecutor,
              processor,
              processorExecutor,
              task.getIOConfig()
          )
      );

      Assertions.assertEquals("processor failure", failure.getMessage());
      Assertions.assertTrue(supplier.wakeupCount.get() > 0);
    }
    finally {
      stagingExecutor.shutdownNow();
      processorExecutor.shutdownNow();
    }
  }

  @Test
  public void testResolveMaxPollIntervalUsesConsumerConfiguration()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "test-share-group",
        Map.of("bootstrap.servers", "localhost:9092", "max.poll.interval.ms", 12_345),
        null,
        null,
        "test-inbox",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
    );

    Assertions.assertEquals(12_345L, runner.resolveMaxPollIntervalMs(ioConfig));
  }

  @Test
  public void testReacquiresSegmentLockForRestoredSegment() throws Exception
  {
    final ShareGroupIndexTask segmentLockTask = buildTask(
        "segment_lock_task",
        ImmutableMap.of(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false)
    );
    final TaskActionClient taskActionClient = Mockito.mock(TaskActionClient.class);
    final TaskLock taskLock = Mockito.mock(TaskLock.class);
    Mockito.when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    Mockito.when(taskActionClient.submit(Mockito.any(SegmentLockAcquireAction.class)))
           .thenReturn(LockResult.ok(taskLock, null));
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(segmentLockTask, toolbox, mapper);

    Assertions.assertTrue(runner.acquireLockForRestoredSegment(RESTORED_SEGMENT));

    final ArgumentCaptor<SegmentLockAcquireAction> actionCaptor =
        ArgumentCaptor.forClass(SegmentLockAcquireAction.class);
    Mockito.verify(taskActionClient).submit(actionCaptor.capture());
    final SegmentLockAcquireAction action = actionCaptor.getValue();
    Assertions.assertEquals(TaskLockType.APPEND, action.getLockType());
    Assertions.assertEquals(RESTORED_SEGMENT.getInterval(), action.getInterval());
    Assertions.assertEquals(RESTORED_SEGMENT.getVersion(), action.getVersion());
    Assertions.assertEquals(RESTORED_SEGMENT.getShardSpec().getPartitionNum(), action.getPartitionId());
    Assertions.assertEquals(1000L, action.getTimeoutMs());
  }

  @Test
  public void testRestoredSegmentLockAcquisitionFailureReturnsFalse() throws Exception
  {
    final ShareGroupIndexTask segmentLockTask = buildTask(
        "segment_lock_task",
        ImmutableMap.of(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false)
    );
    final TaskActionClient taskActionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    Mockito.when(taskActionClient.submit(Mockito.any(SegmentLockAcquireAction.class)))
           .thenReturn(LockResult.fail());
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(segmentLockTask, toolbox, mapper);

    Assertions.assertFalse(runner.acquireLockForRestoredSegment(RESTORED_SEGMENT));
  }

  @Test
  public void testReacquiresTimeChunkLockForRestoredSegment() throws Exception
  {
    final ShareGroupIndexTask timeChunkTask = buildTask(
        "time_chunk_lock_task",
        ImmutableMap.of(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true)
    );
    final TaskActionClient taskActionClient = Mockito.mock(TaskActionClient.class);
    final TaskLock taskLock = Mockito.mock(TaskLock.class);
    Mockito.when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    Mockito.when(taskActionClient.submit(Mockito.any(TimeChunkLockAcquireAction.class))).thenReturn(taskLock);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(timeChunkTask, toolbox, mapper);

    Assertions.assertTrue(runner.acquireLockForRestoredSegment(RESTORED_SEGMENT));

    final ArgumentCaptor<TimeChunkLockAcquireAction> actionCaptor =
        ArgumentCaptor.forClass(TimeChunkLockAcquireAction.class);
    Mockito.verify(taskActionClient).submit(actionCaptor.capture());
    final TimeChunkLockAcquireAction action = actionCaptor.getValue();
    Assertions.assertEquals(TaskLockType.APPEND, action.getType());
    Assertions.assertEquals(RESTORED_SEGMENT.getInterval(), action.getInterval());
    Assertions.assertEquals(1000L, action.getTimeoutMs());
    Mockito.verify(taskLock).assertNotRevoked();
  }

  @Test
  public void testRestoredTimeChunkLockAcquisitionFailureReturnsFalse() throws Exception
  {
    final ShareGroupIndexTask timeChunkTask = buildTask(
        "time_chunk_lock_task",
        ImmutableMap.of(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true)
    );
    final TaskActionClient taskActionClient = Mockito.mock(TaskActionClient.class);
    Mockito.when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    Mockito.when(taskActionClient.submit(Mockito.any(TimeChunkLockAcquireAction.class))).thenReturn(null);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(timeChunkTask, toolbox, mapper);

    Assertions.assertFalse(runner.acquireLockForRestoredSegment(RESTORED_SEGMENT));
  }

  private ShareGroupIndexTask buildTask(String id, Map<String, Object> context)
  {
    return new ShareGroupIndexTask(
        id,
        null,
        task.getDataSchema(),
        task.getTuningConfig(),
        task.getIOConfig(),
        context,
        mapper
    );
  }

  private ShareInboxProcessor inboxProcessor(
      TaskActionClient actionClient,
      ScheduledExecutorService claimExecutor
  )
  {
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    return new ShareInboxProcessor(
        actionClient,
        Mockito.mock(ShareInboxRecordSource.class),
        Mockito.mock(ShareInboxBatchHandler.class),
        claimExecutor,
        task.getDataSource(),
        ioConfig.getInboxId(),
        "fingerprint",
        task.getId(),
        ioConfig.getMaxProcessingManifests(),
        ioConfig.getMaxProcessingRecords(),
        ioConfig.getMaxProcessingBytes(),
        ioConfig.getClaimDurationMillis(),
        ioConfig.getClaimRenewalPeriodMillis(),
        ioConfig.getInboxPollPeriodMillis()
    );
  }

  private static class DurableInboxFakeSupplier
      implements AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
  {
    private final CountDownLatch accepted = new CountDownLatch(1);
    private final AtomicInteger pollCount = new AtomicInteger();
    private final AtomicInteger flushCount = new AtomicInteger();
    private final AtomicInteger wakeupCount = new AtomicInteger();

    @Override
    public void subscribe(Set<String> topics)
    {
    }

    @Override
    public void unsubscribe()
    {
    }

    @Override
    public Set<String> subscription()
    {
      return Set.of("test-topic");
    }

    @Override
    public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(long timeoutMs)
    {
      if (pollCount.getAndIncrement() > 0) {
        return List.of();
      }
      return List.of(KafkaShareGroupRecord.from(new ConsumerRecord<>(
          "test-topic",
          0,
          1L,
          new byte[]{1},
          new byte[]{2}
      )));
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset)
    {
      acknowledge(partitionId, offset, AcknowledgeType.ACCEPT);
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset, AcknowledgeType type)
    {
      if (type == AcknowledgeType.ACCEPT) {
        accepted.countDown();
      }
    }

    @Override
    public void acknowledge(Map<KafkaTopicPartition, Collection<Long>> offsets, AcknowledgeType type)
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
      flushCount.incrementAndGet();
      return Map.of(new KafkaTopicPartition(true, "test-topic", 0), Optional.empty());
    }

    @Override
    public Map<KafkaTopicPartition, Optional<Exception>> commitSync()
    {
      return flushAcknowledgementsSync();
    }

    @Override
    public Set<KafkaTopicPartition> getPartitionIds(String stream)
    {
      return Set.of();
    }

    @Override
    public void wakeup()
    {
      wakeupCount.incrementAndGet();
    }

    @Override
    public Optional<Integer> acquisitionLockTimeoutMs()
    {
      return Optional.of(30_000);
    }

    @Override
    public void close()
    {
    }
  }
}
