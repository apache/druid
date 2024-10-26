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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SegmentAllocationQueueTest
{
  private static final Logger log = new Logger(SegmentAllocationQueueTest.class);

  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private SegmentAllocationQueue allocationQueue;

  private StubServiceEmitter emitter;
  private BlockingExecutorService executor;

  @Before
  public void setUp()
  {
    executor = new BlockingExecutorService("alloc-test-exec");
    emitter = new StubServiceEmitter("overlord", "alloc-test");

    final TaskLockConfig lockConfig = new TaskLockConfig()
    {
      @Override
      public boolean isBatchSegmentAllocation()
      {
        return true;
      }

      @Override
      public long getBatchAllocationWaitTime()
      {
        return 0;
      }

      @Override
      public boolean isSegmentAllocationReduceMetadataIO()
      {
        return true;
      }
    };

    allocationQueue = new SegmentAllocationQueue(
        taskActionTestKit.getTaskLockbox(),
        lockConfig,
        taskActionTestKit.getMetadataStorageCoordinator(),
        emitter,
        (corePoolSize, nameFormat)
            -> new WrappingScheduledExecutorService(nameFormat, executor, false)
    );
    allocationQueue.start();
    allocationQueue.becomeLeader();
  }

  @After
  public void tearDown()
  {
    if (allocationQueue != null) {
      allocationQueue.stop();
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    emitter.flush();
  }

  @Test
  @Ignore
  public void testLongQueue() throws Exception
  {
    allocationQueue.start();
    allocationQueue.becomeLeader();
    final Task task = NoopTask.create();
    taskActionTestKit.getTaskLockbox().add(task);

    final Interval startInterval = Intervals.of("2024-01-01/PT1H");
    final long hourToMillis = 1000L * 60L * 60L;
    final long numHours = 48;
    List<Interval> intervals = new ArrayList<>();
    for (long hour = 0; hour < numHours; hour++) {
      intervals.add(
          new Interval(
              startInterval.getStartMillis() + hourToMillis * hour,
              startInterval.getStartMillis() + hourToMillis * hour + hourToMillis,
              DateTimeZone.UTC
          )
      );
    }

    final IndexerSQLMetadataStorageCoordinator coordinator =
        (IndexerSQLMetadataStorageCoordinator) taskActionTestKit.getMetadataStorageCoordinator();

    final List<String> dimensions = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      dimensions.add("dimension_" + i);
    }
    final Set<DataSegment> segments = new HashSet<>();
    final int numUsedSegmentsPerInterval = 2000;
    int version = 0;
    for (Interval interval : intervals) {
      for (int i = 0; i < numUsedSegmentsPerInterval; i++) {
        segments.add(
            DataSegment.builder()
                       .dataSource(task.getDataSource())
                       .interval(interval)
                       .version("version" + version)
                       .shardSpec(new NumberedShardSpec(i, numUsedSegmentsPerInterval))
                       .dimensions(dimensions)
                       .size(100)
                       .build()
        );
      }
      coordinator.commitSegments(segments, null);
      segments.clear();
      version = (version + 1) % 10;
    }


    final int numAllocations = 10;
    final int replicas = 2;
    Map<String, List<Future<SegmentIdWithShardSpec>>> sequenceNameAndPrevIdToFutures = new HashMap<>();
    for (int i = 0; i < numAllocations; i++) {
      for (int j = 0; j < numHours; j++) {
        final int id = numUsedSegmentsPerInterval + i / replicas;
        final String sequenceId = j + "-sequence" + id;
        final String prevSequenceId = j + "-sequence" + (id - 1);
        sequenceNameAndPrevIdToFutures.computeIfAbsent(
            sequenceId + "|" + prevSequenceId,
            k -> new ArrayList<>()
        ).add(
            allocationQueue.add(
                new SegmentAllocateRequest(
                    task,
                    new SegmentAllocateAction(
                        task.getDataSource(),
                        intervals.get(j).getStart(),
                        Granularities.NONE,
                        Granularities.HOUR,
                        sequenceId,
                        prevSequenceId,
                        false,
                        NumberedPartialShardSpec.instance(),
                        LockGranularity.TIME_CHUNK,
                        TaskLockType.APPEND
                    ),
                    10
                )
            )
        );
      }
    }
    executor.finishAllPendingTasks();

    final Set<SegmentIdWithShardSpec> allocatedIds = new HashSet<>();
    for (List<Future<SegmentIdWithShardSpec>> sameIds : sequenceNameAndPrevIdToFutures.values()) {
      if (sameIds.isEmpty()) {
        return;
      }
      final SegmentIdWithShardSpec id = sameIds.get(0).get();
      Assert.assertNotNull(id);
      for (int i = 1; i < sameIds.size(); i++) {
        Assert.assertEquals(id, sameIds.get(i).get());
      }
      Assert.assertFalse(allocatedIds.contains(id));
      allocatedIds.add(id);
    }
    Assert.assertEquals(numHours * numAllocations, allocatedIds.size() * replicas);
    printStats("task/action/batch/runTime");
    printStats("task/action/batch/queueTime");
  }

  private void printStats(String metricName)
  {
    int count = 0;
    double max = 0;
    double min = Double.POSITIVE_INFINITY;
    double sum = 0;
    for (ServiceMetricEvent event : emitter.getMetricEvents().get(metricName)) {
      count++;
      max = Math.max(max, event.getValue().doubleValue());
      min = Math.min(min, event.getValue().doubleValue());
      sum += event.getValue().doubleValue();
    }
    double avg = 0;
    if (count > 0) {
      avg = sum / count;
    }
    log.info(
        "Metric[%s]: count[%d], min[%f], max[%f], avg[%f], sum[%f]",
        metricName, count, min, max, avg, sum
    );
  }

  @Test
  public void testBatchWithMultipleTimestamps()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01T01:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_1")
                         .build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01T02:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_2")
                         .build(),
        true
    );
  }

  @Test
  public void testBatchWithExclusiveLocks()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.EXCLUSIVE).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.EXCLUSIVE).build(),
        true
    );
  }

  @Test
  public void testBatchWithSharedLocks()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.SHARED).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.SHARED).build(),
        true
    );
  }

  @Test
  public void testBatchWithMultipleQueryGranularities()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withQueryGranularity(Granularities.SECOND).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withQueryGranularity(Granularities.MINUTE).build(),
        true
    );
  }

  @Test
  public void testMultipleDatasourcesCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build(),
        allocateRequest().forTask(createTask(TestDataSource.KOALA, "group_1")).build(),
        false
    );
  }

  @Test
  public void testMultipleGroupIdsCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_2")).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_3")).build(),
        false
    );
  }

  @Test
  public void testMultipleLockGranularitiesCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.TIME_CHUNK).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.SEGMENT).build(),
        false
    );
  }

  @Test
  public void testMultipleAllocateIntervalsCannotBatch()
  {
    verifyAllocationWithBatching(
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-01")
                         .withSegmentGranularity(Granularities.DAY).build(),
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .forTimestamp("2022-01-02")
                         .withSegmentGranularity(Granularities.DAY).build(),
        false
    );
  }

  @Test
  public void testConflictingPendingSegment()
  {
    SegmentAllocateRequest hourSegmentRequest =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withSegmentGranularity(Granularities.HOUR)
                         .build();
    Future<SegmentIdWithShardSpec> hourSegmentFuture = allocationQueue.add(hourSegmentRequest);

    SegmentAllocateRequest halfHourSegmentRequest =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1"))
                         .withSegmentGranularity(Granularities.THIRTY_MINUTE)
                         .build();
    Future<SegmentIdWithShardSpec> halfHourSegmentFuture = allocationQueue.add(halfHourSegmentRequest);

    executor.finishNextPendingTask();

    Assert.assertNotNull(getSegmentId(hourSegmentFuture));
    Assert.assertNull(getSegmentId(halfHourSegmentFuture));
  }

  @Test
  public void testFullAllocationQueue()
  {
    for (int i = 0; i < 2000; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i)).build();
      allocationQueue.add(request);
    }

    SegmentAllocateRequest request =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "next_group")).build();
    Future<SegmentIdWithShardSpec> future = allocationQueue.add(request);

    // Verify that the future is already complete and segment allocation has failed
    Throwable t = Assert.assertThrows(ISE.class, () -> getSegmentId(future));
    Assert.assertEquals(
        "Segment allocation queue is full. Check the metric `task/action/batch/runTime` "
        + "to determine if metadata operations are slow.",
        t.getMessage()
    );
  }

  @Test
  public void testMaxBatchSize()
  {
    for (int i = 0; i < 500; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build();
      allocationQueue.add(request);
    }

    // Verify that next request is added to a new batch
    Assert.assertEquals(1, allocationQueue.size());
    SegmentAllocateRequest request =
        allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_1")).build();
    allocationQueue.add(request);
    Assert.assertEquals(2, allocationQueue.size());
  }

  @Test
  public void testMultipleRequestsForSameSegment()
  {
    final List<Future<SegmentIdWithShardSpec>> segmentFutures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i))
                           .withSequenceName("sequence_1")
                           .withPreviousSegmentId("segment_1")
                           .build();
      segmentFutures.add(allocationQueue.add(request));
    }

    executor.finishNextPendingTask();

    SegmentIdWithShardSpec segmentId1 = getSegmentId(segmentFutures.get(0));

    for (Future<SegmentIdWithShardSpec> future : segmentFutures) {
      Assert.assertEquals(getSegmentId(future), segmentId1);
    }
  }

  @Test
  public void testMaxWaitTime()
  {
    // Verify that the batch is due yet
  }

  @Test
  public void testRequestsFailOnLeaderChange()
  {
    final List<Future<SegmentIdWithShardSpec>> segmentFutures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      SegmentAllocateRequest request =
          allocateRequest().forTask(createTask(TestDataSource.WIKI, "group_" + i)).build();
      segmentFutures.add(allocationQueue.add(request));
    }

    allocationQueue.stopBeingLeader();
    executor.finishNextPendingTask();

    for (Future<SegmentIdWithShardSpec> future : segmentFutures) {
      Throwable t = Assert.assertThrows(ISE.class, () -> getSegmentId(future));
      Assert.assertEquals("Not leader anymore", t.getMessage());
    }
  }

  private void verifyAllocationWithBatching(
      SegmentAllocateRequest a,
      SegmentAllocateRequest b,
      boolean canBatch
  )
  {
    Assert.assertEquals(0, allocationQueue.size());
    final Future<SegmentIdWithShardSpec> futureA = allocationQueue.add(a);
    final Future<SegmentIdWithShardSpec> futureB = allocationQueue.add(b);

    final int expectedCount = canBatch ? 1 : 2;
    Assert.assertEquals(expectedCount, allocationQueue.size());

    executor.finishNextPendingTask();
    emitter.verifyEmitted("task/action/batch/size", expectedCount);

    Assert.assertNotNull(getSegmentId(futureA));
    Assert.assertNotNull(getSegmentId(futureB));
  }

  private SegmentIdWithShardSpec getSegmentId(Future<SegmentIdWithShardSpec> future)
  {
    try {
      return future.get(5, TimeUnit.SECONDS);
    }
    catch (ExecutionException e) {
      throw new ISE(e.getCause().getMessage());
    }
    catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private SegmentAllocateActionBuilder allocateRequest()
  {
    return new SegmentAllocateActionBuilder()
        .forDatasource(TestDataSource.WIKI)
        .forTimestamp("2022-01-01")
        .withLockGranularity(LockGranularity.TIME_CHUNK)
        .withTaskLockType(TaskLockType.SHARED)
        .withQueryGranularity(Granularities.SECOND)
        .withSegmentGranularity(Granularities.HOUR);
  }

  private Task createTask(String datasource, String groupId)
  {
    Task task = new NoopTask(null, groupId, datasource, 0, 0, null);
    taskActionTestKit.getTaskLockbox().add(task);
    return task;
  }
}
