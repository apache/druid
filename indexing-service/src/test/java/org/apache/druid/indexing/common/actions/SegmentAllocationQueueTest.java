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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SegmentAllocationQueueTest
{
  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final String DS_WIKI = "wiki";
  private static final String DS_KOALA = "koala";

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
      public long getBatchAllocationMaxWaitTime()
      {
        return 0;
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
  public void testCriteriaForBatching()
  {
    // Requests with different timestamps and sequence names can batched together
    // as long as the other params are the same
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .forTimestamp("2022-01-01T01:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_1")
                         .build(),
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .forTimestamp("2022-01-01T02:00:00")
                         .withSegmentGranularity(Granularities.DAY)
                         .withQueryGranularity(Granularities.SECOND)
                         .withLockGranularity(LockGranularity.TIME_CHUNK)
                         .withSequenceName("seq_2")
                         .build(),
        true
    );

    // Requests for different lock types can be batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.EXCLUSIVE).build(),
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .withTaskLockType(TaskLockType.SHARED).build(),
        true
    );
  }

  @Test(timeout = 60_000)
  public void testCriteriaForBatchingNegative()
  {
    // Requests for different datasources are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "group_1")).build(),
        allocateRequest().forTask(createTask(DS_KOALA, "group_1")).build(),
        false
    );

    // Requests for different groupIds are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "group_1")).build(),
        allocateRequest().forTask(createTask(DS_WIKI, "group_2")).build(),
        false
    );

    // Requests for different lock granularities are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.TIME_CHUNK).build(),
        allocateRequest().forTask(createTask(DS_WIKI, "group_1"))
                         .withLockGranularity(LockGranularity.SEGMENT).build(),
        false
    );

    // Requests for different query granularities are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "1"))
                         .withQueryGranularity(Granularities.SECOND).build(),
        allocateRequest().forTask(createTask(DS_WIKI, "2"))
                         .withQueryGranularity(Granularities.MINUTE).build(),
        false
    );

    // Requests for different segment granularities are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "1"))
                         .withSegmentGranularity(Granularities.HOUR).build(),
        allocateRequest().forTask(createTask(DS_WIKI, "2"))
                         .withSegmentGranularity(Granularities.THIRTY_MINUTE).build(),
        false
    );

    // Requests for different allocate intervals are not batched together
    verifyRequestsCanBeBatched(
        allocateRequest().forTask(createTask(DS_WIKI, "1"))
                         .forTimestamp("2022-01-01")
                         .withSegmentGranularity(Granularities.DAY)
                         .withSequenceName("seq_1").build(),
        allocateRequest().forTask(createTask(DS_WIKI, "2"))
                         .forTimestamp("2022-01-02")
                         .withSegmentGranularity(Granularities.DAY).build(),
        false
    );
  }

  private void verifyRequestsCanBeBatched(
      SegmentAllocateRequest a,
      SegmentAllocateRequest b,
      boolean canBatch
  )
  {
    // Disable queue and cleanup state
    allocationQueue.stopBeingLeader();
    executor.finishAllPendingTasks();
    emitter.flush();

    // Enable queue again
    allocationQueue.becomeLeader();

    Assert.assertEquals(0, allocationQueue.size());
    allocationQueue.add(a);
    allocationQueue.add(b);

    final int expectedCount = canBatch ? 1 : 2;
    Assert.assertEquals(expectedCount, allocationQueue.size());

    executor.finishNextPendingTask();
    emitter.verifyEmitted("task/action/batch/size", expectedCount);
  }

  @Test
  public void testBatchNotDue()
  {

  }

  @Test
  public void testAllocationWithBatch()
  {
    // try out different combos of these:
    // lock granularity, lock type, skip lineage,
  }

  // now we need to test out a bunch of scenarios
  // possibly with all combos above?

  @Test
  public void testMultipleRequestsForSameSegment()
  {

  }

  @Test
  public void testMultipleInOrderRequests()
  {
    // multiple requests which are requesting for segments in the right prev_segment_sequence whatever
  }

  @Test
  public void testMultipleOutOfOrderRequests()
  {
    // multiple out of order thingies
  }

  @Test
  public void testRequestsWithDifferentRowIntervals()
  {

  }

  @Test
  public void testPartialSuccess()
  {

  }

  @Test
  public void testRetry()
  {

  }


  @Test
  public void testMaxWaitTime()
  {

  }

  @Test
  public void testEnableDisableQueue()
  {

  }

  @Test
  public void testLeadershipChanges()
  {

  }

  @Test
  public void testDifferentGranualarities()
  {

  }

  @Test
  public void testWeekGranularity()
  {
    // This test should probably be included in the old SegmentAllocateActionTest as well
  }

  private SegmentAllocateActionBuilder allocateRequest()
  {
    return new SegmentAllocateActionBuilder()
        .forDatasource(DS_WIKI)
        .forTimestamp("2022-01-01")
        .withQueryGranularity(Granularities.SECOND)
        .withSegmentGranularity(Granularities.HOUR);
  }

  private void assertEquals(final SegmentIdWithShardSpec expected, final SegmentIdWithShardSpec actual)
  {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.getShardSpec(), actual.getShardSpec());
  }

  private Task createTask(String datasource, String groupId)
  {
    Task task = new NoopTask(null, groupId, datasource, 0, 0, null, null, null);
    taskActionTestKit.getTaskLockbox().add(task);
    return task;
  }
}
