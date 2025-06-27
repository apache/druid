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

package org.apache.druid.testing.embedded.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tests all the REST APIs exposed by the Overlord using the
 * {@link OverlordClient}.
 */
public class OverlordClientEmbeddedTest extends EmbeddedClusterTestBase
{
  private static final String UNKNOWN_TASK_ID
      = IdUtils.newTaskId("sim_test_noop", "dummy", null);
  private static final String UNKNOWN_TASK_ERROR
      = StringUtils.format("Cannot find any task with id: [%s]", UNKNOWN_TASK_ID);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(new EmbeddedIndexer().addProperty("druid.worker.capacity", "3"))
                               .addServer(overlord);
  }

  @Test
  public void test_findCurrentLeader()
  {
    URI currentLeader = cluster.callApi().onLeaderOverlord(OverlordClient::findCurrentLeader);
    Assertions.assertEquals(8090, currentLeader.getPort());
  }

  @Test
  public void test_runTask_ofTypeNoop()
  {
    final String taskId = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, new NoopTask(taskId, null, null, 1L, 0L, null))
    );
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  @Test
  public void test_runKillTask()
  {
    final String taskId = cluster.callApi().onLeaderOverlord(
        o -> o.runKillTask("sim_test", TestDataSource.WIKI, Intervals.ETERNITY, null, null, null)
    );
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  @Test
  public void test_cancelTask_fails_forUnknownTaskId()
  {
    verifyFailsWith(
        cluster.leaderOverlord().cancelTask(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  @Disabled
  public void test_cancelTask_ofTypeNoop_andLongRunDuration()
  {
    // Start a long-running task
    final long taskRunDuration = 10_000L;
    final String taskId = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, new NoopTask(taskId, null, null, taskRunDuration, 0L, null))
    );
    cluster.callApi().verifyTaskHasStatus(taskId, TaskStatus.running(taskId));

    // Cancel and verify status
    cluster.callApi().onLeaderOverlord(o -> o.cancelTask(taskId));
    cluster.callApi().verifyTaskHasStatus(
        taskId,
        TaskStatus.failure(taskId, "Shutdown request from user")
    );
  }

  @Test
  public void test_taskStatuses_returnsEmpty_forRunningTasks()
  {
    CloseableIterator<TaskStatusPlus> result = cluster.callApi().onLeaderOverlord(
        o -> o.taskStatuses("running", null, null)
    );
    final List<TaskStatusPlus> runningTasks = ImmutableList.copyOf(result);
    Assertions.assertTrue(runningTasks.isEmpty());
  }

  @Test
  public void test_taskStatuses_forCompleteTasks()
  {
    // Run multiple tasks
    final String task1 = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(task1, new NoopTask(task1, null, null, 1L, 0L, null))
    );
    cluster.callApi().waitForTaskToSucceed(task1, overlord);

    final String task2 = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(task2, new NoopTask(task2, null, null, 1L, 0L, null))
    );
    cluster.callApi().waitForTaskToSucceed(task2, overlord);

    CloseableIterator<TaskStatusPlus> result = cluster.callApi().onLeaderOverlord(
        o -> o.taskStatuses("complete", null, null)
    );
    final Map<String, TaskStatusPlus> completeTaskIdToStatus
        = ImmutableList.copyOf(result)
                       .stream()
                       .collect(Collectors.toMap(TaskStatusPlus::getId, status -> status));
    Assertions.assertTrue(completeTaskIdToStatus.size() >= 2);

    Assertions.assertEquals(TaskState.SUCCESS, completeTaskIdToStatus.get(task1).getStatusCode());
    Assertions.assertEquals(TaskState.SUCCESS, completeTaskIdToStatus.get(task2).getStatusCode());
  }

  @Test
  public void test_taskStatuses_byIds_returnsEmpty_forUnknownTaskIds()
  {
    Map<String, TaskStatus> result = cluster.callApi().onLeaderOverlord(
        o -> o.taskStatuses(Set.of(UNKNOWN_TASK_ID))
    );
    Assertions.assertTrue(result.isEmpty());
  }

  @Test
  public void test_taskStatus_fails_forUnknownTaskId()
  {
    verifyFailsWith(
        cluster.leaderOverlord().taskStatus(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  public void test_taskPayload_fails_forUnknownTaskId()
  {
    verifyFailsWith(
        cluster.leaderOverlord().taskPayload(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  public void test_supervisorStatuses()
  {
    CloseableIterator<SupervisorStatus> result = cluster.callApi().onLeaderOverlord(
        OverlordClient::supervisorStatuses
    );
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_findLockedIntervals_fails_whenNoFilter()
  {
    verifyFailsWith(
        cluster.leaderOverlord().findLockedIntervals(List.of()),
        "No filter provided"
    );
  }

  @Test
  public void test_killPendingSegments()
  {
    Integer numPendingSegmentsDeleted = cluster.callApi().onLeaderOverlord(
        o -> o.killPendingSegments(TestDataSource.WIKI, Intervals.ETERNITY)
    );
    Assertions.assertEquals(0, numPendingSegmentsDeleted.intValue());
  }

  @Test
  public void test_getWorkers()
  {
    List<IndexingWorkerInfo> workers = cluster.callApi().onLeaderOverlord(OverlordClient::getWorkers);
    Assertions.assertEquals(1, workers.size());
    Assertions.assertEquals(3, workers.get(0).getWorker().getCapacity());
  }

  @Test
  public void test_getTotalWorkerCapacity()
  {
    IndexingTotalWorkerCapacityInfo result = cluster.callApi().onLeaderOverlord(
        OverlordClient::getTotalWorkerCapacity
    );
    Assertions.assertEquals(3, result.getCurrentClusterCapacity());
  }

  @Test
  public void test_isCompactionSupervisorEnabled()
  {
    Boolean result = cluster.callApi().onLeaderOverlord(OverlordClient::isCompactionSupervisorEnabled);
    Assertions.assertFalse(result);
  }

  @Test
  public void test_markNonOvershadowedSegmentsAsUsed_basic()
  {
    SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(o -> o.markNonOvershadowedSegmentsAsUsed(TestDataSource.WIKI));
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_markNonOvershadowedSegmentsAsUsed_filtered()
  {
    SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(
        o -> o.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.WIKI,
            new SegmentsToUpdateFilter(Intervals.ETERNITY, null, null)
        )
    );
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_markSegmentAsUsed()
  {
    SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(
        o -> o.markSegmentAsUsed(SegmentId.dummy(TestDataSource.WIKI))
    );
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_markSegmentsAsUnused_basic()
  {
    final SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(
        o -> o.markSegmentsAsUnused(TestDataSource.WIKI)
    );
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_markSegmentsAsUnused_filtered()
  {
    SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(
        o -> o.markSegmentsAsUnused(
            TestDataSource.WIKI,
            new SegmentsToUpdateFilter(Intervals.ETERNITY, null, null)
        )
    );
    Assertions.assertNotNull(result);
  }

  @Test
  public void test_markSegmentAsUnused()
  {
    SegmentUpdateResponse result = cluster.callApi().onLeaderOverlord(
        o -> o.markSegmentAsUnused(
            SegmentId.dummy(TestDataSource.WIKI)
        )
    );
    Assertions.assertNotNull(result);
  }

  private static <T> void verifyFailsWith(ListenableFuture<T> future, String message)
  {
    final CountDownLatch isFutureDone = new CountDownLatch(1);
    final AtomicReference<Throwable> capturedError = new AtomicReference<>();
    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(T result)
          {
            isFutureDone.countDown();
          }

          @Override
          public void onFailure(Throwable t)
          {
            capturedError.set(t);
            isFutureDone.countDown();
          }
        },
        MoreExecutors.directExecutor()
    );

    try {
      isFutureDone.await();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    Assertions.assertNotNull(capturedError.get());
    Assertions.assertTrue(capturedError.get().getMessage().contains(message));
  }
}
