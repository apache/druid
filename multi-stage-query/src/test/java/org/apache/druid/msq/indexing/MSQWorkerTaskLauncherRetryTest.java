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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.WorkerFailureListener;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MSQWorkerTaskLauncherRetryTest
{
  private static final TaskLocation RUNNING_TASK_LOCATION = new TaskLocation("host", 1, 2, null);

  @Test
  public void testLaunchWorkersIfNeeded_returnsFailedWorkers() throws InterruptedException
  {
    TestOverlordClient overlordClient = new TestOverlordClient();
    overlordClient.addFailedWorker(1);

    AtomicInteger failureCallbackCount = new AtomicInteger(0);
    WorkerFailureListener workerFailureListener = (task, fault) -> {
      failureCallbackCount.incrementAndGet();
      Assertions.assertEquals(1, task.getWorkerNumber());
      Assertions.assertTrue(fault instanceof WorkerFailedFault);
    };

    MSQWorkerTaskLauncher launcher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        workerFailureListener,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig(),
        2
    );

    launcher.start();

    // Should return failed workers in the set
    Set<IntObjectPair<MSQFault>> failedWorkers = launcher.launchWorkersIfNeeded(2);

    // The method should not invoke the failure listener directly anymore, 
    // but should return the failed workers
    Assertions.assertFalse(failedWorkers.isEmpty());

    // Check that the failed worker is in the returned set
    boolean foundFailedWorker = false;
    for (IntObjectPair<MSQFault> failedWorker : failedWorkers) {
      if (failedWorker.leftInt() == 1) {
        foundFailedWorker = true;
        Assertions.assertTrue(failedWorker.right() instanceof WorkerFailedFault);
      }
    }
    Assertions.assertTrue(foundFailedWorker, "Failed worker should be in the returned set");

    launcher.stop(true);
  }

  @Test
  public void testWaitForWorkers_returnsFailedWorkers() throws InterruptedException
  {
    TestOverlordClient overlordClient = new TestOverlordClient();
    overlordClient.addFailedWorker(0);

    AtomicInteger failureCallbackCount = new AtomicInteger(0);
    WorkerFailureListener workerFailureListener = (task, fault) -> {
      failureCallbackCount.incrementAndGet();
    };

    MSQWorkerTaskLauncher launcher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        workerFailureListener,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig(),
        2
    );

    launcher.start();

    // Launch workers first
    launcher.launchWorkersIfNeeded(1);

    // Should return failed workers in the set when waiting for specific workers
    Set<IntObjectPair<MSQFault>> failedWorkers = launcher.waitForWorkers(ImmutableSet.of(0));

    // The method should not invoke the failure listener directly anymore,
    // but should return the failed workers
    Assertions.assertFalse(failedWorkers.isEmpty());

    // Check that the failed worker is in the returned set
    boolean foundFailedWorker = false;
    for (IntObjectPair<MSQFault> failedWorker : failedWorkers) {
      if (failedWorker.leftInt() == 0) {
        foundFailedWorker = true;
        Assertions.assertTrue(failedWorker.right() instanceof WorkerFailedFault);
      }
    }
    Assertions.assertTrue(foundFailedWorker, "Failed worker should be in the returned set");

    launcher.stop(true);
  }

  @Test
  public void testLaunchWorkersIfNeeded_returnsEmptySet_whenNoFailures() throws InterruptedException
  {
    TestOverlordClient overlordClient = new TestOverlordClient();
    // Don't add any failed workers

    WorkerFailureListener workerFailureListener = (task, fault) -> {
      Assertions.fail("Should not call failure listener when no workers fail");
    };

    MSQWorkerTaskLauncher launcher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        workerFailureListener,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig(),
        2
    );

    launcher.start();

    // Should return empty set when no workers fail
    Set<IntObjectPair<MSQFault>> failedWorkers = launcher.launchWorkersIfNeeded(1);
    Assertions.assertTrue(failedWorkers.isEmpty());

    launcher.stop(true);
  }

  @Test
  public void testWaitForWorkers_returnsEmptySet_whenNoFailures() throws InterruptedException
  {
    TestOverlordClient overlordClient = new TestOverlordClient();
    // Don't add any failed workers

    WorkerFailureListener workerFailureListener = (task, fault) -> {
      Assertions.fail("Should not call failure listener when no workers fail");
    };

    MSQWorkerTaskLauncher launcher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        workerFailureListener,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig(),
        2
    );

    launcher.start();

    // Launch workers first
    launcher.launchWorkersIfNeeded(1);

    // Should return empty set when no workers fail
    Set<IntObjectPair<MSQFault>> failedWorkers = launcher.waitForWorkers(ImmutableSet.of(0));
    Assertions.assertTrue(failedWorkers.isEmpty());

    launcher.stop(true);
  }

  private static class TestOverlordClient implements OverlordClient
  {
    private final ConcurrentSkipListSet<Integer> unknownLocationWorkers = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Integer> failedWorkers = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<String> canceledTasks = new ConcurrentSkipListSet<>();

    @Override
    public ListenableFuture<URI> findCurrentLeader()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> cancelTask(String taskId)
    {
      canceledTasks.add(taskId);
      return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
    {
      final Map<String, TaskStatus> taskStatusMap = new HashMap<>();
      for (String taskId : taskIds) {
        int workerNumber = MSQTasks.workerFromTaskId(taskId);
        if (canceledTasks.contains(taskId)) {
          taskStatusMap.put(taskId, TaskStatus.failure(taskId, "Canceled"));
        } else if (failedWorkers.contains(workerNumber)) {
          taskStatusMap.put(taskId, TaskStatus.failure(taskId, "Task failed"));
        } else if (unknownLocationWorkers.contains(workerNumber)) {
          taskStatusMap.put(taskId, TaskStatus.running(taskId).withLocation(TaskLocation.unknown()));
        } else {
          taskStatusMap.put(taskId, TaskStatus.running(taskId).withLocation(RUNNING_TASK_LOCATION));
        }
      }
      return Futures.immediateFuture(taskStatusMap);
    }

    @Override
    public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
    {
      if (canceledTasks.contains(taskId) || failedWorkers.contains(MSQTasks.workerFromTaskId(taskId))) {
        return Futures.immediateFuture(new TaskStatusResponse(taskId, createFailedTaskStatus(taskId)));
      }
      if (unknownLocationWorkers.contains(MSQTasks.workerFromTaskId(taskId))) {
        return Futures.immediateFuture(new TaskStatusResponse(taskId, createRunningTaskStatusWithOutLocation(taskId)));
      } else {
        return Futures.immediateFuture(new TaskStatusResponse(taskId, createRunningTaskStatus(taskId)));
      }
    }

    @Override
    public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Map<String, String>> postSupervisor(SupervisorSpec supervisor)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Map<String, String>> terminateSupervisor(String supervisorId)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<CloseableIterator<SupervisorStatus>> supervisorStatuses()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<Boolean> isCompactionSupervisorEnabled()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<ClusterCompactionConfig> getClusterCompactionConfig()
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<UpdateResponse> updateClusterCompactionConfig(ClusterCompactionConfig config)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(String dataSource)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(
        String dataSource,
        SegmentsToUpdateFilter filter
    )
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markSegmentAsUsed(SegmentId segmentId)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(String dataSource)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(
        String dataSource,
        SegmentsToUpdateFilter filter
    )
    {
      throw new UOE("Not implemented");
    }

    @Override
    public ListenableFuture<SegmentUpdateResponse> markSegmentAsUnused(SegmentId segmentId)
    {
      throw new UOE("Not implemented");
    }

    @Override
    public OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
    {
      return this;
    }

    public void addUnknownLocationWorker(int workerNumber)
    {
      unknownLocationWorkers.add(workerNumber);
    }

    public void addFailedWorker(int workerNumber)
    {
      failedWorkers.add(workerNumber);
    }

    public void removefailedWorker(int workerNumber)
    {
      failedWorkers.remove(workerNumber);
    }

    public void removeUnknownLocationWorker(int workerNumber)
    {
      unknownLocationWorkers.remove(workerNumber);
    }

    private TaskStatusPlus createRunningTaskStatusWithOutLocation(String taskid)
    {
      return createTaskStatusPlus(taskid, TaskState.RUNNING, TaskLocation.unknown());
    }

    private TaskStatusPlus createRunningTaskStatus(String taskid)
    {
      return createTaskStatusPlus(taskid, TaskState.RUNNING, RUNNING_TASK_LOCATION);
    }

    private TaskStatusPlus createFailedTaskStatus(String taskid)
    {
      return createTaskStatusPlus(taskid, TaskState.FAILED, RUNNING_TASK_LOCATION);
    }

    private TaskStatusPlus createTaskStatusPlus(String taskid, TaskState taskState, TaskLocation location)
    {
      return new TaskStatusPlus(
          taskid,
          "group-id",
          "type",
          DateTime.now(DateTimeZone.UTC),
          DateTime.now(DateTimeZone.UTC),
          taskState,
          RunnerTaskState.RUNNING,
          1000L,
          location,
          "dataSource",
          null
      );
    }
  }
}
