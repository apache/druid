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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.msq.exec.MSQTasks;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MSQWorkerTaskLauncherRetryTests
{

  private static final TaskLocation RUNNING_TASK_LOCATION = new TaskLocation("host", 1, 2, null);

  @Test
  public void mainThreadBlockingSimulationTest() throws Exception
  {
    final ExecutorService executors = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(false)
                                                                                                  .setNameFormat(
                                                                                                      "Controller-simulator-%d")
                                                                                                  .build());

    final TestOverlordClient overlordClient = new TestOverlordClient();
    final int failedWorkerNumber = 2;
    final CountDownLatch workerFailedLatch = new CountDownLatch(1);
    final CountDownLatch workerStartedLatch = new CountDownLatch(1);
    overlordClient.addFailedWorker(2);
    overlordClient.addUnknownLocationWorker(1);

    final MSQWorkerTaskLauncher msqWorkerTaskLauncher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig()
    );

    try {
      final long workerThreadId = Thread.currentThread().getId();

      startTaskLauncher(
          msqWorkerTaskLauncher,
          failedWorkerNumber,
          workerFailedLatch,
          overlordClient,
          workerThreadId,
          workerStartedLatch
      );

      MockConsumer mockConsumer = new MockConsumer(
          msqWorkerTaskLauncher,
          3,
          workerStartedLatch
      );
      Future<?> futures = executors.submit(mockConsumer);
      // hook called but worker not queued for relaunch.
      workerFailedLatch.await();
      Assertions.assertEquals(1, workerStartedLatch.getCount());
      // we would need to call hooks to allow the main thread to proceed since we are using an exec service to so the thread id's would not match.
      enableWorkerRelaunch(overlordClient, failedWorkerNumber, msqWorkerTaskLauncher, workerStartedLatch);
      // future should be completed in 5 seconds else throw an exception.
      Assertions.assertNull(futures.get(5, TimeUnit.SECONDS));
    }
    finally {
      msqWorkerTaskLauncher.stop(true);
      executors.shutdownNow();
    }
  }

  private static void enableWorkerRelaunch(
      TestOverlordClient overlordClient,
      int failedWorkerNumber,
      MSQWorkerTaskLauncher msqWorkerTaskLauncher,
      CountDownLatch workerStartedLatch
  )
  {
    overlordClient.removeUnknownLocationWorker(1);
    overlordClient.removefailedWorker(failedWorkerNumber);
    msqWorkerTaskLauncher.submitForRelaunch(failedWorkerNumber);
    workerStartedLatch.countDown();
  }

  private static void startTaskLauncher(
      MSQWorkerTaskLauncher msqWorkerTaskLauncher,
      int failedWorkerNumber,
      CountDownLatch workerFailedLatch,
      TestOverlordClient overlordClient,
      long workerThreadId,
      CountDownLatch workerStartedLatch
  )
  {
    msqWorkerTaskLauncher.start((task, fault) -> {
      Assertions.assertEquals(failedWorkerNumber, task.getWorkerNumber());
      workerFailedLatch.countDown();
      if (workerThreadId == Thread.currentThread().getId()) {
        // If the worker thread is the same as the main thread, we can directly relaunch the worker.
        enableWorkerRelaunch(overlordClient, failedWorkerNumber, msqWorkerTaskLauncher, workerStartedLatch);
      }
    });
  }


  @Test
  public void mainThreadNonBlockingSimulationTest() throws Exception
  {
    final TestOverlordClient overlordClient = new TestOverlordClient();
    final int failedWorkerNumber = 2;
    final CountDownLatch workerFailedLatch = new CountDownLatch(1);
    final CountDownLatch workerStartedLatch = new CountDownLatch(1);
    overlordClient.addFailedWorker(2);
    overlordClient.addUnknownLocationWorker(1);

    final MSQWorkerTaskLauncher msqWorkerTaskLauncher = new MSQWorkerTaskLauncher(
        "controller-id",
        "foo",
        overlordClient,
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5),
        new MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig()
    );

    try {
      final long workerThreadId = Thread.currentThread().getId();

      startTaskLauncher(
          msqWorkerTaskLauncher,
          failedWorkerNumber,
          workerFailedLatch,
          overlordClient,
          workerThreadId,
          workerStartedLatch
      );


      MockConsumer mockConsumer = new MockConsumer(
          msqWorkerTaskLauncher,
          3,
          workerStartedLatch
      );
      mockConsumer.run();
      // failed latch  called
      workerFailedLatch.await();
      // worker started.
      workerStartedLatch.await();
    }
    finally {
      msqWorkerTaskLauncher.stop(true);
    }
  }


  private static class MockConsumer implements Runnable
  {

    private final MSQWorkerTaskLauncher msqWorkerTaskLauncher;
    private final int taskCount;
    private final CountDownLatch workerStartedLatch;

    public MockConsumer(
        MSQWorkerTaskLauncher msqWorkerTaskLauncher,
        int tasksCount,
        CountDownLatch workerStartedLatch
    )
    {
      this.msqWorkerTaskLauncher = msqWorkerTaskLauncher;
      this.taskCount = tasksCount;
      this.workerStartedLatch = workerStartedLatch;
    }


    @Override
    public void run()
    {
      // start stages
      try {
        msqWorkerTaskLauncher.launchWorkersIfNeeded(taskCount);
        workerStartedLatch.await();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      Set<Integer> workerNumbers = new HashSet<>();
      for (int i = 0; i < taskCount; i++) {
        workerNumbers.add(i);
      }

      // submit work worders
      try {
        msqWorkerTaskLauncher.waitForWorkers(workerNumbers);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }


  private static class TestOverlordClient implements OverlordClient
  {
    private final ConcurrentSkipListSet<Integer> unknownLocationWorkers = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<Integer> failedWorkers = new ConcurrentSkipListSet<>();

    public TestOverlordClient()
    {
    }

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
      if (failedWorkers.contains(MSQTasks.workerFromTaskId(taskId))) {
        return Futures.immediateFuture(null);
      } else {
        throw DruidException.defensive("Task %s should not be cancelled", taskId);
      }
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
        if (failedWorkers.contains(workerNumber)) {
          taskStatusMap.put(taskId, TaskStatus.failure(taskId, TaskQueue.FAILED_TO_RUN_TASK_SEE_OVERLORD_MSG));
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
      if (failedWorkers.contains(MSQTasks.workerFromTaskId(taskId))) {
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
