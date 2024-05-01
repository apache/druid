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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForJob;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.utils.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class MSQTasksTest
{
  private static final String CONTROLLER_ID = "controller-id";
  private static final String WORKER_ID = "worker-id";
  private static final String CONTROLLER_HOST = "controller-host";
  private static final String WORKER_HOST = "worker-host";

  @Test
  public void test_makeErrorReport_allNull()
  {
    Assert.assertEquals(
        MSQErrorReport.fromFault(
            CONTROLLER_ID,
            CONTROLLER_HOST,
            null,
            UnknownFault.forMessage(MSQTasks.GENERIC_QUERY_FAILED_MESSAGE)
        ),
        MSQTasks.makeErrorReport(CONTROLLER_ID, CONTROLLER_HOST, null, null)
    );
  }

  @Test
  public void test_makeErrorReport_controllerOnly()
  {
    final MSQErrorReport controllerReport = MSQTasks.makeErrorReport(
        CONTROLLER_ID,
        CONTROLLER_HOST,
        MSQErrorReport.fromFault(CONTROLLER_ID, CONTROLLER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(controllerReport, MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, null));
  }

  @Test
  public void test_makeErrorReport_workerOnly()
  {
    final MSQErrorReport workerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(workerReport, MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, null, workerReport));
  }

  @Test
  public void test_makeErrorReport_controllerPreferred()
  {
    final MSQErrorReport controllerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyWorkersFault(2, 20)),
        null
    );

    final MSQErrorReport workerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        controllerReport,
        MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_makeErrorReport_workerPreferred()
  {
    final MSQErrorReport controllerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new WorkerRpcFailedFault(WORKER_ID)),
        null
    );

    final MSQErrorReport workerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        workerReport,
        MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_makeErrorReport_controllerWithTooManyAttemptsForJob_workerPreferred()
  {
    final MSQErrorReport controllerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyAttemptsForJob(1, 1, "xxx", "xxx")),
        null
    );

    final MSQErrorReport workerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        workerReport,
        MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_makeErrorReport_controllerWithTooManyAttemptsForWorker_workerPreferred()
  {
    final MSQErrorReport controllerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyAttemptsForWorker(1, "xxx", 1, "xxx")),
        null
    );

    final MSQErrorReport workerReport = MSQTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        MSQErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        workerReport,
        MSQTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }


  @Test
  public void test_getWorkerFromTaskId()
  {
    Assert.assertEquals(1, MSQTasks.workerFromTaskId("xxxx-worker1_0"));
    Assert.assertEquals(10, MSQTasks.workerFromTaskId("xxxx-worker10_0"));
    Assert.assertEquals(0, MSQTasks.workerFromTaskId("xxdsadxx-worker0_0"));
    Assert.assertEquals(90, MSQTasks.workerFromTaskId("dx-worker90_0"));
    Assert.assertEquals(9, MSQTasks.workerFromTaskId("12dsa1-worker9_0"));

    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xxxx-worker-0"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("worker-0"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xxxx-worker1-0"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xxxx-worker0-"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xxxx-worr1_0"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xxxx-worker-1-0"));
    Assert.assertThrows(ISE.class, () -> MSQTasks.workerFromTaskId("xx"));
  }

  @Test
  public void test_queryWithoutEnoughSlots_shouldThrowException()
  {
    final int numSlots = 5;
    final int numTasks = 10;

    MSQWorkerTaskLauncher msqWorkerTaskLauncher = new MSQWorkerTaskLauncher(
        CONTROLLER_ID,
        "foo",
        new TasksTestOverlordClient(numSlots),
        (task, fault) -> {},
        ImmutableMap.of(),
        TimeUnit.SECONDS.toMillis(5)
    );

    try {
      msqWorkerTaskLauncher.start();
      msqWorkerTaskLauncher.launchWorkersIfNeeded(numTasks);
      fail();
    }
    catch (Exception e) {
      Assert.assertEquals(
          MSQFaultUtils.generateMessageWithErrorCode(new TaskStartTimeoutFault(5, numTasks + 1, 5000)),
          MSQFaultUtils.generateMessageWithErrorCode(((MSQException) e.getCause()).getFault())
      );
    }
  }

  static class TasksTestOverlordClient extends NoopOverlordClient
  {
    // Num of slots available for tasks
    final int numSlots;

    @GuardedBy("this")
    final Set<String> allTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> runningTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> canceledTasks = new HashSet<>();

    public TasksTestOverlordClient(final int numSlots)
    {
      this.numSlots = numSlots;
    }

    @Override
    public synchronized ListenableFuture<Map<String, TaskStatus>> taskStatuses(final Set<String> taskIds)
    {
      final Map<String, TaskStatus> retVal = new HashMap<>();

      for (final String taskId : taskIds) {
        if (allTasks.contains(taskId)) {
          retVal.put(
              taskId,
              new TaskStatus(
                  taskId,
                  canceledTasks.contains(taskId) ? TaskState.FAILED : TaskState.RUNNING,
                  2,
                  null,
                  null
              )
          );
        }
      }

      return Futures.immediateFuture(retVal);
    }

    @Override
    public synchronized ListenableFuture<TaskStatusResponse> taskStatus(String workerId)
    {
      final TaskStatus status = CollectionUtils.getOnlyElement(
          FutureUtils.getUnchecked(taskStatuses(ImmutableSet.of(workerId)), true).values(),
          xs -> new ISE("Expected one worker with id[%s] but saw[%s]", workerId, xs)
      );

      final TaskLocation location;

      if (runningTasks.contains(workerId)) {
        location = TaskLocation.create("host-" + workerId, 1, -1);
      } else {
        location = TaskLocation.unknown();
      }

      return Futures.immediateFuture(
          new TaskStatusResponse(
              status.getId(),
              new TaskStatusPlus(
                  status.getId(),
                  null,
                  null,
                  DateTimes.utc(0),
                  DateTimes.utc(0),
                  status.getStatusCode(),
                  status.getStatusCode(),
                  RunnerTaskState.NONE,
                  status.getDuration(),
                  location,
                  null,
                  status.getErrorMsg()
              )
          )
      );
    }

    @Override
    public synchronized ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      final MSQWorkerTask task = (MSQWorkerTask) taskObject;

      allTasks.add(task.getId());

      if (runningTasks.size() < numSlots) {
        runningTasks.add(task.getId());
      }

      return Futures.immediateFuture(null);
    }

    @Override
    public synchronized ListenableFuture<Void> cancelTask(String workerId)
    {
      runningTasks.remove(workerId);
      canceledTasks.add(workerId);
      return Futures.immediateFuture(null);
    }
  }
}
