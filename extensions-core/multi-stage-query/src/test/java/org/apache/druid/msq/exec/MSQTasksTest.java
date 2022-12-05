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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  public void test_queryWithoutEnoughSlots_shouldThrowException()
  {
    final int numSlots = 5;
    final int numTasks = 10;

    ControllerContext controllerContext = mock(ControllerContext.class);
    when(controllerContext.workerManager()).thenReturn(new TasksTestWorkerManagerClient(numSlots));
    MSQWorkerTaskLauncher msqWorkerTaskLauncher = new MSQWorkerTaskLauncher(
        CONTROLLER_ID,
        "foo",
        controllerContext,
        false,
        -1L,
        TimeUnit.SECONDS.toMillis(5)
    );

    try {
      msqWorkerTaskLauncher.start();
      msqWorkerTaskLauncher.launchTasksIfNeeded(numTasks);
      fail();
    }
    catch (Exception e) {
      Assert.assertEquals(
          new TaskStartTimeoutFault(numTasks + 1).getCodeWithMessage(),
          ((MSQException) e.getCause()).getFault().getCodeWithMessage()
      );
    }
  }

  static class TasksTestWorkerManagerClient implements WorkerManagerClient
  {
    // Num of slots available for tasks
    final int numSlots;

    @GuardedBy("this")
    final Set<String> allTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> runningTasks = new HashSet<>();

    @GuardedBy("this")
    final Set<String> canceledTasks = new HashSet<>();

    public TasksTestWorkerManagerClient(final int numSlots)
    {
      this.numSlots = numSlots;
    }

    @Override
    public synchronized Map<String, TaskStatus> statuses(final Set<String> taskIds)
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

      return retVal;
    }

    @Override
    public synchronized TaskLocation location(String workerId)
    {
      if (runningTasks.contains(workerId)) {
        return TaskLocation.create("host-" + workerId, 1, -1);
      } else {
        return TaskLocation.unknown();
      }
    }

    @Override
    public synchronized String run(String controllerId, MSQWorkerTask task)
    {
      allTasks.add(task.getId());

      if (runningTasks.size() < numSlots) {
        runningTasks.add(task.getId());
      }

      return task.getId();
    }

    @Override
    public synchronized void cancel(String workerId)
    {
      runningTasks.remove(workerId);
      canceledTasks.add(workerId);
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
