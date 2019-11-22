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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.SubTaskCompleteEvent;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskMonitorTest
{
  private static final int SPLIT_NUM = 10;

  private final ExecutorService taskRunner = Execs.multiThreaded(5, "task-monitor-test-%d");
  private final ConcurrentMap<String, TaskState> tasks = new ConcurrentHashMap<>();
  private final TaskMonitor<TestTask> monitor = new TaskMonitor<>(new TestIndexingServiceClient(), 3, SPLIT_NUM);

  @Before
  public void setup()
  {
    tasks.clear();
    monitor.start(100);
  }

  @After
  public void teardown()
  {
    monitor.stop();
    taskRunner.shutdownNow();
  }

  @Test
  public void testBasic() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<SubTaskCompleteEvent<TestTask>>> futures = IntStream
        .range(0, 10)
        .mapToObj(i -> monitor.submit(
            new TestTaskSpec("specId" + i, "groupId", "supervisorId", null, new IntegerInputSplit(i), 100L, 0, false)
        ))
        .collect(Collectors.toList());
    for (int i = 0; i < futures.size(); i++) {
      // # of threads of taskRunner is 5, so the expected max timeout is 2 sec. We additionally wait three more seconds
      // here to make sure the test passes.
      final SubTaskCompleteEvent<TestTask> result = futures.get(i).get(1, TimeUnit.SECONDS);
      Assert.assertEquals("supervisorId", result.getSpec().getSupervisorTaskId());
      Assert.assertEquals("specId" + i, result.getSpec().getId());
      Assert.assertNotNull(result.getLastStatus());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastStatus().getStatusCode());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastState());
    }
  }

  @Test
  public void testRetry() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<TestTaskSpec> specs = IntStream
        .range(0, 10)
        .mapToObj(
            i -> new TestTaskSpec(
                "specId" + i,
                "groupId",
                "supervisorId",
                null,
                new IntegerInputSplit(i),
                100L,
                2,
                false
            )
        )
        .collect(Collectors.toList());
    final List<ListenableFuture<SubTaskCompleteEvent<TestTask>>> futures = specs
        .stream()
        .map(monitor::submit)
        .collect(Collectors.toList());
    for (int i = 0; i < futures.size(); i++) {
      // # of threads of taskRunner is 5, and each task is expected to be run 3 times (with 2 retries), so the expected
      // max timeout is 6 sec. We additionally wait 4 more seconds here to make sure the test passes.
      final SubTaskCompleteEvent<TestTask> result = futures.get(i).get(2, TimeUnit.SECONDS);
      Assert.assertEquals("supervisorId", result.getSpec().getSupervisorTaskId());
      Assert.assertEquals("specId" + i, result.getSpec().getId());

      Assert.assertNotNull(result.getLastStatus());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastStatus().getStatusCode());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastState());

      final TaskHistory<TestTask> taskHistory = monitor.getCompleteSubTaskSpecHistory(specs.get(i).getId());
      Assert.assertNotNull(taskHistory);

      final List<TaskStatusPlus> attemptHistory = taskHistory.getAttemptHistory();
      Assert.assertNotNull(attemptHistory);
      Assert.assertEquals(3, attemptHistory.size());
      Assert.assertEquals(TaskState.FAILED, attemptHistory.get(0).getStatusCode());
      Assert.assertEquals(TaskState.FAILED, attemptHistory.get(1).getStatusCode());
    }
  }

  @Test
  public void testResubmitWithOldType() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<TestTaskSpec> specs = IntStream
        .range(0, 10)
        .mapToObj(
            i -> new TestTaskSpec(
                "specId" + i,
                "groupId",
                "supervisorId",
                null,
                new IntegerInputSplit(i),
                100L,
                0,
                true
            )
        )
        .collect(Collectors.toList());
    final List<ListenableFuture<SubTaskCompleteEvent<TestTask>>> futures = specs
        .stream()
        .map(monitor::submit)
        .collect(Collectors.toList());
    for (int i = 0; i < futures.size(); i++) {
      // # of threads of taskRunner is 5, and each task is expected to be run 3 times (with 2 retries), so the expected
      // max timeout is 6 sec. We additionally wait 4 more seconds here to make sure the test passes.
      final SubTaskCompleteEvent<TestTask> result = futures.get(i).get(2, TimeUnit.SECONDS);
      Assert.assertEquals("supervisorId", result.getSpec().getSupervisorTaskId());
      Assert.assertEquals("specId" + i, result.getSpec().getId());

      Assert.assertNotNull(result.getLastStatus());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastStatus().getStatusCode());
      Assert.assertEquals(TaskState.SUCCESS, result.getLastState());

      final TaskHistory<TestTask> taskHistory = monitor.getCompleteSubTaskSpecHistory(specs.get(i).getId());
      Assert.assertNotNull(taskHistory);

      final List<TaskStatusPlus> attemptHistory = taskHistory.getAttemptHistory();
      Assert.assertNotNull(attemptHistory);
      Assert.assertEquals(1, attemptHistory.size());
      Assert.assertEquals(TaskState.SUCCESS, attemptHistory.get(0).getStatusCode());
    }
  }

  private static class TestTaskSpec extends SubTaskSpec<TestTask>
  {
    private final long runTime;
    private final int numMaxFails;
    private final boolean throwUnknownTypeIdError;

    private int numFails;

    TestTaskSpec(
        String id,
        String groupId,
        String supervisorTaskId,
        Map<String, Object> context,
        InputSplit inputSplit,
        long runTime,
        int numMaxFails,
        boolean throwUnknownTypeIdError
    )
    {
      super(id, groupId, supervisorTaskId, context, inputSplit);
      this.runTime = runTime;
      this.numMaxFails = numMaxFails;
      this.throwUnknownTypeIdError = throwUnknownTypeIdError;
    }

    @Override
    public TestTask newSubTask(int numAttempts)
    {
      return new TestTask(getId(), runTime, numFails++ < numMaxFails, throwUnknownTypeIdError);
    }

    @Override
    public TestTask newSubTaskWithBackwardCompatibleType(int numAttempts)
    {
      return new TestTask(getId(), runTime, numFails++ < numMaxFails, false);
    }
  }

  private static class TestTask extends NoopTask
  {
    private final boolean shouldFail;
    private final boolean throwUnknownTypeIdError;

    TestTask(String id, long runTime, boolean shouldFail, boolean throwUnknownTypeIdError)
    {
      super(id, null, "testDataSource", runTime, 0, null, null, null);
      this.shouldFail = shouldFail;
      this.throwUnknownTypeIdError = throwUnknownTypeIdError;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      if (shouldFail) {
        Thread.sleep(getRunTime());
        return TaskStatus.failure(getId());
      } else {
        return super.run(toolbox);
      }
    }
  }

  private class TestIndexingServiceClient extends NoopIndexingServiceClient
  {
    @Override
    public String runTask(Object taskObject)
    {
      final TestTask task = (TestTask) taskObject;
      tasks.put(task.getId(), TaskState.RUNNING);
      if (task.throwUnknownTypeIdError) {
        throw new RuntimeException(new ISE("Could not resolve type id 'test_task_id'"));
      }
      taskRunner.submit(() -> tasks.put(task.getId(), task.run(null).getStatusCode()));
      return task.getId();
    }

    @Override
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      return new TaskStatusResponse(
          taskId,
          new TaskStatusPlus(
              taskId,
              "groupId",
              "testTask",
              DateTimes.EPOCH,
              DateTimes.EPOCH,
              tasks.get(taskId),
              RunnerTaskState.RUNNING,
              -1L,
              TaskLocation.unknown(),
              "testDataSource",
              null
          )
      );
    }
  }

  private static class IntegerInputSplit extends InputSplit<Integer>
  {
    IntegerInputSplit(int split)
    {
      super(split);
    }
  }
}
