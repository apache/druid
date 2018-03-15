/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.TaskStatus;
import io.druid.client.indexing.TaskStatusResponse;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.TaskToolbox;
import io.druid.java.util.common.concurrent.Execs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
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
  private final ExecutorService taskRunner = Execs.multiThreaded(5, "task-monitor-test-%d");
  private final ConcurrentMap<String, TaskState> tasks = new ConcurrentHashMap<>();
  private final TaskMonitor monitor = new TaskMonitor(new TestIndexingServiceClient(), 3);

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
    final List<ListenableFuture<TaskStatus>> futures = IntStream
        .range(0, 10)
        .mapToObj(i -> monitor.submit(new TestTask("id" + i, 100, 0)))
        .collect(Collectors.toList());
    for (int i = 0; i < futures.size(); i++) {
      // # of threads of taskRunner is 5, so the expected max timeout is 2 sec. We additionally wait three more seconds
      // here to make sure the test passes.
      final TaskStatus result = futures.get(i).get(1, TimeUnit.SECONDS);
      Assert.assertEquals("id" + i, result.getId());
      Assert.assertEquals(TaskState.SUCCESS, result.getStatusCode());
    }
  }

  @Test
  public void testRetry() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<TaskStatus>> futures = IntStream
        .range(0, 10)
        .mapToObj(i -> monitor.submit(new TestTask("id" + i, 100, 2)))
        .collect(Collectors.toList());
    for (int i = 0; i < futures.size(); i++) {
      // # of threads of taskRunner is 5, and each task is expected to be run 3 times (with 2 retries), so the expected
      // max timeout is 6 sec. We additionally wait 4 more seconds here to make sure the test passes.
      final TaskStatus result = futures.get(i).get(2, TimeUnit.SECONDS);
      Assert.assertEquals("id" + i, result.getId());
      Assert.assertEquals(TaskState.SUCCESS, result.getStatusCode());
    }
  }

  private static class TestTask extends NoopTask
  {
    private final int numMaxFails;

    private int numFails;

    public TestTask(String id, long runTime, int numMaxFails)
    {
      super(id, "testDataSource", runTime, 0, null, null, null);
      this.numMaxFails = numMaxFails;
    }

    @Override
    public io.druid.indexing.common.TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      if (numFails < numMaxFails) {
        numFails++;
        Thread.sleep(getRunTime());
        return io.druid.indexing.common.TaskStatus.failure(getId());
      } else {
        return super.run(toolbox);
      }
    }
  }

  private class TestIndexingServiceClient extends IndexingServiceClient
  {
    TestIndexingServiceClient()
    {
      super(null, null);
    }

    @Override
    public String runTask(Object taskObject)
    {
      final TestTask task = (TestTask) taskObject;
      tasks.put(task.getId(), TaskState.RUNNING);
      taskRunner.submit(() -> tasks.put(task.getId(), task.run(null).getStatusCode()));
      return task.getId();
    }

    @Override
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      return new TaskStatusResponse(
          taskId,
          new TaskStatus(taskId, tasks.get(taskId), null, -1)
      );
    }
  }
}
