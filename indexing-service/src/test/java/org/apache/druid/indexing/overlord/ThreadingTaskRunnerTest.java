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

package org.apache.druid.indexing.overlord;

import com.google.common.base.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskStorageDirTracker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ThreadingTaskRunnerTest
{
  private static final Logger log = new Logger(ThreadingTaskRunnerTest.class);

  private ThreadingTaskRunner runner;

  @Before
  public void setup()
  {
    final TaskConfig taskConfig = ForkingTaskRunnerTest.makeDefaultTaskConfigBuilder().build();
    final WorkerConfig workerConfig = new WorkerConfig();
    runner = new ThreadingTaskRunner(
        mockTaskToolboxFactory(),
        taskConfig,
        workerConfig,
        new NoopTaskLogs(),
        new DefaultObjectMapper(),
        new TestAppenderatorsManager(),
        new MultipleFileTaskReportFileWriter(),
        new DruidNode("druid/indexer", "host", false, 8091, null, true, false),
        TaskStorageDirTracker.fromConfigs(workerConfig, taskConfig)
    );
  }

  @Test
  public void testTaskStatusWhenTaskThrowsExceptionWhileRunning() throws ExecutionException, InterruptedException
  {
    Future<TaskStatus> statusFuture = runner.run(new NoopTask(null, null, null, 1L, 0L, Map.of())
    {
      @Override
      public TaskStatus runTask(TaskToolbox toolbox)
      {
        throw new RuntimeException("Task failure test");
      }
    });

    TaskStatus status = statusFuture.get();
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(
        "Failed with exception [Task failure test]. See indexer logs for details.",
        status.getErrorMsg()
    );
  }

  @Test
  public void test_streamTaskLogs_ofRunningTask_readsFromTaskLogFile() throws Exception
  {
    final CountDownLatch taskHasStarted = new CountDownLatch(1);
    final CountDownLatch finishTask = new CountDownLatch(1);
    final Task indexerTask = new NoopTask(null, null, null, 1L, 0L, Map.of())
    {
      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        log.info("Running test task[%s]", getId());

        taskHasStarted.countDown();
        finishTask.await();

        return TaskStatus.success(getId());
      }
    };

    // Submit the task and wait for it to start
    final Future<TaskStatus> statusFuture = runner.run(indexerTask);
    taskHasStarted.await();

    // Stream and verify the contents of the task logs
    final Optional<InputStream> logStream = runner.streamTaskLog(indexerTask.getId(), 0);
    Assert.assertTrue(logStream.isPresent());

    try (final InputStream in = logStream.get()) {
      final String fullTaskLogs = IOUtils.toString(in, StandardCharsets.UTF_8);
      Assert.assertTrue(
          fullTaskLogs.contains(
              StringUtils.format("Running test task[%s]", indexerTask.getId())
          )
      );
    }

    // Finish the task and verify status
    finishTask.countDown();
    Assert.assertEquals(
        TaskStatus.success(indexerTask.getId()),
        statusFuture.get()
    );

    // Verify that task logs cannot be streamed anymore as task has finished
    Assert.assertFalse(
        runner.streamTaskLog(indexerTask.getId(), 0).isPresent()
    );
  }

  private static TaskToolboxFactory mockTaskToolboxFactory()
  {
    return new TestTaskToolboxFactory(new TestTaskToolboxFactory.Builder());
  }
}
