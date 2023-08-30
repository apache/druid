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

import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskStorageDirTracker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ThreadingTaskRunnerTest
{

  @Test
  public void testTaskStatusWhenTaskThrowsExceptionWhileRunning() throws ExecutionException, InterruptedException
  {
    final TaskConfig taskConfig = ForkingTaskRunnerTest.makeDefaultTaskConfigBuilder().build();
    final WorkerConfig workerConfig = new WorkerConfig();
    ThreadingTaskRunner runner = new ThreadingTaskRunner(
        mockTaskToolboxFactory(),
        taskConfig,
        workerConfig,
        new NoopTaskLogs(),
        new DefaultObjectMapper(),
        new TestAppenderatorsManager(),
        new MultipleFileTaskReportFileWriter(),
        new DruidNode("middleManager", "host", false, 8091, null, true, false),
        TaskStorageDirTracker.fromConfigs(workerConfig, taskConfig)
    );

    Future<TaskStatus> statusFuture = runner.run(new AbstractTask("id", "datasource", null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        return true;
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

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

  private static TaskToolboxFactory mockTaskToolboxFactory()
  {
    return new TestTaskToolboxFactory(new TestTaskToolboxFactory.Builder());
  }
}
