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

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
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
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ThreadingTaskRunnerTest
{

  @Test
  public void testTaskStatusWhenTaskThrowsExceptionWhileRunning() throws ExecutionException, InterruptedException
  {
    ThreadingTaskRunner runner = new ThreadingTaskRunner(
        mockTaskToolboxFactory(),
        new TaskConfig(
            null,
            null,
            null,
            null,
            ImmutableList.of(),
            false,
            new Period("PT0S"),
            new Period("PT10S"),
            ImmutableList.of(),
            false,
            false,
            TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
            null
        ),
        new WorkerConfig(),
        new NoopTaskLogs(),
        new DefaultObjectMapper(),
        new TestAppenderatorsManager(),
        new MultipleFileTaskReportFileWriter(),
        new DruidNode("middleManager", "host", false, 8091, null, true, false)
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
      public TaskStatus run(TaskToolbox toolbox)
      {
        throw new RuntimeException("Task failure test");
      }
    });

    TaskStatus status = statusFuture.get();
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(
        "Failed with an exception. See indexer logs for more details.",
        status.getErrorMsg()
    );
  }

  private static TaskToolboxFactory mockTaskToolboxFactory()
  {
    TaskToolboxFactory factory = Mockito.mock(TaskToolboxFactory.class);
    Mockito.when(factory.build(ArgumentMatchers.any())).thenReturn(Mockito.mock(TaskToolbox.class));
    return factory;
  }
}
