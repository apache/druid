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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.UpdateStatusAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractTaskTest
{
  private ObjectMapper objectMapper;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup()
  {
    objectMapper = new TestUtils().getTestObjectMapper();
  }

  @Test
  public void testSetupAndCleanupIsCalledWtihParameter() throws Exception
  {
    // These tests apparently use Mockito.  Mockito is bad as we've seen it rewrite byte code and effectively cause
    // impact to other totally unrelated tests.  Mockito needs to be completely erradicated from the codebase.  This
    // comment is here to either cause me to do it in this commit or just for posterity so that it is clear that it
    // should happen in the future.
    TaskToolbox toolbox = mock(TaskToolbox.class);
    when(toolbox.getAttemptId()).thenReturn("1");

    DruidNode node = new DruidNode("foo", "foo", false, 1, 2, true, true);
    when(toolbox.getTaskExecutorNode()).thenReturn(node);

    TaskLogPusher pusher = mock(TaskLogPusher.class);
    when(toolbox.getTaskLogPusher()).thenReturn(pusher);

    TaskConfig config = mock(TaskConfig.class);
    when(config.isEncapsulatedTask()).thenReturn(true);
    File folder = temporaryFolder.newFolder();
    when(config.getTaskDir(eq("myID"))).thenReturn(folder);
    when(toolbox.getConfig()).thenReturn(config);
    when(toolbox.getJsonMapper()).thenReturn(objectMapper);

    TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(any())).thenReturn(TaskConfig.class);
    when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);


    AbstractTask task = new NoopTask("myID", null, null, 1, 0, null)
    {
      @Nullable
      @Override
      public String setup(TaskToolbox toolbox) throws Exception
      {
        // create a reports file to test the taskLogPusher pushes task reports
        String result = super.setup(toolbox);
        File attemptDir = Paths.get(folder.getAbsolutePath(), "attempt", toolbox.getAttemptId()).toFile();
        File reportsDir = new File(attemptDir, "report.json");
        File statusDir = new File(attemptDir, "status.json");
        FileUtils.write(reportsDir, "foo", StandardCharsets.UTF_8);
        FileUtils.write(statusDir, "{}", StandardCharsets.UTF_8);
        return result;
      }
    };
    task.run(toolbox);

    // call it 3 times, once to update location in setup, then one for status and location in cleanup
    Mockito.verify(taskActionClient, times(3)).submit(any());
    verify(pusher, times(1)).pushTaskReports(eq("myID"), any());
    verify(pusher, times(1)).pushTaskStatus(eq("myID"), any());
  }

  @Test
  public void testWithNoEncapsulatedTask() throws Exception
  {
    TaskToolbox toolbox = mock(TaskToolbox.class);
    when(toolbox.getAttemptId()).thenReturn("1");

    DruidNode node = new DruidNode("foo", "foo", false, 1, 2, true, true);
    when(toolbox.getTaskExecutorNode()).thenReturn(node);

    TaskLogPusher pusher = mock(TaskLogPusher.class);
    when(toolbox.getTaskLogPusher()).thenReturn(pusher);

    TaskConfig config = mock(TaskConfig.class);
    when(config.isEncapsulatedTask()).thenReturn(false);
    File folder = temporaryFolder.newFolder();
    when(config.getTaskDir(eq("myID"))).thenReturn(folder);
    when(toolbox.getConfig()).thenReturn(config);
    when(toolbox.getJsonMapper()).thenReturn(objectMapper);

    TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(any())).thenReturn(TaskConfig.class);
    when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);


    AbstractTask task = new NoopTask("myID", null, null, 1, 0, null)
    {
      @Nullable
      @Override
      public String setup(TaskToolbox toolbox) throws Exception
      {
        // create a reports file to test the taskLogPusher pushes task reports
        String result = super.setup(toolbox);
        File attemptDir = Paths.get(folder.getAbsolutePath(), "attempt", toolbox.getAttemptId()).toFile();
        File reportsDir = new File(attemptDir, "report.json");
        FileUtils.write(reportsDir, "foo", StandardCharsets.UTF_8);
        return result;
      }
    };
    task.run(toolbox);

    // encapsulated task is set to false, should never get called
    Mockito.verify(taskActionClient, never()).submit(any());
    verify(pusher, never()).pushTaskReports(eq("myID"), any());
  }

  @Test
  public void testTaskFailureWithoutExceptionGetsReportedCorrectly() throws Exception
  {
    TaskToolbox toolbox = mock(TaskToolbox.class);
    when(toolbox.getAttemptId()).thenReturn("1");

    DruidNode node = new DruidNode("foo", "foo", false, 1, 2, true, true);
    when(toolbox.getTaskExecutorNode()).thenReturn(node);

    TaskLogPusher pusher = mock(TaskLogPusher.class);
    when(toolbox.getTaskLogPusher()).thenReturn(pusher);

    TaskConfig config = mock(TaskConfig.class);
    when(config.isEncapsulatedTask()).thenReturn(true);
    File folder = temporaryFolder.newFolder();
    when(config.getTaskDir(eq("myID"))).thenReturn(folder);
    when(toolbox.getConfig()).thenReturn(config);
    when(toolbox.getJsonMapper()).thenReturn(objectMapper);

    TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(any())).thenReturn(TaskConfig.class);
    when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);

    TaskStatus taskStatus = TaskStatus.failure("myId", "failed");
    AbstractTask task = new NoopTask("myID", null, null, 1, 0, null)
    {
      @Override
      public TaskStatus runTask(TaskToolbox toolbox)
      {
        return taskStatus;
      }
    };
    task.run(toolbox);
    UpdateStatusAction action = new UpdateStatusAction("", taskStatus);
    verify(taskActionClient).submit(eq(action));
  }

  @Test
  public void testNullStackStatusGetsReportedCorrectly() throws Exception
  {
    TaskToolbox toolbox = mock(TaskToolbox.class);
    when(toolbox.getAttemptId()).thenReturn("1");

    DruidNode node = new DruidNode("foo", "foo", false, 1, 2, true, true);
    when(toolbox.getTaskExecutorNode()).thenReturn(node);

    TaskLogPusher pusher = mock(TaskLogPusher.class);
    when(toolbox.getTaskLogPusher()).thenReturn(pusher);

    TaskConfig config = mock(TaskConfig.class);
    when(config.isEncapsulatedTask()).thenReturn(true);
    File folder = temporaryFolder.newFolder();
    when(config.getTaskDir(eq("myID"))).thenReturn(folder);
    when(toolbox.getConfig()).thenReturn(config);
    when(toolbox.getJsonMapper()).thenReturn(objectMapper);

    TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(any())).thenReturn(TaskConfig.class);
    when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    AbstractTask task = new NoopTask("myID", null, null, 1, 0, null)
    {
      @Nullable
      @Override
      public TaskStatus runTask(TaskToolbox toolbox)
      {
        // Simulate the scenario where taskStatus is never set and cleanUp is called with null.
        return null;
      }
    };
    task.run(toolbox);
    UpdateStatusAction action = new UpdateStatusAction("", TaskStatus.failure(task.getId(), "Task failed to run"));
    verify(taskActionClient).submit(eq(action));
  }

  @Test
  public void testSetupFailsGetsReportedCorrectly() throws Exception
  {
    TaskToolbox toolbox = mock(TaskToolbox.class);
    when(toolbox.getAttemptId()).thenReturn("1");

    DruidNode node = new DruidNode("foo", "foo", false, 1, 2, true, true);
    when(toolbox.getTaskExecutorNode()).thenReturn(node);

    TaskLogPusher pusher = mock(TaskLogPusher.class);
    when(toolbox.getTaskLogPusher()).thenReturn(pusher);

    TaskConfig config = mock(TaskConfig.class);
    when(config.isEncapsulatedTask()).thenReturn(true);
    File folder = temporaryFolder.newFolder();
    when(config.getTaskDir(eq("myID"))).thenReturn(folder);
    when(toolbox.getConfig()).thenReturn(config);
    when(toolbox.getJsonMapper()).thenReturn(objectMapper);

    TaskActionClient taskActionClient = mock(TaskActionClient.class);
    when(taskActionClient.submit(any())).thenReturn(TaskConfig.class);
    when(toolbox.getTaskActionClient()).thenReturn(taskActionClient);
    AbstractTask task = new NoopTask("myID", null, null, 1, 0, null)
    {
      @Nullable
      @Override
      public String setup(TaskToolbox toolbox)
      {
        return "setup error";
      }
    };
    task.run(toolbox);
    UpdateStatusAction action = new UpdateStatusAction("", TaskStatus.failure(task.getId(), "setup error"));
    verify(taskActionClient).submit(eq(action));
  }


  @Test
  public void testBatchIOConfigAppend()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("APPEND");
    Assert.assertEquals(AbstractTask.IngestionMode.APPEND, ingestionMode);
  }

  @Test
  public void testBatchIOConfigReplace()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("REPLACE");
    Assert.assertEquals(AbstractTask.IngestionMode.REPLACE, ingestionMode);
  }

  @Test
  public void testBatchIOConfigOverwrite()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("REPLACE_LEGACY");
    Assert.assertEquals(AbstractTask.IngestionMode.REPLACE_LEGACY, ingestionMode);
  }

  @Test
  public void testBatchIOConfigHadoop()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("HADOOP");
    Assert.assertEquals(AbstractTask.IngestionMode.HADOOP, ingestionMode);
  }

  @Test
  public void testBatchIOConfigNone()
  {
    AbstractTask.IngestionMode ingestionMode = AbstractTask.IngestionMode.fromString("NONE");
    Assert.assertEquals(AbstractTask.IngestionMode.NONE, ingestionMode);
  }

}
