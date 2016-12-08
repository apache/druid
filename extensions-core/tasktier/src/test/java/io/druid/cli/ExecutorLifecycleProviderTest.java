/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

@RunWith(Parameterized.class)
public class ExecutorLifecycleProviderTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        Arrays.asList(TaskStatus.Status.values()),
        new Function<TaskStatus.Status, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(TaskStatus.Status status)
          {
            return new Object[]{status};
          }
        }
    );
  }

  private static final String TASK_ID = "taskId";
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final File taskFile = new File(TierLocalTaskRunner.TASK_FILE_NAME);
  private final File logFile = new File(TierLocalTaskRunner.LOG_FILE_NAME);
  private final File portFile = new File(TierLocalTaskRunner.PORT_FILE_NAME);
  private final File statusFile = new File(TierLocalTaskRunner.STATUS_FILE_NAME);

  private final TaskStatus.Status status;

  public ExecutorLifecycleProviderTest(TaskStatus.Status status) throws IOException
  {
    this.status = status;
  }

  @Before
  public void setUp()
  {
    clearFiles();
  }

  @After
  public void cleanUp()
  {
    clearFiles();
  }

  private void clearFiles()
  {
    taskFile.delete();
    logFile.delete();
    portFile.delete();
    statusFile.delete();
  }

  private void doTestTask(TaskStatus.Status retval) throws Exception
  {
    final TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);

    final TaskRunner taskRunner = EasyMock.createNiceMock(TaskRunner.class);
    final SettableFuture<TaskStatus> future = SettableFuture.create();
    EasyMock.expect(taskRunner.run(EasyMock.anyObject(Task.class))).andReturn(future).once();
    EasyMock.replay(taskRunner);

    final CountDownLatch neverEnd = new CountDownLatch(1);
    final ParentMonitorInputStreamFaker parentStream = new ParentMonitorInputStreamFaker()
    {
      @Override
      public int read() throws IOException
      {
        try {
          neverEnd.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return -1;
      }
    };
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(TestTask.class);
    final TaskConfig taskConfig = new TaskConfig(
        temporaryFolder.newFolder().getAbsolutePath(),
        temporaryFolder.newFolder().getAbsolutePath(),
        temporaryFolder.newFolder().getAbsolutePath(),
        null,
        null,
        false,
        Period.seconds(1),
        Period.seconds(1)
    );
    final File taskLockParent = taskConfig.getTaskLockFile(TASK_ID).getParentFile();
    Assert.assertTrue(taskLockParent.isDirectory() || taskLockParent.mkdirs());
    final ExecutorLifecycleProvider provider = new ExecutorLifecycleProvider(
        taskActionClientFactory,
        taskRunner,
        taskConfig,
        parentStream,
        jsonMapper
    );

    Assert.assertFalse(taskFile.exists());
    final TestTask task = new TestTask(retval, TASK_ID, "dataSource");
    jsonMapper.writeValue(taskFile, task);
    final ExecutorLifecycle lifecycle = provider.get();
    Assert.assertTrue(statusFile.exists());
    lifecycle.start();
    future.set(TaskStatus.fromCode(task.getId(), retval));
    while (!statusFile.exists()) {
      // NOOP
    }
    lifecycle.stop();
    final TaskStatus status = jsonMapper.readValue(statusFile, TaskStatus.class);
    Assert.assertEquals(task.getId(), status.getId());
    Assert.assertEquals(retval, status.getStatusCode());
  }

  @Test(timeout = 1_000L)
  public void testGet() throws Exception
  {
    doTestTask(status);
  }

  @JsonTypeName(TestTask.TYPE)
  public static class TestTask extends AbstractTask
  {
    static final String TYPE = "testSuccessTask";
    private final TaskStatus.Status status;

    @JsonCreator
    public TestTask(
        @JsonProperty("status") TaskStatus.Status status,
        @JsonProperty("id") String id,
        @JsonProperty("dataSource") @NotNull String dataSource
    )
    {
      super(
          AbstractTask.makeId(id, TYPE, Preconditions.checkNotNull(dataSource), Interval.parse("2015/2016")),
          dataSource,
          ImmutableMap.<String, Object>of()
      );
      this.status = status;
    }

    @JsonProperty("status")
    public TaskStatus.Status getStatus()
    {
      return status;
    }

    @Override
    public String getType()
    {
      return TYPE;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return true;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      return TaskStatus.fromCode(getId(), status);
    }
  }
}

