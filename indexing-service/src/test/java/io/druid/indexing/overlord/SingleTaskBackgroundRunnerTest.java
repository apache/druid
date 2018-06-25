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

package io.druid.indexing.overlord;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskReportFileWriter;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.NoopTask;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.segment.loading.NoopDataSegmentArchiver;
import io.druid.segment.loading.NoopDataSegmentKiller;
import io.druid.segment.loading.NoopDataSegmentMover;
import io.druid.segment.loading.NoopDataSegmentPusher;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.server.DruidNode;
import io.druid.server.coordination.NoopDataSegmentAnnouncer;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SingleTaskBackgroundRunnerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SingleTaskBackgroundRunner runner;

  @Before
  public void setup() throws IOException
  {
    final TestUtils utils = new TestUtils();
    final DruidNode node = new DruidNode("testServer", "testHost", 1000, null, true, false);
    final TaskConfig taskConfig = new TaskConfig(
        temporaryFolder.newFile().toString(),
        null,
        null,
        50000,
        null,
        true,
        null,
        null
    );
    final ServiceEmitter emitter = new NoopServiceEmitter();
    final TaskToolboxFactory toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        EasyMock.createMock(TaskActionClientFactory.class),
        emitter,
        new NoopDataSegmentPusher(),
        new NoopDataSegmentKiller(),
        new NoopDataSegmentMover(),
        new NoopDataSegmentArchiver(),
        new NoopDataSegmentAnnouncer(),
        null,
        null,
        null,
        null,
        null,
        new SegmentLoaderFactory(EasyMock.createMock(SegmentLoaderLocalCacheManager.class)),
        utils.getTestObjectMapper(),
        utils.getTestIndexIO(),
        null,
        null,
        null,
        utils.getTestIndexMergerV9(),
        null,
        node,
        null,
        null,
        new TaskReportFileWriter(new File("fake"))
    );
    runner = new SingleTaskBackgroundRunner(
        toolboxFactory,
        taskConfig,
        emitter,
        node,
        new ServerConfig()
    );
  }

  @After
  public void teardown()
  {
    runner.stop();
  }

  @Test
  public void testRun() throws ExecutionException, InterruptedException
  {
    Assert.assertEquals(
        TaskState.SUCCESS,
        runner.run(new NoopTask(null, null, 500L, 0, null, null, null)).get().getStatusCode()
    );
  }

  @Test
  public void testStop() throws ExecutionException, InterruptedException, TimeoutException
  {
    final ListenableFuture<TaskStatus> future = runner.run(
        new NoopTask(null, null, Long.MAX_VALUE, 0, null, null, null) // infinite task
    );
    runner.stop();
    Assert.assertEquals(
        TaskState.FAILED,
        future.get(1000, TimeUnit.MILLISECONDS).getStatusCode()
    );
  }

  @Test
  public void testStopWithRestorableTask() throws InterruptedException, ExecutionException, TimeoutException
  {
    final BooleanHolder holder = new BooleanHolder();
    final ListenableFuture<TaskStatus> future = runner.run(
        new RestorableTask(holder)
    );
    runner.stop();
    Assert.assertEquals(
        TaskState.SUCCESS,
        future.get(1000, TimeUnit.MILLISECONDS).getStatusCode()
    );
    Assert.assertTrue(holder.get());
  }

  private static class RestorableTask extends AbstractTask
  {
    private final BooleanHolder gracefullyStopped;

    RestorableTask(BooleanHolder gracefullyStopped)
    {
      super("testId", "testDataSource", Collections.emptyMap());

      this.gracefullyStopped = gracefullyStopped;
    }

    @Override
    public String getType()
    {
      return "restorable";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox)
    {
      return TaskStatus.success(getId());
    }

    @Override
    public boolean canRestore()
    {
      return true;
    }

    @Override
    public void stopGracefully()
    {
      gracefullyStopped.set();
    }
  }

  private static class BooleanHolder
  {
    private boolean value;

    void set()
    {
      this.value = true;
    }

    boolean get()
    {
      return value;
    }
  }
}
