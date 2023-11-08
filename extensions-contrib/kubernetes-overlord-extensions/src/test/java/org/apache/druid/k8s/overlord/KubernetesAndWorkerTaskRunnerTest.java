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


package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.k8s.overlord.runnerstrategy.KubernetesRunnerStrategy;
import org.apache.druid.k8s.overlord.runnerstrategy.TaskTypeRunnerStrategy;
import org.apache.druid.k8s.overlord.runnerstrategy.WorkerRunnerStrategy;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@RunWith(EasyMockRunner.class)
public class KubernetesAndWorkerTaskRunnerTest extends EasyMockSupport
{

  private static final String ID = "id";
  private static final String DATA_SOURCE = "dataSource";
  @Mock KubernetesTaskRunner kubernetesTaskRunner;
  @Mock HttpRemoteTaskRunner workerTaskRunner;

  KubernetesAndWorkerTaskRunner runner;

  private Task task;

  @Before
  public void setup()
  {
    task = NoopTask.create();
    runner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new KubernetesRunnerStrategy()
    );
  }

  @Test
  public void test_runOnKubernetes() throws ExecutionException, InterruptedException
  {
    KubernetesAndWorkerTaskRunner kubernetesAndWorkerTaskRunner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new KubernetesRunnerStrategy()
    );
    TaskStatus taskStatus = TaskStatus.success(ID);
    EasyMock.expect(kubernetesTaskRunner.run(task)).andReturn(Futures.immediateFuture(taskStatus));

    replayAll();
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(task).get());
    verifyAll();
  }

  @Test
  public void test_runOnWorker() throws ExecutionException, InterruptedException
  {
    KubernetesAndWorkerTaskRunner kubernetesAndWorkerTaskRunner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new WorkerRunnerStrategy()
    );
    TaskStatus taskStatus = TaskStatus.success(ID);
    EasyMock.expect(workerTaskRunner.run(task)).andReturn(Futures.immediateFuture(taskStatus));

    replayAll();
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(task).get());
    verifyAll();
  }

  @Test
  public void test_runOnKubernetesOrWorkerBasedOnStrategy() throws ExecutionException, InterruptedException
  {
    TaskTypeRunnerStrategy runnerStrategy = new TaskTypeRunnerStrategy("k8s", ImmutableMap.of("index_kafka", "worker"));
    KubernetesAndWorkerTaskRunner kubernetesAndWorkerTaskRunner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        runnerStrategy
    );
    Task taskMock = EasyMock.createMock(Task.class);
    TaskStatus taskStatus = TaskStatus.success(ID);
    EasyMock.expect(taskMock.getId()).andReturn(ID).anyTimes();

    EasyMock.expect(taskMock.getType()).andReturn("index_kafka").once();
    EasyMock.expect(workerTaskRunner.run(taskMock)).andReturn(Futures.immediateFuture(taskStatus)).once();
    EasyMock.replay(taskMock, workerTaskRunner);
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(taskMock).get());
    EasyMock.verify(taskMock, workerTaskRunner);
    EasyMock.reset(taskMock, workerTaskRunner);

    EasyMock.expect(taskMock.getType()).andReturn("compact").once();
    EasyMock.expect(kubernetesTaskRunner.run(taskMock)).andReturn(Futures.immediateFuture(taskStatus)).once();
    EasyMock.replay(taskMock, kubernetesTaskRunner);
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(taskMock).get());
    EasyMock.verify(taskMock, kubernetesTaskRunner);
  }

  @Test
  public void test_getUsedCapacity()
  {
    EasyMock.expect(kubernetesTaskRunner.getUsedCapacity()).andReturn(1);
    EasyMock.expect(workerTaskRunner.getUsedCapacity()).andReturn(1);

    replayAll();
    Assert.assertEquals(2, runner.getUsedCapacity());
    verifyAll();
  }

  @Test
  public void test_getTotalCapacity()
  {
    EasyMock.expect(kubernetesTaskRunner.getTotalCapacity()).andReturn(1);
    EasyMock.expect(workerTaskRunner.getTotalCapacity()).andReturn(1);

    replayAll();
    Assert.assertEquals(2, runner.getTotalCapacity());
    verifyAll();
  }

  @Test
  public void test_getRunnerTaskState_kubernetes()
  {
    RunnerTaskState runnerTaskState = RunnerTaskState.RUNNING;
    EasyMock.expect(kubernetesTaskRunner.getRunnerTaskState(ID)).andReturn(runnerTaskState);

    replayAll();
    Assert.assertEquals(runnerTaskState, runner.getRunnerTaskState(ID));
    verifyAll();
  }

  @Test
  public void test_getRunnerTaskState_worker()
  {
    RunnerTaskState runnerTaskState = RunnerTaskState.RUNNING;
    EasyMock.expect(kubernetesTaskRunner.getRunnerTaskState(ID)).andReturn(null);
    EasyMock.expect(workerTaskRunner.getRunnerTaskState(ID)).andReturn(runnerTaskState);

    replayAll();
    Assert.assertEquals(runnerTaskState, runner.getRunnerTaskState(ID));
    verifyAll();
  }

  @Test
  public void test_streamTaskLog_kubernetes() throws IOException
  {
    InputStream inputStream = IOUtils.toInputStream("inputStream", Charset.defaultCharset());
    EasyMock.expect(kubernetesTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.of(inputStream));

    replayAll();
    Assert.assertEquals(inputStream, runner.streamTaskLog(ID, 0).get());
    verifyAll();
  }

  @Test
  public void test_streamTasklog_worker() throws IOException
  {
    InputStream inputStream = IOUtils.toInputStream("inputStream", Charset.defaultCharset());
    EasyMock.expect(kubernetesTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.absent());
    EasyMock.expect(workerTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.of(inputStream));

    replayAll();
    Assert.assertEquals(inputStream, runner.streamTaskLog(ID, 0).get());
    verifyAll();
  }

  @Test
  public void test_getBlacklistedTaskSlotCount()
  {
    Map<String, Long> kubernetesTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> workerTaskSlots = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getBlacklistedTaskSlotCount()).andReturn(kubernetesTaskSlots);
    EasyMock.expect(workerTaskRunner.getBlacklistedTaskSlotCount()).andReturn(workerTaskSlots);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(kubernetesTaskSlots).putAll(workerTaskSlots).build(),
        runner.getBlacklistedTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getLazyTaskSlotCount()
  {
    Map<String, Long> kubernetesTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> workerTaskSlots = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getLazyTaskSlotCount()).andReturn(kubernetesTaskSlots);
    EasyMock.expect(workerTaskRunner.getLazyTaskSlotCount()).andReturn(workerTaskSlots);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(kubernetesTaskSlots).putAll(workerTaskSlots).build(),
        runner.getLazyTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getIdleTaskSlotCount()
  {
    Map<String, Long> kubernetesTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> workerTaskSlots = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getLazyTaskSlotCount()).andReturn(kubernetesTaskSlots);
    EasyMock.expect(workerTaskRunner.getLazyTaskSlotCount()).andReturn(workerTaskSlots);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(kubernetesTaskSlots).putAll(workerTaskSlots).build(),
        runner.getLazyTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getTotalTaskSlotCount()
  {
    Map<String, Long> kubernetesTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> workerTaskSlots = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getLazyTaskSlotCount()).andReturn(kubernetesTaskSlots);
    EasyMock.expect(workerTaskRunner.getLazyTaskSlotCount()).andReturn(workerTaskSlots);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(kubernetesTaskSlots).putAll(workerTaskSlots).build(),
        runner.getLazyTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getKnownTasks()
  {
    EasyMock.expect(kubernetesTaskRunner.getKnownTasks()).andReturn(ImmutableList.of());
    EasyMock.expect(workerTaskRunner.getKnownTasks()).andReturn(ImmutableList.of());

    replayAll();
    Assert.assertEquals(
        0,
        runner.getKnownTasks().size()
    );
    verifyAll();
  }

  @Test
  public void test_getPendingTasks()
  {
    EasyMock.expect(kubernetesTaskRunner.getPendingTasks()).andReturn(ImmutableList.of());
    EasyMock.expect(workerTaskRunner.getPendingTasks()).andReturn(ImmutableList.of());

    replayAll();
    Assert.assertEquals(
        0,
        runner.getPendingTasks().size()
    );
    verifyAll();
  }

  @Test
  public void test_getRunningTasks()
  {
    EasyMock.expect(kubernetesTaskRunner.getRunningTasks()).andReturn(ImmutableList.of());
    EasyMock.expect(workerTaskRunner.getRunningTasks()).andReturn(ImmutableList.of());

    replayAll();
    Assert.assertEquals(
        0,
        runner.getRunningTasks().size()
    );
    verifyAll();
  }

  @Test
  public void test_shutdown()
  {
    String reason = "reason";
    kubernetesTaskRunner.shutdown(ID, reason);
    workerTaskRunner.shutdown(ID, reason);
    replayAll();
    runner.shutdown(ID, reason);
    verifyAll();
  }

  @Test
  public void test_restore()
  {
    EasyMock.expect(kubernetesTaskRunner.restore()).andReturn(ImmutableList.of());
    EasyMock.expect(workerTaskRunner.restore()).andReturn(ImmutableList.of());
    replayAll();
    Assert.assertEquals(0, runner.restore().size());
    verifyAll();
  }

  @Test
  public void test_getTaskLocation_kubernetes()
  {
    TaskLocation kubernetesTaskLocation = TaskLocation.create("host", 0, 0, false);
    EasyMock.expect(kubernetesTaskRunner.getTaskLocation(ID)).andReturn(kubernetesTaskLocation);
    replayAll();
    Assert.assertEquals(kubernetesTaskLocation, runner.getTaskLocation(ID));
    verifyAll();
  }

  @Test
  public void test_getTaskLocation_worker()
  {
    TaskLocation workerTaskLocation = TaskLocation.create("host", 0, 0, false);
    EasyMock.expect(kubernetesTaskRunner.getTaskLocation(ID)).andReturn(TaskLocation.unknown());
    EasyMock.expect(workerTaskRunner.getTaskLocation(ID)).andReturn(workerTaskLocation);

    replayAll();
    Assert.assertEquals(workerTaskLocation, runner.getTaskLocation(ID));
    verifyAll();
  }

  @Test
  public void test_updateStatus()
  {
    kubernetesTaskRunner.updateStatus(task, TaskStatus.running(ID));
    replayAll();
    runner.updateStatus(task, TaskStatus.running(ID));
    verifyAll();
  }

  @Test
  public void test_updateLocation()
  {
    kubernetesTaskRunner.updateLocation(task, TaskLocation.unknown());
    replayAll();
    runner.updateLocation(task, TaskLocation.unknown());
    verifyAll();
  }
}
