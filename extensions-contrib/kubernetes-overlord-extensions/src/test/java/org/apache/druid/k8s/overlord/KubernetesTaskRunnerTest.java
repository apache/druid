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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EasyMockRunner.class)
@SuppressWarnings("DoNotMock")
public class KubernetesTaskRunnerTest extends EasyMockSupport
{
  private static final String ID = "id";

  @Mock private HttpClient httpClient;
  @Mock private TaskAdapter taskAdapter;
  @Mock private KubernetesPeonClient peonClient;
  @Mock private KubernetesPeonLifecycle kubernetesPeonLifecycle;
  @Mock private ServiceEmitter emitter;

  private KubernetesTaskRunnerConfig config;
  private KubernetesTaskRunner runner;
  private Task task;

  @Before
  public void setup()
  {
    config = KubernetesTaskRunnerConfig.builder()
        .withCapacity(1)
        .build();

    task = K8sTestUtils.createTask(ID, 0);

    runner = new KubernetesTaskRunner(
        taskAdapter,
        config,
        peonClient,
        httpClient,
        new TestPeonLifecycleFactory(kubernetesPeonLifecycle),
        emitter
    );
  }

  @Test
  public void test_start_withExistingJobs() throws IOException
  {
    KubernetesTaskRunner runner = new KubernetesTaskRunner(
        taskAdapter,
        config,
        peonClient,
        httpClient,
        new TestPeonLifecycleFactory(kubernetesPeonLifecycle),
        emitter
    )
    {
      @Override
      protected ListenableFuture<TaskStatus> joinAsync(Task task)
      {
        return tasks.computeIfAbsent(
            task.getId(),
            k -> new KubernetesWorkItem(
                task,
                Futures.immediateFuture(TaskStatus.success(task.getId()))
            )
        ).getResult();
      }
    };

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    EasyMock.expect(peonClient.getPeonJobs()).andReturn(ImmutableList.of(job));
    EasyMock.expect(taskAdapter.toTask(job)).andReturn(task);

    replayAll();

    runner.start();

    verifyAll();

    Assert.assertNotNull(runner.tasks);
    Assert.assertEquals(1, runner.tasks.size());
  }

  @Test
  public void test_start_whenDeserializationExceptionThrown_isIgnored() throws IOException
  {
    KubernetesTaskRunner runner = new KubernetesTaskRunner(
        taskAdapter,
        config,
        peonClient,
        httpClient,
        new TestPeonLifecycleFactory(kubernetesPeonLifecycle),
        emitter
    )
    {
      @Override
      protected ListenableFuture<TaskStatus> joinAsync(Task task)
      {
        return tasks.computeIfAbsent(task.getId(), k -> new KubernetesWorkItem(task, null))
                    .getResult();
      }
    };

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    EasyMock.expect(peonClient.getPeonJobs()).andReturn(ImmutableList.of(job));
    EasyMock.expect(taskAdapter.toTask(job)).andThrow(new IOException());

    replayAll();

    runner.start();

    verifyAll();

    Assert.assertNotNull(runner.tasks);
    Assert.assertEquals(0, runner.tasks.size());
  }

  @Test
  public void test_streamTaskLog_withoutExistingTask_returnsEmptyOptional()
  {
    Optional<InputStream> maybeInputStream = runner.streamTaskLog(task.getId(), 0L);
    assertFalse(maybeInputStream.isPresent());
  }

  @Test
  public void test_streamTaskLog_withExistingTask() throws IOException
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null)
    {
      @Override
      protected Optional<InputStream> streamTaskLogs()
      {
        return Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8));
      }
    };

    runner.tasks.put(task.getId(), workItem);

    Optional<InputStream> maybeInputStream = runner.streamTaskLog(task.getId(), 0L);

    assertTrue(maybeInputStream.isPresent());
    assertEquals("", IOUtils.toString(maybeInputStream.get(), StandardCharsets.UTF_8));
  }

  @Test
  public void test_run_withoutExistingTask() throws IOException, ExecutionException, InterruptedException
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    TaskStatus taskStatus = TaskStatus.success(task.getId());

    EasyMock.expect(taskAdapter.fromTask(task)).andReturn(job);
    EasyMock.expect(taskAdapter.shouldUseDeepStorageForTaskPayload(task)).andReturn(false);
    EasyMock.expect(kubernetesPeonLifecycle.run(
        EasyMock.eq(job),
        EasyMock.anyLong(),
        EasyMock.anyLong(),
        EasyMock.anyBoolean()
    )).andReturn(taskStatus);

    replayAll();

    ListenableFuture<TaskStatus> future = runner.run(task);
    Assert.assertEquals(taskStatus, future.get());

    verifyAll();
  }

  @Test
  public void test_run_withExistingTask_returnsExistingWorkItem()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null);
    runner.tasks.put(task.getId(), workItem);

    ListenableFuture<TaskStatus> future = runner.run(task);

    assertEquals(workItem.getResult(), future);
  }

  @Test
  public void test_run_whenExceptionThrown_throwsRuntimeException() throws IOException
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    EasyMock.expect(taskAdapter.fromTask(task)).andReturn(job);
    EasyMock.expect(taskAdapter.shouldUseDeepStorageForTaskPayload(task)).andReturn(false);
    EasyMock.expect(kubernetesPeonLifecycle.run(
        EasyMock.eq(job),
        EasyMock.anyLong(),
        EasyMock.anyLong(),
        EasyMock.anyBoolean()
    )).andThrow(new IllegalStateException());

    replayAll();

    ListenableFuture<TaskStatus> future = runner.run(task);

    Exception e = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertTrue(e.getCause() instanceof RuntimeException);

    verifyAll();
  }

  @Test
  public void test_join_withoutExistingTask() throws ExecutionException, InterruptedException
  {
    TaskStatus taskStatus = TaskStatus.success(task.getId());

    EasyMock.expect(kubernetesPeonLifecycle.join(EasyMock.anyLong())).andReturn(taskStatus);

    replayAll();

    ListenableFuture<TaskStatus> future = runner.joinAsync(task);
    Assert.assertEquals(taskStatus, future.get());

    verifyAll();
  }

  @Test
  public void test_join_withExistingTask_returnsExistingWorkItem()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null);
    runner.tasks.put(task.getId(), workItem);

    ListenableFuture<TaskStatus> future = runner.run(task);

    assertEquals(workItem.getResult(), future);
  }

  @Test
  public void test_join_whenExceptionThrown_throwsRuntimeException()
  {
    EasyMock.expect(kubernetesPeonLifecycle.join(EasyMock.anyLong())).andThrow(new IllegalStateException());

    replayAll();

    ListenableFuture<TaskStatus> future = runner.joinAsync(task);

    Exception e = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertTrue(e.getCause() instanceof RuntimeException);

    verifyAll();
  }

  @Test
  public void test_doTask_whenShutdownRequested_throwsRuntimeException()
  {
    Assert.assertThrows(
        "Task [id] has been shut down",
        RuntimeException.class,
        () -> runner.doTask(task, true)
    );
  }

  @Test
  public void test_shutdown_withExistingTask_removesTaskFromMap()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      protected synchronized void shutdown()
      {
      }
    };

    runner.tasks.put(task.getId(), workItem);
    runner.shutdown(task.getId(), "");
    Assert.assertTrue(runner.tasks.isEmpty());
  }

  @Test
  public void test_shutdown_withoutExistingTask()
  {
    runner.shutdown(task.getId(), "");
  }

  @Test
  public void test_getTotalTaskSlotCount()
  {
    Map<String, Long> slotCount = runner.getTotalTaskSlotCount();

    MatcherAssert.assertThat(slotCount, Matchers.allOf(
        Matchers.aMapWithSize(1),
        Matchers.hasEntry(
            Matchers.equalTo(KubernetesTaskRunner.WORKER_CATEGORY),
            Matchers.equalTo(1L)
        )
    ));
  }

  @Test
  public void test_getKnownTasks()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null);

    runner.tasks.put(task.getId(), workItem);

    Collection<? extends TaskRunnerWorkItem> tasks = runner.getKnownTasks();

    assertEquals(1, tasks.size());
    assertEquals(Collections.singletonList(workItem), runner.getKnownTasks());
  }

  @Test
  public void test_getRunningTasks()
  {
    Task pendingTask = K8sTestUtils.createTask("pending-id", 0);
    KubernetesWorkItem pendingWorkItem = new KubernetesWorkItem(pendingTask, null) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.PENDING;
      }
    };
    runner.tasks.put(pendingTask.getId(), pendingWorkItem);

    Task runningTask = K8sTestUtils.createTask("running-id", 0);
    KubernetesWorkItem runningWorkItem = new KubernetesWorkItem(runningTask, null) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };
    runner.tasks.put(runningTask.getId(), runningWorkItem);

    Collection<? extends TaskRunnerWorkItem> tasks = runner.getRunningTasks();

    assertEquals(1, tasks.size());
    assertEquals(Collections.singletonList(runningWorkItem), tasks);
  }

  @Test
  public void test_getPendingTasks()
  {
    Task pendingTask = K8sTestUtils.createTask("pending-id", 0);
    KubernetesWorkItem pendingWorkItem = new KubernetesWorkItem(pendingTask, null) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.PENDING;
      }
    };
    runner.tasks.put(pendingTask.getId(), pendingWorkItem);

    Task runningTask = K8sTestUtils.createTask("running-id", 0);
    KubernetesWorkItem runningWorkItem = new KubernetesWorkItem(runningTask, null) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };
    runner.tasks.put(runningTask.getId(), runningWorkItem);

    Collection<? extends TaskRunnerWorkItem> tasks = runner.getPendingTasks();

    assertEquals(1, tasks.size());
    assertEquals(Collections.singletonList(pendingWorkItem), tasks);
  }

  @Test
  public void test_getRunnerTaskState_withoutExistingTask_returnsNull()
  {
    Assert.assertNull(runner.getRunnerTaskState(task.getId()));
  }

  @Test
  public void test_getRunnerTaskState_withExistingTask()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.NONE;
      }
    };
    runner.tasks.put(task.getId(), workItem);

    Assert.assertEquals(RunnerTaskState.NONE, runner.getRunnerTaskState(task.getId()));
  }

  @Test
  public void test_streamTaskReports_withExistingTask() throws Exception
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.create("host", 0, 1, false);
      }
    };

    runner.tasks.put(task.getId(), workItem);

    EasyMock.expect(httpClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(InputStreamResponseHandler.class))
    ).andReturn(Futures.immediateFuture(IOUtils.toInputStream("{}", StandardCharsets.UTF_8)));

    replayAll();

    Optional<InputStream> maybeInputStream = runner.streamTaskReports(task.getId());

    verifyAll();

    Assert.assertTrue(maybeInputStream.isPresent());
    Assert.assertEquals("{}", IOUtils.toString(maybeInputStream.get(), StandardCharsets.UTF_8));
  }

  @Test
  public void test_streamTaskReports_withoutExistingTask_returnsEmptyOptional() throws Exception
  {
    Optional<InputStream> maybeInputStream = runner.streamTaskReports(task.getId());
    Assert.assertFalse(maybeInputStream.isPresent());
  }

  @Test
  public void test_streamTaskReports_withUnknownTaskLocation_returnsEmptyOptional() throws Exception
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.unknown();
      }
    };

    runner.tasks.put(task.getId(), workItem);

    Optional<InputStream> maybeInputStream = runner.streamTaskReports(task.getId());
    Assert.assertFalse(maybeInputStream.isPresent());
  }

  @Test
  public void test_streamTaskReports_whenInterruptedExceptionThrown_throwsRuntimeException()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.create("host", 0, 1, false);
      }
    };

    runner.tasks.put(task.getId(), workItem);

    ListenableFuture<InputStream> future = new ListenableFuture<InputStream>()
    {
      @Override
      public void addListener(Runnable runnable, Executor executor)
      {
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public InputStream get() throws InterruptedException
      {
        throw new InterruptedException();
      }

      @Override
      public InputStream get(long timeout, TimeUnit unit) throws InterruptedException
      {
        throw new InterruptedException();
      }
    };

    EasyMock.expect(httpClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(InputStreamResponseHandler.class))
    ).andReturn(future);

    replayAll();

    Exception e = Assert.assertThrows(RuntimeException.class, () -> runner.streamTaskReports(task.getId()));
    Assert.assertTrue(e.getCause() instanceof InterruptedException);

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_whenExecutionExceptionThrown_throwsRuntimeException()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.create("host", 0, 1, false);
      }
    };

    runner.tasks.put(task.getId(), workItem);

    EasyMock.expect(httpClient.go(
        EasyMock.anyObject(Request.class),
        EasyMock.anyObject(InputStreamResponseHandler.class))
    ).andReturn(Futures.immediateFailedFuture(new Exception()));

    replayAll();

    Assert.assertThrows(RuntimeException.class, () -> runner.streamTaskReports(task.getId()));

    verifyAll();
  }

  @Test
  public void test_metricsReported_whenTaskStateChange()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.unknown();
      }
    };

    runner.tasks.put(task.getId(), workItem);

    emitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));

    replayAll();

    runner.emitTaskStateMetrics(KubernetesPeonLifecycle.State.RUNNING, task.getId());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withExistingTask()
  {
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null) {
      @Override
      public TaskLocation getLocation()
      {
        return TaskLocation.create("host", 0, 1, false);
      }
    };

    runner.tasks.put(task.getId(), workItem);

    TaskLocation taskLocation = runner.getTaskLocation(task.getId());
    Assert.assertEquals(TaskLocation.create("host", 0, 1, false), taskLocation);
  }

  @Test
  public void test_getTaskLocation_noTaskFound()
  {
    TaskLocation taskLocation = runner.getTaskLocation(task.getId());
    Assert.assertEquals(TaskLocation.unknown(), taskLocation);
  }

  @Test
  public void test_getTotalCapacity()
  {
    Assert.assertEquals(1, runner.getTotalCapacity());
  }

  @Test
  public void test_getUsedCapacity()
  {
    Assert.assertEquals(0, runner.getUsedCapacity());
    KubernetesWorkItem workItem = new KubernetesWorkItem(task, null);
    runner.tasks.put(task.getId(), workItem);
    Assert.assertEquals(1, runner.getUsedCapacity());
    runner.tasks.remove(task.getId());
    Assert.assertEquals(0, runner.getUsedCapacity());
  }
}
