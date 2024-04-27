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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.common.JobResponse;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.PeonPhase;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(EasyMockRunner.class)
public class KubernetesPeonLifecycleTest extends EasyMockSupport
{
  private static final String ID = "id";
  private static final TaskStatus SUCCESS = TaskStatus.success(ID);

  @Mock KubernetesPeonClient kubernetesClient;
  @Mock TaskLogs taskLogs;

  @Mock LogWatch logWatch;
  @Mock KubernetesPeonLifecycle.TaskStateListener stateListener;

  private ObjectMapper mapper;
  private Task task;
  private K8sTaskId k8sTaskId;

  @Before
  public void setup()
  {
    mapper = new TestUtils().getTestObjectMapper();
    task = K8sTestUtils.createTask(ID, 0);
    k8sTaskId = new K8sTaskId(task);
    EasyMock.expect(logWatch.getOutput()).andReturn(IOUtils.toInputStream("", StandardCharsets.UTF_8)).anyTimes();
  }

  @Test
  public void test_run() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    )
    {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        return TaskStatus.success(ID);
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.eq(task),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    stateListener.stateChanged(KubernetesPeonLifecycle.State.PENDING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    replayAll();

    TaskStatus taskStatus = peonLifecycle.run(job, 0L, 0L, false);

    verifyAll();

    Assert.assertTrue(taskStatus.isSuccess());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_run_useTaskManager() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    )
    {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        return TaskStatus.success(ID);
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.eq(task),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);
    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    stateListener.stateChanged(KubernetesPeonLifecycle.State.PENDING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    taskLogs.pushTaskPayload(EasyMock.anyString(), EasyMock.anyObject());
    replayAll();

    TaskStatus taskStatus = peonLifecycle.run(job, 0L, 0L, true);

    verifyAll();
    Assert.assertTrue(taskStatus.isSuccess());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_run_whenCalledMultipleTimes_raisesIllegalStateException() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    )
    {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        return TaskStatus.success(ID);
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.eq(task),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    stateListener.stateChanged(KubernetesPeonLifecycle.State.PENDING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    replayAll();

    peonLifecycle.run(job, 0L, 0L, false);

    Assert.assertThrows(
        "Task [id] failed to run: invalid peon lifecycle state transition [STOPPED]->[PENDING]",
        IllegalStateException.class,
        () -> peonLifecycle.run(job, 0L, 0L, false)
    );

    verifyAll();

    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_run_whenExceptionRaised_setsRunnerTaskStateToNone()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    )
    {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        throw new IllegalStateException();
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.eq(task),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);
    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());
    stateListener.stateChanged(KubernetesPeonLifecycle.State.PENDING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    replayAll();

    Assert.assertThrows(
        Exception.class,
        () -> peonLifecycle.run(job, 0L, 0L, false)
    );

    verifyAll();

    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_withoutJob_returnsFailedTaskStatus() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(null, PeonPhase.FAILED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.absent());
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(Optional.absent());

    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals("Peon did not report status successfully.", taskStatus.getErrorMsg());
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .withStartTime("2022-09-19T23:31:50Z")
        .withCompletionTime("2022-09-19T23:32:48Z")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(job, PeonPhase.SUCCEEDED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.of(logWatch));
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(Optional.of(
        IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8)
    ));
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();
    logWatch.close();
    EasyMock.expectLastCall();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertEquals(SUCCESS.withDuration(58000), taskStatus);
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_whenCalledMultipleTimes_raisesIllegalStateException() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(job, PeonPhase.SUCCEEDED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.of(logWatch));
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(
        Optional.of(IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8))
    );
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    logWatch.close();
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();
    logWatch.close();
    EasyMock.expectLastCall();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    Assert.assertThrows(
        "Task [id] failed to join: invalid peon lifecycle state transition [STOPPED]->[PENDING]",
        IllegalStateException.class,
        () -> peonLifecycle.join(0L)
    );

    verifyAll();

    Assert.assertEquals(SUCCESS, taskStatus);
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_withoutTaskStatus_returnsFailedTaskStatus() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(job, PeonPhase.SUCCEEDED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.absent());
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(Optional.absent());
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals("Peon did not report status successfully.", taskStatus.getErrorMsg());
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_whenIOExceptionThrownWhileStreamingTaskStatus_returnsFailedTaskStatus() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(job, PeonPhase.SUCCEEDED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.of(logWatch));
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andThrow(new IOException());
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();
    logWatch.close();
    EasyMock.expectLastCall();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals("error loading status: null", taskStatus.getErrorMsg());
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_whenIOExceptionThrownWhileStreamingTaskLogs_isIgnored() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(new JobResponse(job, PeonPhase.SUCCEEDED));
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.of(logWatch));
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(
        Optional.of(IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8))
    );
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall().andThrow(new IOException());
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();
    logWatch.close();
    EasyMock.expectLastCall();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertEquals(SUCCESS, taskStatus);
    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_join_whenRuntimeExceptionThrownWhileWaitingForKubernetesJob_throwsException() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );

    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andThrow(new RuntimeException());

    // We should still try to push logs
    EasyMock.expect(kubernetesClient.getPeonLogWatcher(k8sTaskId)).andReturn(Optional.of(logWatch));
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.RUNNING, ID);
    EasyMock.expectLastCall().once();
    stateListener.stateChanged(KubernetesPeonLifecycle.State.STOPPED, ID);
    EasyMock.expectLastCall().once();
    logWatch.close();
    EasyMock.expectLastCall();

    Assert.assertEquals(KubernetesPeonLifecycle.State.NOT_STARTED, peonLifecycle.getState());

    replayAll();

    Assert.assertThrows(RuntimeException.class, () -> peonLifecycle.join(0L));

    verifyAll();

    Assert.assertEquals(KubernetesPeonLifecycle.State.STOPPED, peonLifecycle.getState());
  }

  @Test
  public void test_shutdown_withNotStartedTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    peonLifecycle.shutdown();
  }

  @Test
  public void test_shutdown_withPendingTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.PENDING);

    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    replayAll();

    peonLifecycle.shutdown();

    verifyAll();
  }

  @Test
  public void test_shutdown_withRunningTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    replayAll();

    peonLifecycle.shutdown();

    verifyAll();
  }

  @Test
  public void test_shutdown_withStoppedTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.STOPPED);

    peonLifecycle.shutdown();
  }

  @Test
  public void test_streamLogs_withNotStartedTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.NOT_STARTED);

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_streamLogs_withPendingTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.PENDING);

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_streamLogs_withRunningTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );

    replayAll();

    peonLifecycle.streamLogs();

    verifyAll();
  }

  @Test
  public void test_streamLogs_withStoppedTaskState() throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.STOPPED);

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_getTaskLocation_withNotStartedTaskState_returnsUnknown()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.NOT_STARTED);

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
  }

  @Test
  public void test_getTaskLocation_withPendingTaskState_returnsUnknown()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.PENDING);

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withoutPeonPod_returnsUnknown()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.absent());

    replayAll();

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithoutStatus_returnsUnknown()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.of(pod));

    replayAll();

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithStatus_returnsLocation()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.of(pod));

    replayAll();

    TaskLocation location = peonLifecycle.getTaskLocation();

    Assert.assertEquals("ip", location.getHost());
    Assert.assertEquals(8100, location.getPort());
    Assert.assertEquals(-1, location.getTlsPort());
    Assert.assertEquals(ID, location.getK8sPodName());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_saveTaskLocation()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.of(pod)).once();

    replayAll();

    TaskLocation location = peonLifecycle.getTaskLocation();
    peonLifecycle.getTaskLocation();
    Assert.assertEquals("ip", location.getHost());
    Assert.assertEquals(8100, location.getPort());
    Assert.assertEquals(-1, location.getTlsPort());
    Assert.assertEquals(ID, location.getK8sPodName());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithStatusWithTLSAnnotation_returnsLocation()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.RUNNING);

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .addToAnnotations("tls.enabled", "true")
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.of(pod));

    replayAll();

    TaskLocation location = peonLifecycle.getTaskLocation();

    Assert.assertEquals("ip", location.getHost());
    Assert.assertEquals(-1, location.getPort());
    Assert.assertEquals(8091, location.getTlsPort());
    Assert.assertEquals(ID, location.getK8sPodName());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withStoppedTaskState_returnsUnknown()
      throws NoSuchFieldException, IllegalAccessException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(
        task,
        kubernetesClient,
        taskLogs,
        mapper,
        stateListener
    );
    setPeonLifecycleState(peonLifecycle, KubernetesPeonLifecycle.State.STOPPED);
    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId.getK8sJobName())).andReturn(Optional.absent()).once();

    replayAll();
    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
    verifyAll();
  }

  private void setPeonLifecycleState(KubernetesPeonLifecycle peonLifecycle, KubernetesPeonLifecycle.State state)
      throws NoSuchFieldException, IllegalAccessException
  {
    Field stateField = peonLifecycle.getClass().getDeclaredField("state");
    stateField.setAccessible(true);
    stateField.set(peonLifecycle, new AtomicReference<>(state));
  }
}
