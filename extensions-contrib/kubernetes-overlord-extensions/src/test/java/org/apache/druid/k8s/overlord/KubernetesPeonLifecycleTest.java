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
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@RunWith(EasyMockRunner.class)
public class KubernetesPeonLifecycleTest extends EasyMockSupport
{
  private static final String ID = "id";
  private static final TaskStatus SUCCESS = TaskStatus.success(ID);

  @Mock KubernetesPeonClient kubernetesClient;
  @Mock TaskLogs taskLogs;

  private ObjectMapper mapper;
  private K8sTaskId k8sTaskId;
  private Task task;

  @Before
  public void setup()
  {
    task = NoopTask.create(ID, 0);

    k8sTaskId = new K8sTaskId(task);

    mapper = new TestUtils().getTestObjectMapper();
  }

  @Test
  public void test_run()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        return TaskStatus.success(ID);
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.run(job, 0L, 0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isSuccess());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_run_whenCalledMultipleTimes_raisesIllegalStateException()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        return TaskStatus.success(ID);
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    peonLifecycle.run(job, 0L, 0L);

    Assert.assertThrows(
        "Task [id] failed to run: invalid state transition [NONE]->[PENDING]",
        IllegalStateException.class,
        () -> peonLifecycle.run(job, 0L, 0L)
    );

    verifyAll();

    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_run_whenExceptionRaised_setsRunnerTaskStateToNone()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected synchronized TaskStatus join(long timeout)
      {
        throw new IllegalStateException();
      }
    };

    Job job = new JobBuilder().withNewMetadata().withName(ID).endMetadata().build();

    EasyMock.expect(kubernetesClient.launchPeonJobAndWaitForStart(
        EasyMock.eq(job),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    Assert.assertThrows(
        Exception.class,
        () -> peonLifecycle.run(job, 0L, 0L)
    );

    verifyAll();

    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_withoutJob_raisesISE()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.absent());
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    Assert.assertThrows(
        "Failed to join job [id], does not exist",
        ISE.class,
        () -> peonLifecycle.join(0L)
    );

    verifyAll();

    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_withActiveJob() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.of(job));
    EasyMock.expect(kubernetesClient.waitForPeonJobCompletion(
        EasyMock.eq(k8sTaskId),
        EasyMock.anyLong(),
        EasyMock.eq(TimeUnit.MILLISECONDS)
    )).andReturn(null);
    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(Optional.of(
        IOUtils.toInputStream("", StandardCharsets.UTF_8)
    ));
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(Optional.of(
        IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8)
    ));
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertEquals(SUCCESS, taskStatus);
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_whenCalledMultipleTimes_raisesIllegalStateException() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.of(job));
    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(
        Optional.of(IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8))
    );
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    Assert.assertThrows(
        "Task [id] failed to run: invalid state transition [NONE]->[PENDING]",
        IllegalStateException.class,
        () -> peonLifecycle.join(0L)
    );

    verifyAll();

    Assert.assertEquals(SUCCESS, taskStatus);
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_withoutTaskStatus_returnsFailedTaskStatus() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.of(job));
    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(Optional.absent());
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals("Task [id] failed, task status not found", taskStatus.getErrorMsg());
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_whenIOExceptionThrownWhileStreamingTaskStatus_returnsFailedTaskStatus() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.of(job));
    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andThrow(new IOException());
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall();
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(ID, taskStatus.getId());
    Assert.assertEquals(
        "Task [id] failed, caught exception when loading task status: null",
        taskStatus.getErrorMsg()
    );
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_join_whenIOExceptionThrownWhileStreamingTaskLogs_isIgnored() throws IOException
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonJob(k8sTaskId)).andReturn(Optional.of(job));
    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );
    EasyMock.expect(taskLogs.streamTaskStatus(ID)).andReturn(
        Optional.of(IOUtils.toInputStream(mapper.writeValueAsString(SUCCESS), StandardCharsets.UTF_8))
    );
    taskLogs.pushTaskLog(EasyMock.eq(ID), EasyMock.anyObject(File.class));
    EasyMock.expectLastCall().andThrow(new IOException());
    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    Assert.assertEquals(RunnerTaskState.WAITING, peonLifecycle.getRunnerTaskState());

    replayAll();

    TaskStatus taskStatus = peonLifecycle.join(0L);

    verifyAll();

    Assert.assertEquals(SUCCESS, taskStatus);
    Assert.assertEquals(RunnerTaskState.NONE, peonLifecycle.getRunnerTaskState());
  }

  @Test
  public void test_shutdown_withWaitingTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };

    peonLifecycle.shutdown();
  }

  @Test
  public void test_shutdown_withPendingTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.PENDING;
      }
    };

    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    replayAll();

    peonLifecycle.shutdown();

    verifyAll();
  }

  @Test
  public void test_shutdown_withRunningTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    EasyMock.expect(kubernetesClient.deletePeonJob(k8sTaskId)).andReturn(true);

    replayAll();

    peonLifecycle.shutdown();

    verifyAll();
  }

  @Test
  public void test_shutdown_withNoneTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.NONE;
      }
    };

    peonLifecycle.shutdown();
  }

  @Test
  public void test_streamLogs_withWaitingTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_streamLogs_withPendingTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.PENDING;
      }
    };

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_streamLogs_withRunningTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    EasyMock.expect(kubernetesClient.getPeonLogs(k8sTaskId)).andReturn(
        Optional.of(IOUtils.toInputStream("", StandardCharsets.UTF_8))
    );

    replayAll();

    peonLifecycle.streamLogs();

    verifyAll();
  }

  @Test
  public void test_streamLogs_withNoneTaskState()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.NONE;
      }
    };

    peonLifecycle.streamLogs();
  }

  @Test
  public void test_getTaskLocation_withWaitingTaskState_returnsUnknown()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
  }

  @Test
  public void test_getTaskLocation_withPendingTaskState_returnsUnknown()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.WAITING;
      }
    };

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withoutPeonPod_returnsUnknown()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId)).andReturn(Optional.absent());

    replayAll();

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithoutStatus_returnsUnknown()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId)).andReturn(Optional.of(pod));

    replayAll();

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithStatus_returnsLocation()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId)).andReturn(Optional.of(pod));

    replayAll();

    TaskLocation location = peonLifecycle.getTaskLocation();

    Assert.assertEquals("ip", location.getHost());
    Assert.assertEquals(8100, location.getPort());
    Assert.assertEquals(-1, location.getTlsPort());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withRunningTaskState_withPeonPodWithStatusWithTLSAnnotation_returnsLocation()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.RUNNING;
      }
    };

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(ID)
        .addToAnnotations("tls.enabled", "true")
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    EasyMock.expect(kubernetesClient.getPeonPod(k8sTaskId)).andReturn(Optional.of(pod));

    replayAll();

    TaskLocation location = peonLifecycle.getTaskLocation();

    Assert.assertEquals("ip", location.getHost());
    Assert.assertEquals(-1, location.getPort());
    Assert.assertEquals(8091, location.getTlsPort());

    verifyAll();
  }

  @Test
  public void test_getTaskLocation_withNoneTaskState_returnsUnknown()
  {
    KubernetesPeonLifecycle peonLifecycle = new KubernetesPeonLifecycle(task, kubernetesClient, taskLogs, mapper) {
      @Override
      protected RunnerTaskState getRunnerTaskState()
      {
        return RunnerTaskState.NONE;
      }
    };

    Assert.assertEquals(TaskLocation.unknown(), peonLifecycle.getTaskLocation());
  }
}
