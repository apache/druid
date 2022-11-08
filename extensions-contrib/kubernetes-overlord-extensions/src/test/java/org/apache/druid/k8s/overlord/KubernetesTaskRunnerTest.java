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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.DruidKubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.JobResponse;
import org.apache.druid.k8s.overlord.common.K8sTaskAdapter;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.PeonCommandContext;
import org.apache.druid.k8s.overlord.common.PeonPhase;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.joda.time.Period;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KubernetesTaskRunnerTest
{

  private TaskQueueConfig taskQueueConfig;
  private StartupLoggingConfig startupLoggingConfig;
  private ObjectMapper jsonMapper;
  private KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private TaskConfig taskConfig;
  private TaskLogPusher taskLogPusher;
  private DruidNode node;

  public KubernetesTaskRunnerTest()
  {
    TestUtils utils = new TestUtils();
    jsonMapper = utils.getTestObjectMapper();
    for (Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTask.IndexTuningConfig.class, "index")
    );
  }

  @BeforeEach
  void setUp()
  {
    taskConfig = new TaskConfig(
        "src/test/resources",
        "src/test/resources",
        null,
        null,
        null,
        false,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        false
    );
    kubernetesTaskRunnerConfig = new KubernetesTaskRunnerConfig();
    kubernetesTaskRunnerConfig.namespace = "test";
    kubernetesTaskRunnerConfig.javaOptsArray = Collections.singletonList("-Xmx2g");
    taskQueueConfig = new TaskQueueConfig(1, Period.millis(1), Period.millis(1), Period.millis(1));
    startupLoggingConfig = new StartupLoggingConfig();
    taskLogPusher = mock(TaskLogPusher.class);
    node = mock(DruidNode.class);
    when(node.isEnableTlsPort()).thenReturn(false);
  }

  @Test
  void testAlreadyRunningJobInK8s() throws Exception
  {
    Task task = makeTask();
    K8sTaskId k8sTaskId = new K8sTaskId(task.getId());

    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("jobName");

    Job job = mock(Job.class);
    JobStatus status = mock(JobStatus.class);
    when(job.getMetadata()).thenReturn(metadata);
    when(status.getActive()).thenReturn(1).thenReturn(null);
    when(job.getStatus()).thenReturn(status);

    Pod peonPod = mock(Pod.class);
    when(peonPod.getMetadata()).thenReturn(metadata);
    PodStatus podStatus = mock(PodStatus.class);
    when(podStatus.getPodIP()).thenReturn("SomeIP");
    when(peonPod.getStatus()).thenReturn(podStatus);

    K8sTaskAdapter adapter = mock(K8sTaskAdapter.class);
    when(adapter.fromTask(eq(task), any())).thenReturn(job);

    DruidKubernetesPeonClient peonClient = mock(DruidKubernetesPeonClient.class);

    when(peonClient.jobExists(eq(k8sTaskId))).thenReturn(Optional.of(job));
    when(peonClient.getMainJobPod(eq(k8sTaskId))).thenReturn(peonPod);
    when(peonClient.waitForJobCompletion(eq(k8sTaskId), anyLong(), isA(TimeUnit.class))).thenReturn(new JobResponse(
        job,
        PeonPhase.SUCCEEDED
    ));
    when(peonClient.cleanUpJob(eq(k8sTaskId))).thenReturn(true);

    KubernetesTaskRunner taskRunner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        adapter,
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        peonClient,
        node
    );
    KubernetesTaskRunner spyRunner = spy(taskRunner);

    ListenableFuture<TaskStatus> future = spyRunner.run(task);
    future.get();
    // we should never launch the job here, one exists
    verify(peonClient, never()).launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class));
    verify(peonClient, times(1)).cleanUpJob(eq(k8sTaskId));
    verify(spyRunner, times(1)).updateStatus(eq(task), eq(TaskStatus.success(task.getId())));
  }

  @Test
  void testJobNeedsToLaunchInK8s() throws Exception
  {
    Task task = makeTask();
    K8sTaskId k8sTaskId = new K8sTaskId(task.getId());

    Job job = mock(Job.class);
    ObjectMeta jobMetadata = mock(ObjectMeta.class);
    when(jobMetadata.getName()).thenReturn(k8sTaskId.getK8sTaskId());
    JobStatus status = mock(JobStatus.class);
    when(status.getActive()).thenReturn(1).thenReturn(null);
    when(job.getStatus()).thenReturn(status);
    when(job.getMetadata()).thenReturn(jobMetadata);

    Pod peonPod = mock(Pod.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("peonPodName");
    when(peonPod.getMetadata()).thenReturn(metadata);
    PodStatus podStatus = mock(PodStatus.class);
    when(podStatus.getPodIP()).thenReturn("SomeIP");
    when(peonPod.getStatus()).thenReturn(podStatus);

    K8sTaskAdapter adapter = mock(K8sTaskAdapter.class);
    when(adapter.fromTask(eq(task), isA(PeonCommandContext.class))).thenReturn(job);

    DruidKubernetesPeonClient peonClient = mock(DruidKubernetesPeonClient.class);

    when(peonClient.jobExists(eq(k8sTaskId))).thenReturn(Optional.fromNullable(null));
    when(peonClient.launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class))).thenReturn(peonPod);
    when(peonClient.getMainJobPod(eq(k8sTaskId))).thenReturn(peonPod);
    when(peonClient.waitForJobCompletion(eq(k8sTaskId), anyLong(), isA(TimeUnit.class))).thenReturn(new JobResponse(
        job,
        PeonPhase.SUCCEEDED
    ));
    when(peonClient.cleanUpJob(eq(k8sTaskId))).thenReturn(true);

    KubernetesTaskRunner taskRunner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        adapter,
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        peonClient,
        node
    );
    KubernetesTaskRunner spyRunner = spy(taskRunner);


    ListenableFuture<TaskStatus> future = spyRunner.run(task);
    future.get();
    // we should never launch the job here, one exists
    verify(peonClient, times(1)).launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class));
    verify(peonClient, times(1)).cleanUpJob(eq(k8sTaskId));
    TaskLocation expectedTaskLocation = TaskLocation.create(
        peonPod.getStatus().getPodIP(),
        DruidK8sConstants.PORT,
        DruidK8sConstants.TLS_PORT,
        node.isEnableTlsPort()
    );
    verify(spyRunner, times(1)).updateStatus(eq(task), eq(TaskStatus.success(task.getId(), expectedTaskLocation)));
  }

  @Test
  void testTheK8sRestartState() throws Exception
  {
    // we have a shutdown, now we start-up the overlord, it should catch and deal with all the peon k8s tasks in-flight
    Task task = makeTask();
    K8sTaskId k8sTaskId = new K8sTaskId(task.getId());

    Job job = mock(Job.class);
    ObjectMeta jobMetadata = mock(ObjectMeta.class);
    when(jobMetadata.getName()).thenReturn(k8sTaskId.getK8sTaskId());
    JobStatus status = mock(JobStatus.class);
    when(status.getActive()).thenReturn(1).thenReturn(null);
    when(job.getStatus()).thenReturn(status);
    when(job.getMetadata()).thenReturn(jobMetadata);

    Pod peonPod = mock(Pod.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("peonPodName");
    when(metadata.getCreationTimestamp()).thenReturn(DateTimes.nowUtc().toString());
    when(peonPod.getMetadata()).thenReturn(metadata);
    PodStatus podStatus = mock(PodStatus.class);
    when(podStatus.getPodIP()).thenReturn("SomeIP");
    when(peonPod.getStatus()).thenReturn(podStatus);

    K8sTaskAdapter adapter = mock(K8sTaskAdapter.class);
    when(adapter.fromTask(eq(task), isA(PeonCommandContext.class))).thenReturn(job);
    when(adapter.toTask(eq(peonPod))).thenReturn(task);

    DruidKubernetesPeonClient peonClient = mock(DruidKubernetesPeonClient.class);

    when(peonClient.listPeonPods()).thenReturn(Collections.singletonList(peonPod));
    when(peonClient.jobExists(eq(k8sTaskId))).thenReturn(Optional.of(job));
    when(peonClient.launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class))).thenReturn(peonPod);
    when(peonClient.getMainJobPod(eq(k8sTaskId))).thenReturn(peonPod);
    when(peonClient.waitForJobCompletion(eq(k8sTaskId), anyLong(), isA(TimeUnit.class))).thenReturn(new JobResponse(
        job,
        PeonPhase.SUCCEEDED
    ));
    when(peonClient.cleanUpJob(eq(k8sTaskId))).thenReturn(true);

    KubernetesTaskRunner taskRunner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        adapter,
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        peonClient,
        node
    );
    KubernetesTaskRunner spyRunner = spy(taskRunner);
    Collection<? extends TaskRunnerWorkItem> workItems = spyRunner.getKnownTasks();
    assertEquals(1, workItems.size());
    TaskRunnerWorkItem item = Iterables.getOnlyElement(workItems);
    item.getResult().get();

    // we should never launch the job here, one exists
    verify(peonClient, never()).launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class));
    verify(peonClient, times(1)).cleanUpJob(eq(k8sTaskId));
    verify(spyRunner, times(1)).updateStatus(eq(task), eq(TaskStatus.success(task.getId())));
    verify(spyRunner, times(1)).run(eq(task));
  }

  @Test
  void testTheK8sRestartStateAndHandleJobsThatAlreadyCompletedWhileDown() throws Exception
  {
    // we have a shutdown, now we start-up the overlord, it should monitor k8s jobs that finished.
    Task task = makeTask();
    K8sTaskId k8sTaskId = new K8sTaskId(task.getId());

    Job job = mock(Job.class);
    ObjectMeta jobMetadata = mock(ObjectMeta.class);
    when(jobMetadata.getName()).thenReturn(k8sTaskId.getK8sTaskId());
    JobStatus status = mock(JobStatus.class);
    when(status.getActive()).thenReturn(null);
    when(job.getStatus()).thenReturn(status);
    when(job.getStatus().getSucceeded()).thenReturn(1);
    when(job.getMetadata()).thenReturn(jobMetadata);

    Pod peonPod = mock(Pod.class);
    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getName()).thenReturn("peonPodName");
    when(metadata.getCreationTimestamp()).thenReturn(DateTimes.nowUtc().toString());
    when(peonPod.getMetadata()).thenReturn(metadata);
    PodStatus podStatus = mock(PodStatus.class);
    when(podStatus.getPodIP()).thenReturn("SomeIP");
    when(peonPod.getStatus()).thenReturn(podStatus);

    K8sTaskAdapter adapter = mock(K8sTaskAdapter.class);
    when(adapter.fromTask(eq(task), isA(PeonCommandContext.class))).thenReturn(job);
    when(adapter.toTask(eq(peonPod))).thenReturn(task);

    DruidKubernetesPeonClient peonClient = mock(DruidKubernetesPeonClient.class);

    when(peonClient.listPeonPods()).thenReturn(Collections.singletonList(peonPod));
    when(peonClient.jobExists(eq(k8sTaskId))).thenReturn(Optional.of(job));
    when(peonClient.launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class))).thenReturn(peonPod);
    when(peonClient.getMainJobPod(eq(k8sTaskId))).thenReturn(peonPod);
    when(peonClient.waitForJobCompletion(eq(k8sTaskId), anyLong(), isA(TimeUnit.class))).thenReturn(new JobResponse(
        job,
        PeonPhase.SUCCEEDED
    ));
    when(peonClient.cleanUpJob(eq(k8sTaskId))).thenReturn(true);

    KubernetesTaskRunner taskRunner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        adapter,
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        peonClient,
        node
    );
    KubernetesTaskRunner spyRunner = spy(taskRunner);
    Collection<? extends TaskRunnerWorkItem> workItems = spyRunner.getKnownTasks();
    assertEquals(1, workItems.size());
    TaskRunnerWorkItem item = Iterables.getOnlyElement(workItems);
    item.getResult().get();

    // we should never launch the job here, one exists
    verify(peonClient, never()).launchJobAndWaitForStart(isA(Job.class), anyLong(), isA(TimeUnit.class));
    verify(peonClient, times(1)).cleanUpJob(eq(k8sTaskId));
    TaskLocation expectedTaskLocation = TaskLocation.create(
        peonPod.getStatus().getPodIP(),
        DruidK8sConstants.PORT,
        DruidK8sConstants.TLS_PORT
    );
    // don't need to update the location, the one in the db was correct when it launched,
    verify(spyRunner, never()).updateLocation(eq(task), eq(expectedTaskLocation));
    // the state is still running, as it was before the overlord went down.
    verify(spyRunner, never()).updateStatus(eq(task), eq(TaskStatus.running(task.getId())));
    verify(spyRunner, times(1)).updateStatus(eq(task), eq(TaskStatus.success(task.getId(), expectedTaskLocation)));
    verify(spyRunner, times(1)).run(eq(task));
  }

  @Test
  void testMakingCodeCoverageHappy()
  {
    // have to test multiple branches of code for code-coverage, avoiding doing a lot of repetetive setup.
    DruidKubernetesPeonClient peonClient = mock(DruidKubernetesPeonClient.class);
    Pod pod = mock(Pod.class);
    PodStatus status = mock(PodStatus.class);
    when(status.getPhase()).thenReturn(PeonPhase.PENDING.getPhase()).thenReturn(PeonPhase.FAILED.getPhase());
    when(pod.getStatus()).thenReturn(status);
    when(peonClient.getMainJobPod(any())).thenReturn(null).thenReturn(pod);

    KubernetesTaskRunner taskRunner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        mock(K8sTaskAdapter.class),
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        peonClient,
        node
    );

    RunnerTaskState state = taskRunner.getRunnerTaskState("foo");
    assertNull(state);
    assertEquals(RunnerTaskState.PENDING, taskRunner.getRunnerTaskState("bar"));
    assertEquals(RunnerTaskState.WAITING, taskRunner.getRunnerTaskState("baz"));

    assertThrows(ISE.class, () -> {
      taskRunner.monitorJob(null, new K8sTaskId("foo"));
    });
  }

  @Test
  void testMaxQueueSizeIsEnforced()
  {
    TaskQueueConfig taskQueueConfig = new TaskQueueConfig(
        Integer.MAX_VALUE,
        Period.millis(1),
        Period.millis(1),
        Period.millis(1)
    );
    assertThrows(IllegalArgumentException.class, () -> new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        mock(K8sTaskAdapter.class),
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        mock(DruidKubernetesPeonClient.class),
        node
    ));
  }

  @Test
  void testWorkItemGetLocation()
  {
    KubernetesPeonClient client = mock(KubernetesPeonClient.class);
    Pod pod = mock(Pod.class);
    PodStatus status = mock(PodStatus.class);
    when(status.getPodIP()).thenReturn(null).thenReturn("tweak");
    when(pod.getStatus()).thenReturn(status);

    ObjectMeta metadata = mock(ObjectMeta.class);
    when(metadata.getAnnotations()).thenReturn(ImmutableMap.of(DruidK8sConstants.TLS_ENABLED, "false"));

    when(pod.getMetadata()).thenReturn(metadata);
    when(client.getMainJobPod(any())).thenReturn(pod);

    Task task = mock(Task.class);
    when(task.getId()).thenReturn("butters");
    KubernetesTaskRunner.K8sWorkItem k8sWorkItem = new KubernetesTaskRunner.K8sWorkItem(client, task, null);
    TaskLocation location = k8sWorkItem.getLocation();
    assertEquals(TaskLocation.unknown(), location);

    TaskLocation realLocation = k8sWorkItem.getLocation();
    assertEquals(TaskLocation.create("tweak", DruidK8sConstants.PORT, DruidK8sConstants.TLS_PORT, false), realLocation);
  }

  private Task makeTask()
  {
    return new TestableNoopTask(
        "k8sTaskId",
        null,
        null,
        0,
        0,
        null,
        null,
        ImmutableMap.of("druid.indexer.runner.javaOpts", "abc",
                        "druid.indexer.fork.property.druid.processing.buffer.sizeBytes", "2048",
                        "druid.peon.pod.cpu", "1",
                        "druid.peon.pod.memory", "2G"
        )
    );
  }

  private static class TestableNoopTask extends NoopTask
  {
    TestableNoopTask(
        @JsonProperty("id") String id,
        @JsonProperty("groupId") String groupId,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("runTime") long runTime,
        @JsonProperty("isReadyTime") long isReadyTime,
        @JsonProperty("isReadyResult") String isReadyResult,
        @JsonProperty("firehose") FirehoseFactory firehoseFactory,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(id, groupId, dataSource, runTime, isReadyTime, isReadyResult, firehoseFactory, context);
    }

    @Override
    public String getNodeType()
    {
      return "ForkNodeType";
    }

    @Override
    public boolean supportsQueries()
    {
      return true;
    }
  }
}
