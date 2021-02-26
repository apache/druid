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

package org.apache.druid.k8s.middlemanager;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.middlemanager.common.DefaultK8sApiClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

public class K8sForkingTaskRunnerTest extends EasyMockSupport
{
  private ApiClient realK8sClient;
  private CoreV1Api coreV1Api;
  private GenericKubernetesApi<V1Pod, V1PodList> podClient;
  private TaskLogPusher taskLogPusher;
  private ObjectMapper jsonMapper;
  private PodLogs podLogs;
  private DruidNode node;
  private static final EmittingLogger LOGGER = new EmittingLogger(K8sForkingTaskRunnerTest.class);


  @Before
  public void setUp()
  {
    this.realK8sClient = EasyMock.mock(ApiClient.class);
    this.coreV1Api = EasyMock.mock(CoreV1Api.class);
    this.podClient = EasyMock.mock(GenericKubernetesApi.class);
    this.podLogs = EasyMock.mock(PodLogs.class);
    this.realK8sClient = EasyMock.mock(ApiClient.class);
    this.taskLogPusher = EasyMock.mock(TaskLogPusher.class);
    this.jsonMapper = new DefaultObjectMapper();
    this.node = new DruidNode("forkServiceName", "0.0.0.0", false, 8000, 9000, true, true);
  }

  @Test(timeout = 60_000L)
  public void testK8sForkingTaskRunnerRunAndFinished() throws Exception
  {
    DefaultK8sApiClient k8sApiClient = new DefaultK8sApiClient(realK8sClient, jsonMapper);
    k8sApiClient.setCoreV1Api(coreV1Api);
    k8sApiClient.setPodClient(podClient);
    k8sApiClient.setPodLogsClient(podLogs);
    k8sApiClient.setPodName("mm");
    k8sApiClient.setPodUID("abc");
    ForkingTaskRunnerConfig forkingTaskRunnerConfig = jsonMapper.convertValue(ImmutableMap.of(
            "javaOpts", "a \"\"b",
            "classpath", "/aaa"), ForkingTaskRunnerConfig.class);
    TaskConfig taskConfig = new TaskConfig("src/test/resources", "src/test/resources", null, null, null, false, null, null, null);
    WorkerConfig workerConfig = new WorkerConfig();
    Properties properties = new Properties();
    properties.putAll(ImmutableMap.of(
            "druid.extensions.loadList", "[\"org.apache.druid.java.util.metrics.JvmCpuMonitor\",\"org.apache.druid.java.util.metrics.JvmMonitor\",\"org.apache.druid.java.util.metrics.JvmThreadsMonitor\",\"org.apache.druid.java.util.metrics.SysMonitor\",\"org.apache.druid.server.metrics.EventReceiverFirehoseMonitor\"]",
            "druid.realtime.cache.unCacheable", "[]",
            "druid.indexer.fork.property.druid.processing.buffer.sizeBytes", "1024"));
    StartupLoggingConfig startupLoggingConfig = new StartupLoggingConfig();

    K8sForkingTaskRunner k8sForkingTaskRunner = new K8sForkingTaskRunner(
            forkingTaskRunnerConfig,
            taskConfig,
            workerConfig,
            properties,
            taskLogPusher,
            jsonMapper,
            node,
            startupLoggingConfig,
            k8sApiClient);

    Task task = new TestableNoopTask(
            "forktaskid",
            null,
            null,
            0,
            0,
            null,
            null,
            ImmutableMap.of("druid.indexer.runner.javaOpts", "abc",
                    "druid.indexer.fork.property.druid.processing.buffer.sizeBytes", "2048",
                    "druid.peon.pod.cpu", "1",
                    "druid.peon.pod.memory", "2G"));
    String taskID = task.getId();
    EasyMock.expect(coreV1Api.listNamespacedConfigMap(EasyMock.anyString(),
            EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(null)
            .anyTimes();

    V1ConfigMap configMap = new V1ConfigMapBuilder()
            .withNewMetadata()
            .withName(taskID)
            .withLabels(ImmutableMap.of("druid.ingest.task.id", taskID))
            .endMetadata()
            .withData(ImmutableMap.of("task.json", jsonMapper.writeValueAsString(task)))
            .build();

    EasyMock.expect(coreV1Api.createNamespacedConfigMap(EasyMock.anyString(), EasyMock.anyObject(V1ConfigMap.class), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(configMap)
            .anyTimes();

    V1Pod pod = new V1PodBuilder()
            .withNewMetadata()
            .withNamespace("default")
            .withName(taskID)
            .withLabels(ImmutableMap.of("druid.ingest.task.id", taskID))
            .endMetadata()
            .withNewSpec()
            .withNewSecurityContext()
            .withFsGroup(0L)
            .withRunAsGroup(0L)
            .withRunAsUser(0L)
            .endSecurityContext()
            .withNewRestartPolicy("Never")
            .endSpec()
            .withNewStatus()
            .addNewPodIP()
            .withNewIp("0.0.0.1")
            .endPodIP()
            .withNewPhase("Succeeded")
            .endStatus()
            .build();

    EasyMock.expect(coreV1Api.createNamespacedPod(EasyMock.anyString(), EasyMock.anyObject(V1Pod.class), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(pod)
            .anyTimes();
    V1PodList v1PodListEmpty = new V1PodList();
    V1PodList v1PodList = new V1PodList().addItemsItem(pod);
    EasyMock.expect(coreV1Api.listNamespacedPod(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(v1PodListEmpty)
            .once();
    EasyMock.expect(coreV1Api.listNamespacedPod(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(v1PodList)
            .once();
    EasyMock.expect(coreV1Api.deleteCollectionNamespacedConfigMap(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyInt(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(null)
            .anyTimes();
    EasyMock.replay(coreV1Api);


    KubernetesApiResponse<V1Pod> response = new KubernetesApiResponse<V1Pod>(pod);
    EasyMock.expect(podClient.get(EasyMock.anyString(), EasyMock.anyString()))
            .andReturn(response)
            .anyTimes();
    EasyMock.expect(podClient.delete(EasyMock.anyString(), EasyMock.anyString()))
            .andReturn(null)
            .anyTimes();
    EasyMock.replay(podClient);

    EasyMock.expect(podLogs.streamNamespacedPodLog(EasyMock.anyObject(V1Pod.class)))
            .andReturn(new FileInputStream(new File("src/test/resources/logExample.txt")))
            .anyTimes();
    EasyMock.replay(podLogs);

    ListenableFuture<TaskStatus> taskFuture = k8sForkingTaskRunner.run(task);

    // No exceptions will be threw.
    k8sForkingTaskRunner.getBlacklistedTaskSlotCount();
    k8sForkingTaskRunner.getIdleTaskSlotCount();
    k8sForkingTaskRunner.getLazyTaskSlotCount();
    k8sForkingTaskRunner.getPendingTasks();
    k8sForkingTaskRunner.getTotalTaskSlotCount();
    k8sForkingTaskRunner.getUsedTaskSlotCount();
    k8sForkingTaskRunner.getRunningTasks();
    k8sForkingTaskRunner.getScalingStats();
    k8sForkingTaskRunner.getKnownTasks();

    while (!taskFuture.isDone()) {
      Thread.sleep(1000);
    }
    TaskStatus taskStatus = taskFuture.get();
    Assert.assertTrue(taskStatus.getStatusCode().isSuccess());
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
            @JsonProperty("context") Map<String, Object> context)
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
