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

package org.apache.druid.k8s.overlord.taskadapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.common.Base64Compression;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PodTemplateTaskAdapterTest
{
  private KubernetesTaskRunnerConfig taskRunnerConfig;
  private PodTemplate podTemplateSpec;
  private TaskConfig taskConfig;
  private DruidNode node;
  private ObjectMapper mapper;
  private TaskLogs taskLogs;

  @BeforeEach
  public void setup()
  {
    taskRunnerConfig = KubernetesTaskRunnerConfig.builder().build();
    taskConfig = new TaskConfigBuilder().setBaseDir("/tmp").build();
    node = new DruidNode(
        "test",
        "",
        false,
        0,
        1,
        true,
        false
    );
    mapper = new TestUtils().getTestObjectMapper();
    podTemplateSpec = K8sTestUtils.fileToResource("basePodTemplate.yaml", PodTemplate.class);

    taskLogs = EasyMock.createMock(TaskLogs.class);
  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites_andTlsEnabled() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        new DruidNode(
            "test",
            "",
            false,
            0,
            1,
            false,
            true
        ),
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJobTlsEnabledBase.yaml", Job.class);

    assertJobSpecsEqual(actual, expected);
  }

  @Test
  public void test_fromTask_withNoopPodTemplateInRuntimeProperites_dontSetTaskJSON() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = new NoopTask(
        "id",
        "id",
        "datasource",
        0,
        0,
        ImmutableMap.of("context", RandomStringUtils.randomAlphanumeric((int) DruidK8sConstants.MAX_ENV_VARIABLE_KBS * 20))
    );

    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJobNoTaskJson.yaml", Job.class);

    Assertions.assertEquals(actual, expected);
  }

  @Test
  public void test_fromTask_withoutAnnotations_throwsDruidException()
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);


    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Job job = K8sTestUtils.fileToResource("baseJobWithoutAnnotations.yaml", Job.class);


    Assert.assertThrows(DruidException.class, () -> adapter.toTask(job));
  }

  @Test
  public void test_getTaskId()
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .addToAnnotations(DruidK8sConstants.TASK_ID, "ID")
        .endMetadata().endTemplate().endSpec().build();

    assertEquals(new K8sTaskId("ID"), adapter.getTaskId(job));
  }

  @Test
  public void test_getTaskId_noAnnotations()
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);
    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .endMetadata().endTemplate().endSpec()
        .editMetadata().withName("job").endMetadata().build();

    Assert.assertThrows(DruidException.class, () -> adapter.getTaskId(job));
  }

  @Test
  public void test_getTaskId_missingTaskIdAnnotation()
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);
    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .addToAnnotations(DruidK8sConstants.TASK_GROUP_ID, "ID")
        .endMetadata().endTemplate().endSpec()
        .editMetadata().withName("job").endMetadata().build();

    Assert.assertThrows(DruidException.class, () -> adapter.getTaskId(job));
  }

  @Test
  public void test_toTask_withoutTaskAnnotation_throwsIOE()
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Job baseJob = K8sTestUtils.fileToResource("baseJobWithoutAnnotations.yaml", Job.class);

    Job job = new JobBuilder(baseJob)
        .editSpec()
        .editTemplate()
        .editMetadata()
        .addToAnnotations(Collections.emptyMap())
        .endMetadata()
        .endTemplate()
        .endSpec()
        .build();
    Assert.assertThrows(DruidException.class, () -> adapter.toTask(job));
  }

  @Test
  public void test_toTask() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Job job = K8sTestUtils.fileToResource("baseJob.yaml", Job.class);
    Task actual = adapter.toTask(job);
    Task expected = K8sTestUtils.createTask("id", 1);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_toTask_useTaskPayloadManager() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    Task expected = new NoopTask("id", null, "datasource", 0, 0, ImmutableMap.of());
    TaskLogs mockTestLogs = Mockito.mock(TaskLogs.class);
    Mockito.when(mockTestLogs.streamTaskPayload("id")).thenReturn(Optional.of(
        new ByteArrayInputStream(mapper.writeValueAsString(expected).getBytes(Charset.defaultCharset()))
    ));

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        mockTestLogs,
        podTemplateSelector
    );

    Job job = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);
    Task actual = adapter.toTask(job);
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_fromTask_withRealIds() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = new NoopTask(
        "api-issued_kill_wikipedia3_omjobnbc_1000-01-01T00:00:00.000Z_2023-05-14T00:00:00.000Z_2023-05-15T17:03:01.220Z",
        "api-issued_kill_wikipedia3_omjobnbc_1000-01-01T00:00:00.000Z_2023-05-14T00:00:00.000Z_2023-05-15T17:03:01.220Z",
        "data_source",
        0,
        0,
        null
    );

    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJobLongIds.yaml", Job.class);

    assertJobSpecsEqual(actual, expected);
  }

  @Test
  public void test_fromTask_noTemplate()
  {

    PodTemplateSelector podTemplateSelector = EasyMock.createMock(PodTemplateSelector.class);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = new NoopTask(
        "id",
        "groupId",
        "data_source",
        0,
        0,
        null
    );
    EasyMock.expect(podTemplateSelector.getPodTemplateForTask(EasyMock.anyObject()))
        .andReturn(Optional.absent());

    Assert.assertThrows(DruidException.class, () -> adapter.fromTask(task));
  }

  @Test
  public void test_fromTask_null()
  {

    PodTemplateSelector podTemplateSelector = EasyMock.createMock(PodTemplateSelector.class);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = new NoopTask(
        "id",
        "groupId",
        "data_source",
        0,
        0,
        null
    );
    EasyMock.expect(podTemplateSelector.getPodTemplateForTask(EasyMock.anyObject()))
        .andReturn(null);

    Assert.assertThrows(DruidException.class, () -> adapter.fromTask(task));
  }

  @Test
  public void test_fromTask_taskSupportsQueries() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = EasyMock.mock(Task.class);
    EasyMock.expect(task.supportsQueries()).andReturn(true);
    EasyMock.expect(task.getType()).andReturn("queryable").anyTimes();
    EasyMock.expect(task.getId()).andReturn("id").anyTimes();
    EasyMock.expect(task.getGroupId()).andReturn("groupid").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("datasource").anyTimes();
    EasyMock.expect(task.getBroadcastDatasourceLoadingSpec()).andReturn(BroadcastDatasourceLoadingSpec.ALL).anyTimes();

    EasyMock.replay(task);
    Job actual = adapter.fromTask(task);
    EasyMock.verify(task);

    Assertions.assertEquals("true", actual.getSpec().getTemplate()
        .getSpec().getContainers()
        .get(0).getEnv().stream()
        .filter(env -> env.getName().equals(DruidK8sConstants.LOAD_BROADCAST_SEGMENTS_ENV))
        .collect(Collectors.toList()).get(0).getValue());
  }

  @Test
  public void test_fromTask_withBroadcastDatasourceLoadingModeAll() throws IOException
  {
    TestPodTemplateSelector podTemplateSelector = new TestPodTemplateSelector(podTemplateSpec);


    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        taskLogs,
        podTemplateSelector
    );

    Task task = EasyMock.mock(Task.class);
    EasyMock.expect(task.supportsQueries()).andReturn(true);
    EasyMock.expect(task.getType()).andReturn("queryable").anyTimes();
    EasyMock.expect(task.getId()).andReturn("id").anyTimes();
    EasyMock.expect(task.getGroupId()).andReturn("groupid").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("datasource").anyTimes();
    EasyMock.expect(task.getBroadcastDatasourceLoadingSpec()).andReturn(BroadcastDatasourceLoadingSpec.ALL).anyTimes();

    EasyMock.replay(task);
    Job actual = adapter.fromTask(task);
    EasyMock.verify(task);

    Assertions.assertEquals(BroadcastDatasourceLoadingSpec.Mode.ALL.toString(), actual.getSpec().getTemplate()
                                          .getSpec().getContainers()
                                          .get(0).getEnv().stream()
                                          .filter(env -> env.getName().equals(DruidK8sConstants.LOAD_BROADCAST_DATASOURCE_MODE_ENV))
                                          .collect(Collectors.toList()).get(0).getValue());
  }

  private void assertJobSpecsEqual(Job actual, Job expected) throws IOException
  {
    Map<String, String> actualAnnotations = actual.getSpec().getTemplate().getMetadata().getAnnotations();
    String actualTaskAnnotation = actualAnnotations.get(DruidK8sConstants.TASK);
    actualAnnotations.remove(DruidK8sConstants.TASK);
    actual.getSpec().getTemplate().getMetadata().setAnnotations(actualAnnotations);

    Map<String, String> expectedAnnotations = expected.getSpec().getTemplate().getMetadata().getAnnotations();
    String expectedTaskAnnotation = expectedAnnotations.get(DruidK8sConstants.TASK);
    expectedAnnotations.remove(DruidK8sConstants.TASK);
    expected.getSpec().getTemplate().getMetadata().setAnnotations(expectedAnnotations);

    Assertions.assertEquals(expected, actual);
    Assertions.assertEquals(
        Base64Compression.decompressBase64(actualTaskAnnotation),
        Base64Compression.decompressBase64(expectedTaskAnnotation)
    );
  }
}
