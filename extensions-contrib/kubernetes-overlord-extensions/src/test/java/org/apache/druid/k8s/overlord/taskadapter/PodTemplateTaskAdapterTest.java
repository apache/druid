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
import io.fabric8.kubernetes.api.model.PodTemplateBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.common.Base64Compression;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PodTemplateTaskAdapterTest
{
  @TempDir private Path tempDir;
  private KubernetesTaskRunnerConfig taskRunnerConfig;
  private PodTemplate podTemplateSpec;
  private TaskConfig taskConfig;
  private DruidNode node;
  private ObjectMapper mapper;
  @Mock private TaskLogs taskLogs;

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
  }

  @Test
  public void test_fromTask_withoutBasePodTemplateInRuntimeProperites_raisesIAE()
  {
    Exception exception = Assert.assertThrows(
        "No base prop should throw an IAE",
        IAE.class,
        () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        new Properties(),
        taskLogs
    ));
    Assert.assertEquals(exception.getMessage(), "Pod template task adapter requires a base pod template to be specified under druid.indexer.runner.k8s.podTemplate.base");
  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites_withEmptyFile_raisesIAE() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("empty.yaml"));

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    Exception exception = Assert.assertThrows(
        "Empty base pod template should throw a exception",
        IAE.class,
        () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    ));

    Assert.assertTrue(exception.getMessage().contains("Failed to load pod template file for"));

  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);

    assertJobSpecsEqual(actual, expected);
  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites_andTlsEnabled() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

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
        props,
        taskLogs
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJobTlsEnabled.yaml", Job.class);

    assertJobSpecsEqual(actual, expected);
  }

  @Test
  public void test_fromTask_withNoopPodTemplateInRuntimeProperties_withEmptyFile_raisesIAE() throws IOException
  {
    Path baseTemplatePath = Files.createFile(tempDir.resolve("base.yaml"));
    Path noopTemplatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(baseTemplatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", baseTemplatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.noop", noopTemplatePath.toString());

    Assert.assertThrows(IAE.class, () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    ));
  }

  @Test
  public void test_fromTask_withNoopPodTemplateInRuntimeProperites() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.noop", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);

    assertJobSpecsEqual(actual, expected);
  }

  @Test
  public void test_fromTask_withNoopPodTemplateInRuntimeProperites_dontSetTaskJSON() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.noop", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
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
  public void test_fromTask_withoutAnnotations_throwsDruidException() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );

    Job job = K8sTestUtils.fileToResource("baseJobWithoutAnnotations.yaml", Job.class);


    Assert.assertThrows(DruidException.class, () -> adapter.toTask(job));
  }

  @Test
  public void test_getTaskId() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .addToAnnotations(DruidK8sConstants.TASK_ID, "ID")
        .endMetadata().endTemplate().endSpec().build();

    assertEquals(new K8sTaskId("ID"), adapter.getTaskId(job));
  }

  @Test
  public void test_getTaskId_noAnnotations() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .endMetadata().endTemplate().endSpec()
        .editMetadata().withName("job").endMetadata().build();

    Assert.assertThrows(DruidException.class, () -> adapter.getTaskId(job));
  }

  @Test
  public void test_getTaskId_missingTaskIdAnnotation() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );
    Job job = new JobBuilder()
        .editSpec().editTemplate().editMetadata()
        .addToAnnotations(DruidK8sConstants.TASK_GROUP_ID, "ID")
        .endMetadata().endTemplate().endSpec()
        .editMetadata().withName("job").endMetadata().build();

    Assert.assertThrows(DruidException.class, () -> adapter.getTaskId(job));
  }

  @Test
  public void test_toTask_withoutTaskAnnotation_throwsIOE() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.put("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
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
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.put("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );

    Job job = K8sTestUtils.fileToResource("baseJob.yaml", Job.class);
    Task actual = adapter.toTask(job);
    Task expected = K8sTestUtils.createTask("id", 1);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_toTask_useTaskPayloadManager() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.put("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

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
        props,
        mockTestLogs
    );

    Job job = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);
    Task actual = adapter.toTask(job);
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_fromTask_withRealIds() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.noop", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
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
  public void test_fromTask_taskSupportsQueries() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.queryable", templatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props,
        taskLogs
    );

    Task task = EasyMock.mock(Task.class);
    EasyMock.expect(task.supportsQueries()).andReturn(true);
    EasyMock.expect(task.getType()).andReturn("queryable").anyTimes();
    EasyMock.expect(task.getId()).andReturn("id").anyTimes();
    EasyMock.expect(task.getGroupId()).andReturn("groupid").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("datasource").anyTimes();

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
  public void test_fromTask_withIndexKafkaPodTemplateInRuntimeProperites() throws IOException
  {
    Path baseTemplatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(baseTemplatePath.toFile(), podTemplateSpec);

    Path kafkaTemplatePath = Files.createFile(tempDir.resolve("kafka.yaml"));
    PodTemplate kafkaPodTemplate = new PodTemplateBuilder(podTemplateSpec)
          .editTemplate()
          .editSpec()
          .setNewVolumeLike(0, new VolumeBuilder().withName("volume").build())
          .endVolume()
          .endSpec()
          .endTemplate()
          .build();
    mapper.writeValue(kafkaTemplatePath.toFile(), kafkaPodTemplate);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", baseTemplatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.index_kafka", kafkaTemplatePath.toString());

    PodTemplateTaskAdapter adapter = new PodTemplateTaskAdapter(
          taskRunnerConfig,
          taskConfig,
          node,
          mapper,
          props,
          taskLogs
    );

    Task kafkaTask = new NoopTask("id", "id", "datasource", 0, 0, null) {
      @Override
      public String getType()
      {
        return "index_kafka";
      }
    };

    Task noopTask = new NoopTask("id", "id", "datasource", 0, 0, null);
    Job actual = adapter.fromTask(kafkaTask);
    Assert.assertEquals(1, actual.getSpec().getTemplate().getSpec().getVolumes().size(), 1);

    actual = adapter.fromTask(noopTask);
    Assert.assertEquals(0, actual.getSpec().getTemplate().getSpec().getVolumes().size(), 1);
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
