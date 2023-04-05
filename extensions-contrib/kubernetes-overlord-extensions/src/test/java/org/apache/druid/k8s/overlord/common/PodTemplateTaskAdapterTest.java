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

package org.apache.druid.k8s.overlord.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

public class PodTemplateTaskAdapterTest
{
  @TempDir private Path tempDir;
  private KubernetesTaskRunnerConfig taskRunnerConfig;
  private PodTemplate podTemplateSpec;
  private TaskConfig taskConfig;
  private DruidNode node;
  private ObjectMapper mapper;

  @BeforeEach
  public void setup()
  {
    taskRunnerConfig = new KubernetesTaskRunnerConfig();
    taskConfig = new TaskConfig(
        "/tmp",
        null,
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
        false,
        ImmutableList.of("/tmp")
    );
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
    Assert.assertThrows(
        "Pod template task adapter requires a base pod template to be specified",
        IAE.class,
        () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        new Properties()
    ));
  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites_withEmptyFile_raisesISE() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("empty.yaml"));

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    Assert.assertThrows(
        "Pod template task adapter requires a base pod template to be specified",
        ISE.class,
        () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props
    ));
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
        props
    );

    Task task = new NoopTask(
        "id",
        "id",
        "datasource",
        0,
        0,
        null,
        null,
        null
    );
    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);

    Assertions.assertEquals(expected, actual);
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
        props
    );

    Task task = new NoopTask(
        "id",
        "id",
        "datasource",
        0,
        0,
        null,
        null,
        null
    );

    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJobTlsEnabled.yaml", Job.class);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_fromTask_withNoopPodTemplateInRuntimeProperties_withEmptyFile_raisesISE() throws IOException
  {
    Path baseTemplatePath = Files.createFile(tempDir.resolve("base.yaml"));
    Path noopTemplatePath = Files.createFile(tempDir.resolve("noop.yaml"));
    mapper.writeValue(baseTemplatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", baseTemplatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.noop", noopTemplatePath.toString());

    Assert.assertThrows(ISE.class, () -> new PodTemplateTaskAdapter(
        taskRunnerConfig,
        taskConfig,
        node,
        mapper,
        props
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
        props
    );

    Task task = new NoopTask(
        "id",
        "id",
        "datasource",
        0,
        0,
        null,
        null,
        null
    );

    Job actual = adapter.fromTask(task);
    Job expected = K8sTestUtils.fileToResource("expectedNoopJob.yaml", Job.class);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void test_fromTask_withoutAnnotations_throwsIOE() throws IOException
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
        props
    );

    Pod pod = K8sTestUtils.fileToResource("basePodWithoutAnnotations.yaml", Pod.class);

    Assert.assertThrows(IOE.class, () -> adapter.toTask(pod));
  }

  @Test
  public void test_fromTask_withoutTaskAnnotation_throwsIOE() throws IOException
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
        props
    );

    Pod basePod = K8sTestUtils.fileToResource("basePodWithoutAnnotations.yaml", Pod.class);

    Pod pod = new PodBuilder(basePod)
        .editMetadata()
        .addToAnnotations(Collections.emptyMap())
        .endMetadata()
        .build();

    Assert.assertThrows(IOE.class, () -> adapter.toTask(pod));
  }

  @Test
  public void test_fromTask() throws IOException
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
        props
    );

    Pod pod = K8sTestUtils.fileToResource("basePod.yaml", Pod.class);

    Task actual = adapter.toTask(pod);
    Task expected = NoopTask.create("id", 1);

    Assertions.assertEquals(expected, actual);
  }
}
