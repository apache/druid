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
import com.google.common.base.Supplier;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.PodTemplateBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.execution.DefaultKubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.Selector;
import org.apache.druid.k8s.overlord.execution.SelectorBasedPodTemplateSelectStrategy;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.internal.util.collections.Sets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

public class DynamicConfigPodTemplateSelectorTest
{
  @TempDir
  private Path tempDir;
  private ObjectMapper mapper;
  private PodTemplate podTemplateSpec;
  private Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef;

  @BeforeEach
  public void setup()
  {
    mapper = new TestUtils().getTestObjectMapper();
    podTemplateSpec = K8sTestUtils.fileToResource("basePodTemplate.yaml", PodTemplate.class);
    dynamicConfigRef = () -> new DefaultKubernetesTaskRunnerDynamicConfig(KubernetesTaskRunnerDynamicConfig.DEFAULT_STRATEGY);
  }

  @Test
  public void test_fromTask_withoutBasePodTemplateInRuntimeProperites_raisesIAE()
  {
    Exception exception = Assert.assertThrows(
        "No base prop should throw an IAE",
        IAE.class,
        () -> new DynamicConfigPodTemplateSelector(
        new Properties(),
        dynamicConfigRef
    ));
    Assertions.assertEquals(exception.getMessage(), "Pod template task adapter requires a base pod template to be specified under druid.indexer.runner.k8s.podTemplate.base");
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
        () -> new DynamicConfigPodTemplateSelector(
            props,
            dynamicConfigRef
    ));

    Assertions.assertTrue(exception.getMessage().contains("Failed to load pod template file for"));
  }

  @Test
  public void test_fromTask_withBasePodTemplateInRuntimeProperites() throws IOException
  {
    Path templatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(templatePath.toFile(), podTemplateSpec);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", templatePath.toString());

    DynamicConfigPodTemplateSelector adapter = new DynamicConfigPodTemplateSelector(
        props,
        dynamicConfigRef
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Optional<PodTemplateWithName> actual = adapter.getPodTemplateForTask(task);
    PodTemplate expected = K8sTestUtils.fileToResource("expectedNoopPodTemplate.yaml", PodTemplate.class);

    Assertions.assertTrue(actual.isPresent());
    Assertions.assertEquals("base", actual.get().getName());
    Assertions.assertEquals(expected, actual.get().getPodTemplate());
  }

  @Test
  public void test_fromTask_withIndexKafkaPodTemplateInRuntimeProperties() throws IOException
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

    DynamicConfigPodTemplateSelector selector = new DynamicConfigPodTemplateSelector(
        props,
        dynamicConfigRef
    );

    Task kafkaTask = new NoopTask("id", "id", "datasource", 0, 0, null) {
      @Override
      public String getType()
      {
        return "index_kafka";
      }
    };

    Task noopTask = new NoopTask("id", "id", "datasource", 0, 0, null);
    Optional<PodTemplateWithName> podTemplateWithName = selector.getPodTemplateForTask(kafkaTask);
    Assertions.assertTrue(podTemplateWithName.isPresent());
    Assertions.assertEquals(1, podTemplateWithName.get().getPodTemplate().getTemplate().getSpec().getVolumes().size(), 1);
    Assertions.assertEquals("index_kafka", podTemplateWithName.get().getName());


    podTemplateWithName = selector.getPodTemplateForTask(noopTask);
    Assertions.assertTrue(podTemplateWithName.isPresent());
    Assertions.assertEquals(0, podTemplateWithName.get().getPodTemplate().getTemplate().getSpec().getVolumes().size(), 1);
    Assertions.assertEquals("base", podTemplateWithName.get().getName());
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

    Assert.assertThrows(IAE.class, () -> new DynamicConfigPodTemplateSelector(
        props,
        dynamicConfigRef
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

    DynamicConfigPodTemplateSelector podTemplateSelector = new DynamicConfigPodTemplateSelector(
        props,
        dynamicConfigRef
    );

    Task task = new NoopTask("id", "id", "datasource", 0, 0, null);
    Optional<PodTemplateWithName> podTemplateWithName = podTemplateSelector.getPodTemplateForTask(task);
    Assertions.assertTrue(podTemplateWithName.isPresent());
    PodTemplate expected = K8sTestUtils.fileToResource("expectedNoopPodTemplate.yaml", PodTemplate.class);

    Assertions.assertTrue(podTemplateWithName.isPresent());
    Assertions.assertEquals("noop", podTemplateWithName.get().getName());
    Assertions.assertEquals(expected, podTemplateWithName.get().getPodTemplate());
  }

  @Test
  public void test_fromTask_matchPodTemplateBasedOnStrategy() throws IOException
  {
    String dataSource = "my_table";
    Path baseTemplatePath = Files.createFile(tempDir.resolve("base.yaml"));
    mapper.writeValue(baseTemplatePath.toFile(), podTemplateSpec);

    Path lowThroughputTemplatePath = Files.createFile(tempDir.resolve("low-throughput.yaml"));
    PodTemplate lowThroughputPodTemplate = new PodTemplateBuilder(podTemplateSpec)
        .editTemplate()
        .editSpec()
        .setNewVolumeLike(0, new VolumeBuilder().withName("volume").build())
        .endVolume()
        .endSpec()
        .endTemplate()
        .build();
    mapper.writeValue(lowThroughputTemplatePath.toFile(), lowThroughputPodTemplate);

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", baseTemplatePath.toString());
    props.setProperty("druid.indexer.runner.k8s.podTemplate.lowThroughput", lowThroughputTemplatePath.toString());
    dynamicConfigRef = () -> new DefaultKubernetesTaskRunnerDynamicConfig(new SelectorBasedPodTemplateSelectStrategy(
        Collections.singletonList(
            new Selector("lowThrougput", null, null, Sets.newSet(dataSource)
            )
        )
    ));

    DynamicConfigPodTemplateSelector podTemplateSelector = new DynamicConfigPodTemplateSelector(
        props,
        dynamicConfigRef
    );

    Task taskWithMatchedDatasource = new NoopTask("id", "id", dataSource, 0, 0, null);
    Task noopTask = new NoopTask("id", "id", "datasource", 0, 0, null);
    Optional<PodTemplateWithName> actual = podTemplateSelector.getPodTemplateForTask(taskWithMatchedDatasource);
    Assertions.assertTrue(actual.isPresent());
    Assertions.assertEquals(1, actual.get().getPodTemplate().getTemplate().getSpec().getVolumes().size(), 1);

    actual = podTemplateSelector.getPodTemplateForTask(noopTask);
    Assertions.assertTrue(actual.isPresent());
    Assertions.assertEquals(0, actual.get().getPodTemplate().getTemplate().getSpec().getVolumes().size(), 1);
  }
}
