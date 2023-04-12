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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.api.client.util.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableKubernetesMockClient(crud = true)
class K8sTaskAdapterTest
{
  private KubernetesClient client;

  private final StartupLoggingConfig startupLoggingConfig;
  private final TaskConfig taskConfig;
  private final DruidNode node;
  private final ObjectMapper jsonMapper;

  public K8sTaskAdapterTest()
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
    node = new DruidNode(
        "test",
        null,
        false,
        null,
        null,
        true,
        false
    );
    startupLoggingConfig = new StartupLoggingConfig();
    taskConfig = new TaskConfigBuilder().setBaseDir("src/test/resources").build();
  }

  @Test
  void testAddingLabelsAndAnnotations() throws IOException
  {
    final PodSpec podSpec = K8sTestUtils.getDummyPodSpec();
    TestKubernetesClient testClient = new TestKubernetesClient(client)
    {
      @SuppressWarnings("unchecked")
      @Override
      public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
      {
        return (T) new Pod()
        {
          @Override
          public PodSpec getSpec()
          {
            return podSpec;
          }
        };
      }
    };

    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.namespace = "test";
    config.annotations.put("annotation_key", "annotation_value");
    config.labels.put("label_key", "label_value");
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        node,
        jsonMapper
    );
    Task task = K8sTestUtils.getTask();
    Job jobFromSpec = adapter.fromTask(task);

    assertTrue(jobFromSpec.getMetadata().getAnnotations().containsKey("annotation_key"));
    assertTrue(jobFromSpec.getMetadata().getAnnotations().containsKey(DruidK8sConstants.TASK_ID));
    assertFalse(jobFromSpec.getMetadata().getAnnotations().containsKey("label_key"));
    assertTrue(jobFromSpec.getMetadata().getLabels().containsKey("label_key"));
    assertTrue(jobFromSpec.getMetadata().getLabels().containsKey(DruidK8sConstants.LABEL_KEY));
    assertFalse(jobFromSpec.getMetadata().getLabels().containsKey("annotation_key"));
  }

  @Test
  public void serializingAndDeserializingATask() throws IOException
  {
    // given a task create a k8s job
    TestKubernetesClient testClient = new TestKubernetesClient(client);
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.namespace = "test";
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        node,
        jsonMapper
    );
    Task task = K8sTestUtils.getTask();
    Job jobFromSpec = adapter.createJobFromPodSpec(
        K8sTestUtils.getDummyPodSpec(),
        task,
        new PeonCommandContext(new ArrayList<>(), new ArrayList<>(), new File("/tmp/"))
    );
    client.batch().v1().jobs().inNamespace("test").create(jobFromSpec);
    JobList jobList = client.batch().v1().jobs().inNamespace("test").list();
    assertEquals(1, jobList.getItems().size());

    // assert that the size of the pod is 1g
    Job myJob = Iterables.getOnlyElement(jobList.getItems());
    Quantity containerMemory = myJob.getSpec().getTemplate().getSpec().getContainers().get(0).getResources().getLimits().get("memory");
    String amount = containerMemory.getAmount();
    assertEquals(2400000000L, Long.valueOf(amount));
    assertTrue(StringUtils.isBlank(containerMemory.getFormat())); // no units specified we talk in bytes

    Task taskFromJob = adapter.toTask(Iterables.getOnlyElement(jobList.getItems()));
    assertEquals(task, taskFromJob);
  }

  @Test
  void testGrabbingTheLastXmxValueFromACommand()
  {
    List<String> commands = Lists.newArrayList("-Xmx2g", "-Xms1g", "-Xmx4g");
    Optional<Long> value = K8sTaskAdapter.getJavaOptValueBytes("-Xmx", commands);
    assertEquals(HumanReadableBytes.parse("4g"), value.get());

    // one without Xmx
    commands = new ArrayList<>();
    Optional<Long> result = K8sTaskAdapter.getJavaOptValueBytes("-Xmx", commands);
    assertFalse(result.isPresent());
  }

  @Test
  void testGettingContainerSize()
  {
    // nothing specified no heap no dbb should be (1g + 1g) * 1.2
    long expected = (long) ((HumanReadableBytes.parse("1g") + HumanReadableBytes.parse("1g")) * 1.2);
    PeonCommandContext context = new PeonCommandContext(
        new ArrayList<>(),
        new ArrayList<>(),
        new File("/tmp")
    );
    assertEquals(expected, K8sTaskAdapter.getContainerMemory(context));

    context = new PeonCommandContext(
        new ArrayList<>(),
        Collections.singletonList(
            "-server -Xms512m -Xmx512m -XX:MaxDirectMemorySize=1g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Djava.io.tmpdir=/druid/data -XX:+ExitOnOutOfMemoryError -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"),
        new File("/tmp")
    );
    expected = (long) ((HumanReadableBytes.parse("512m") + HumanReadableBytes.parse("1g")) * 1.2);
    assertEquals(expected, K8sTaskAdapter.getContainerMemory(context));
  }

  @Test
  void testMassagingSpec()
  {
    PodSpec spec = new PodSpec();
    List<Container> containers = new ArrayList<>();
    containers.add(new ContainerBuilder()
                       .withName("secondary").build());
    containers.add(new ContainerBuilder()
                       .withName("sidecar").build());
    containers.add(new ContainerBuilder()
                       .withName("primary").build());
    spec.setContainers(containers);
    K8sTaskAdapter.massageSpec(spec, "primary");

    List<Container> actual = spec.getContainers();
    Assertions.assertEquals(3, containers.size());
    Assertions.assertEquals("primary", actual.get(0).getName());
    Assertions.assertEquals("secondary", actual.get(1).getName());
    Assertions.assertEquals("sidecar", actual.get(2).getName());
  }

  @Test
  void testNoPrimaryFound()
  {
    PodSpec spec = new PodSpec();
    List<Container> containers = new ArrayList<>();
    containers.add(new ContainerBuilder()
                       .withName("istio-proxy").build());
    containers.add(new ContainerBuilder()
                       .withName("main").build());
    containers.add(new ContainerBuilder()
                       .withName("sidecar").build());
    spec.setContainers(containers);


    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      K8sTaskAdapter.massageSpec(spec, "primary");
    });
  }

  @Test
  void testAddingMonitors() throws IOException
  {
    TestKubernetesClient testClient = new TestKubernetesClient(client);
    PeonCommandContext context = new PeonCommandContext(
        new ArrayList<>(),
        new ArrayList<>(),
        new File("/tmp/")
    );
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.namespace = "test";
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        node,
        jsonMapper
    );
    Task task = K8sTestUtils.getTask();
    // no monitor in overlord, no monitor override
    Container container = new ContainerBuilder()
        .withName("container").build();
    adapter.addEnvironmentVariables(container, context, task.toString());
    assertFalse(
        container.getEnv().stream().anyMatch(x -> x.getName().equals("druid_monitoring_monitors")),
        "Didn't match, envs: " + Joiner.on(',').join(container.getEnv())
    );

    // we have an override, but nothing in the overlord
    config.peonMonitors = jsonMapper.readValue("[\"org.apache.druid.java.util.metrics.JvmMonitor\"]", List.class);
    adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        node,
        jsonMapper
    );
    adapter.addEnvironmentVariables(container, context, task.toString());
    EnvVar env = container.getEnv()
                          .stream()
                          .filter(x -> x.getName().equals("druid_monitoring_monitors"))
                          .findFirst()
                          .get();
    assertEquals(jsonMapper.writeValueAsString(config.peonMonitors), env.getValue());

    // we override what is in the overlord
    config.peonMonitors = jsonMapper.readValue("[\"org.apache.druid.java.util.metrics.JvmMonitor\"]", List.class);
    adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        node,
        jsonMapper
    );
    container.getEnv().add(new EnvVarBuilder()
                               .withName("druid_monitoring_monitors")
                               .withValue(
                                   "'[\"org.apache.druid.java.util.metrics.JvmMonitor\", "
                                   + "\"org.apache.druid.server.metrics.TaskCountStatsMonitor\"]'")
                               .build());
    adapter.addEnvironmentVariables(container, context, task.toString());
    env = container.getEnv()
                   .stream()
                   .filter(x -> x.getName().equals("druid_monitoring_monitors"))
                   .findFirst()
                   .get();
    assertEquals(jsonMapper.writeValueAsString(config.peonMonitors), env.getValue());
  }
}
