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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableKubernetesMockClient(crud = true)
class K8sTaskAdapterTest
{
  KubernetesClient client;

  private ObjectMapper jsonMapper;

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
  }

  @Test
  void testAddingLabelsAndAnnotations() throws IOException
  {
    TestKubernetesClient testClient = new TestKubernetesClient(client);
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.namespace = "test";
    config.annotations.put("annotation_key", "annotation_value");
    config.labels.put("label_key", "label_value");
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(testClient, config, jsonMapper);
    Task task = K8sTestUtils.getTask();
    Job jobFromSpec = adapter.createJobFromPodSpec(
        K8sTestUtils.getDummyPodSpec(),
        task,
        new PeonCommandContext(new ArrayList<>(), new ArrayList<>(), new File("/tmp/"))
    );
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
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(testClient, config, jsonMapper);
    Task task = K8sTestUtils.getTask();
    Job jobFromSpec = adapter.createJobFromPodSpec(
        K8sTestUtils.getDummyPodSpec(),
        task,
        new PeonCommandContext(new ArrayList<>(), new ArrayList<>(), new File("/tmp/"))
    );

    // cant launch jobs with test server, we have to hack around this.
    Pod pod = K8sTestUtils.createPodFromJob(jobFromSpec);
    client.pods().inNamespace("test").create(pod);
    PodList podList = client.pods().inNamespace("test").list();
    assertEquals(1, podList.getItems().size());

    // assert that the size of the pod is 1g
    Pod myPod = Iterables.getOnlyElement(podList.getItems());
    Quantity containerMemory = myPod.getSpec().getContainers().get(0).getResources().getLimits().get("memory");
    String amount = containerMemory.getAmount();
    assertEquals(2400000000L, Long.valueOf(amount));
    assertTrue(StringUtils.isBlank(containerMemory.getFormat())); // no units specified we talk in bytes

    Task taskFromPod = adapter.toTask(Iterables.getOnlyElement(podList.getItems()));
    assertEquals(task, taskFromPod);
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
                       .withName("excludeSidecar").build());
    containers.add(new ContainerBuilder()
                       .withName("sidecar").build());
    containers.add(new ContainerBuilder()
                       .withName("primary").build());
    spec.setContainers(containers);
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.primaryContainerName = "primary";
    Set<String> containersToExclude = new HashSet<>();
    containersToExclude.add("excludeSidecar");
    config.containersToExclude = containersToExclude;
    K8sTaskAdapter.massageSpec(config, spec);

    List<Container> actual = spec.getContainers();
    Assertions.assertEquals(2, containers.size());
    Assertions.assertEquals("primary", actual.get(0).getName());
    Assertions.assertEquals("sidecar", actual.get(1).getName());
  }

  @Test
  void testNoPrimaryFound() throws Exception
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
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.primaryContainerName = "primary";
    Set<String> containersToExclude = new HashSet<>();
    containersToExclude.add("istio-proxy");
    config.containersToExclude = containersToExclude;
    K8sTaskAdapter.massageSpec(config, spec);

    List<Container> actual = spec.getContainers();
    Assertions.assertEquals(2, actual.size());
    Assertions.assertEquals("main", actual.get(0).getName());
    Assertions.assertEquals("sidecar", actual.get(1).getName());
  }

}
