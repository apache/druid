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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.common.PeonCommandContext;
import org.apache.druid.k8s.overlord.common.TestKubernetesClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

@EnableKubernetesMockClient(crud = true)
class SingleContainerTaskAdapterTest
{
  private KubernetesClient client;
  private StartupLoggingConfig startupLoggingConfig;
  private TaskConfig taskConfig;
  private DruidNode druidNode;
  private ObjectMapper jsonMapper;

  @Mock private TaskLogs taskLogs;

  @BeforeEach
  public void setup()
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
    druidNode = new DruidNode(
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
  public void testSingleContainerSupport() throws IOException
  {
    TestKubernetesClient testClient = new TestKubernetesClient(client);
    Pod pod = K8sTestUtils.fileToResource("multiContainerPodSpec.yaml", Pod.class);
    KubernetesTaskRunnerConfig config = KubernetesTaskRunnerConfig.builder()
        .withNamespace("test")
        .build();
    SingleContainerTaskAdapter adapter = new SingleContainerTaskAdapter(
        testClient,
        config,
        taskConfig,
        startupLoggingConfig,
        druidNode,
        jsonMapper,
        taskLogs
    );
    NoopTask task = K8sTestUtils.createTask("id", 1);
    Job actual = adapter.createJobFromPodSpec(
        pod.getSpec(),
        task,
        new PeonCommandContext(
            Collections.singletonList("foo && bar"),
            new ArrayList<>(),
            new File("/tmp"),
            config.getCpuCoreInMicro()
        )
    );

    Job expected = K8sTestUtils.fileToResource("expectedSingleContainerOutput.yaml", Job.class);
    // something is up with jdk 17, where if you compress with jdk < 17 and try and decompress you get different results,
    // this would never happen in real life, but for the jdk 17 tests this is a problem
    // could be related to: https://bugs.openjdk.org/browse/JDK-8081450
    actual.getSpec()
          .getTemplate()
          .getSpec()
          .getContainers()
          .get(0)
          .getEnv()
          .removeIf(x -> x.getName().equals("TASK_JSON"));
    expected.getSpec()
            .getTemplate()
            .getSpec()
            .getContainers()
            .get(0)
            .getEnv()
            .removeIf(x -> x.getName().equals("TASK_JSON"));
    Assertions.assertEquals(expected, actual);
  }
}
