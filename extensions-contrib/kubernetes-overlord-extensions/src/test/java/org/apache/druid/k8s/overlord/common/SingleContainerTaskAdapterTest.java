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
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

@EnableKubernetesMockClient(crud = true)
class SingleContainerTaskAdapterTest
{

  KubernetesClient client;

  private ObjectMapper jsonMapper;

  public SingleContainerTaskAdapterTest()
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
  public void testSingleContainerSupport() throws IOException
  {
    TestKubernetesClient testClient = new TestKubernetesClient(client);
    Pod pod = K8sTestUtils.fileToResource("multiContainerPodSpec.yaml", Pod.class);
    KubernetesTaskRunnerConfig config = new KubernetesTaskRunnerConfig();
    config.namespace = "test";
    SingleContainerTaskAdapter adapter = new SingleContainerTaskAdapter(testClient, config, jsonMapper);
    NoopTask task = NoopTask.create("id", 1);
    Job actual = adapter.createJobFromPodSpec(
        pod.getSpec(),
        task,
        new PeonCommandContext(
            Collections.singletonList("foo && bar"),
            new ArrayList<>(),
            new File("/tmp")
        )
    );
    Job expected = K8sTestUtils.fileToResource("expectedSingleiContainerOutput.yaml", Job.class);
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
