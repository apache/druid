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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.k8s.overlord.common.DirectKubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesHttpClientConfig;
import org.apache.druid.k8s.overlord.common.KubernetesClientApi;
import org.apache.druid.k8s.overlord.common.AbstractKubernetesPeonClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

// must have a kind / minikube cluster installed and the image pushed to your repository
@Disabled
public class DruidPeonClientIntegrationTest
{
  private StartupLoggingConfig startupLoggingConfig;
  private TaskConfig taskConfig;
  private DruidNode druidNode;
  private KubernetesClientApi k8sClient;
  private AbstractKubernetesPeonClient peonClient;
  private ObjectMapper jsonMapper;

  @BeforeEach
  public void setup()
  {
    TestUtils utils = new TestUtils();
    jsonMapper = utils.getTestObjectMapper();
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTask.IndexTuningConfig.class, "index")
    );
    k8sClient = new DruidKubernetesClient(new DruidKubernetesHttpClientConfig(), new ConfigBuilder().build(), false);
    peonClient = new DirectKubernetesPeonClient(k8sClient, "default", false, new NoopServiceEmitter());
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
}
