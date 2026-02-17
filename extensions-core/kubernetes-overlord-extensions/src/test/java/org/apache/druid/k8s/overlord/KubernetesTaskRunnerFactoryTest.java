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

package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.k8s.overlord.common.DruidKubernetesCachingClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientConfig;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientFactory;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KubernetesTaskRunnerFactoryTest
{
  private ObjectMapper objectMapper;
  private KubernetesTaskRunnerEffectiveConfig kubernetesTaskRunnerConfig;
  private TaskLogs taskLogs;

  private DruidKubernetesClient druidKubernetesClient;
  private DruidKubernetesCachingClient druidKubernetesCachingClient;
  @Mock private ServiceEmitter emitter;
  private TaskAdapter taskAdapter;
  @Mock private ConfigManager configManager;

  @Before
  public void setup()
  {
    objectMapper = new TestUtils().getTestObjectMapper();
    KubernetesTaskRunnerStaticConfig kubernetesTaskRunnerStaticConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(1)
        .build();
    taskLogs = new NoopTaskLogs();

    Config config = new ConfigBuilder().build();

    druidKubernetesClient =
        new DruidKubernetesClient(new DruidKubernetesVertxHttpClientFactory(new DruidKubernetesVertxHttpClientConfig()), config);
    druidKubernetesCachingClient = null;
    taskAdapter = new TestTaskAdapter();
    kubernetesTaskRunnerConfig = new KubernetesTaskRunnerEffectiveConfig(kubernetesTaskRunnerStaticConfig, () -> null);
    configManager = EasyMock.createNiceMock(ConfigManager.class);
    EasyMock.replay(configManager);
  }

  @Test
  public void test_get_returnsSameKuberentesTaskRunner_asBuild()
  {
    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        taskLogs,
        druidKubernetesClient,
        emitter,
        taskAdapter,
        configManager,
        druidKubernetesCachingClient
    );

    KubernetesTaskRunner expectedRunner = factory.build();
    KubernetesTaskRunner actualRunner = factory.get();

    Assert.assertEquals(expectedRunner, actualRunner);
  }

  static class TestTaskAdapter implements TaskAdapter
  {
    @Override
    public String getAdapterType()
    {
      return "";
    }

    @Override
    public Job fromTask(Task task) throws IOException
    {
      return null;
    }

    @Override
    public Task toTask(Job from) throws IOException
    {
      return null;
    }

    @Override
    public K8sTaskId getTaskId(Job from)
    {
      return null;
    }

    @Override
    public boolean shouldUseDeepStorageForTaskPayload(Task task) throws IOException
    {
      return false;
    }
  }
}
