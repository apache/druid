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
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KubernetesTaskRunnerFactoryTest
{
  private ObjectMapper objectMapper;
  private KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private StartupLoggingConfig startupLoggingConfig;
  private TaskQueueConfig taskQueueConfig;
  private TaskLogPusher taskLogPusher;
  private DruidNode druidNode;
  private TaskConfig taskConfig;

  @Before
  public void setup()
  {
    objectMapper = new TestUtils().getTestObjectMapper();
    kubernetesTaskRunnerConfig = new KubernetesTaskRunnerConfig();
    startupLoggingConfig = new StartupLoggingConfig();
    taskQueueConfig = new TaskQueueConfig(
        1,
        null,
        null,
        null
    );
    taskLogPusher = new NoopTaskLogs();
    druidNode = new DruidNode(
        "test",
        "",
        false,
        0,
        1,
        true,
        false
    );
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
  }

  @Test
  public void test_get_returnsSameKuberentesTaskRunner_asBuild()
  {
    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig
    );

    KubernetesTaskRunner expectedRunner = factory.build();
    KubernetesTaskRunner actualRunner = factory.get();

    Assert.assertEquals(expectedRunner, actualRunner);
  }

  @Test
  public void test_build_withoutSidecarSupport_returnsKubernetesTaskRunnerWithSingleContainerTaskAdapter()
  {
    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig
    );

    KubernetesTaskRunner runner = factory.build();
    Assert.assertNotNull(runner);
  }

  @Test
  public void test_build_withSidecarSupport_returnsKubernetesTaskRunnerWithMultiContainerTaskAdapter()
  {
    kubernetesTaskRunnerConfig.sidecarSupport = true;

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
  }

  @Test
  public void test_build_withClientProxyDisabled_returnsKubernetesTaskRunnerWithDruidKubernetesClientWithoutClientProxySupport()
  {
    kubernetesTaskRunnerConfig.disableClientProxy = true;

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
  }
}
