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
import com.google.common.base.Supplier;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogs;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class KubernetesTaskRunnerFactoryTest
{
  private ObjectMapper objectMapper;
  private KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private StartupLoggingConfig startupLoggingConfig;
  private TaskLogs taskLogs;
  private DruidNode druidNode;
  private TaskConfig taskConfig;
  private Properties properties;

  private DruidKubernetesClient druidKubernetesClient;
  @Mock private ServiceEmitter emitter;
  @Mock private Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef;
  @Mock private TaskAdapter taskAdapter;

  @Before
  public void setup()
  {
    objectMapper = new TestUtils().getTestObjectMapper();
    kubernetesTaskRunnerConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(1)
        .build();
    startupLoggingConfig = new StartupLoggingConfig();
    taskLogs = new NoopTaskLogs();
    druidNode = new DruidNode(
        "test",
        "",
        false,
        0,
        1,
        true,
        false
    );
    taskConfig = new TaskConfigBuilder().setBaseDir("/tmp").build();
    properties = new Properties();
    druidKubernetesClient = new DruidKubernetesClient();
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
        taskAdapter
    );

    KubernetesTaskRunner expectedRunner = factory.build();
    KubernetesTaskRunner actualRunner = factory.get();

    Assert.assertEquals(expectedRunner, actualRunner);
  }
}
