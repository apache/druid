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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.k8s.overlord.common.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.common.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.common.SingleContainerTaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Properties;

public class KubernetesTaskRunnerFactoryTest
{
  private ObjectMapper objectMapper;
  private KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private StartupLoggingConfig startupLoggingConfig;
  private TaskQueueConfig taskQueueConfig;
  private TaskLogPusher taskLogPusher;
  private DruidNode druidNode;
  private TaskConfig taskConfig;
  private Properties properties;

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
    properties = new Properties();
  }

  @Test
  public void test_get_returnsSameKuberentesTaskRunner_asBuild()
  {
    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        properties
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
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        properties
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof SingleContainerTaskAdapter);
  }

  @Test
  public void test_build_withSidecarSupport_returnsKubernetesTaskRunnerWithMultiContainerTaskAdapter()
  {
    kubernetesTaskRunnerConfig.sidecarSupport = true;

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        properties
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withSingleContainerAdapterType_returnsKubernetesTaskRunnerWithSingleContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordSingleContainer");

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        props
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof SingleContainerTaskAdapter);
  }

  @Test
  public void test_build_withSingleContainerAdapterTypeAndSidecarSupport_throwsIAE()
  {
    kubernetesTaskRunnerConfig.sidecarSupport = true;

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordSingleContainer");

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        props
    );

    Assert.assertThrows(
        "Invalid pod adapter [overlordSingleContainer], only pod adapter [overlordMultiContainer] can be specified when sidecarSupport is enabled",
        IAE.class,
        factory::build
    );
  }

  @Test
  public void test_build_withMultiContainerAdapterType_returnsKubernetesTaskRunnerWithMultiContainerTaskAdapter()
  {
    kubernetesTaskRunnerConfig.sidecarSupport = true;

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordMultiContainer");

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        props
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withMultiContainerAdapterTypeAndSidecarSupport_returnsKubernetesTaskRunnerWithMultiContainerTaskAdapter()
  {
    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "overlordMultiContainer");

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        props
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof MultiContainerTaskAdapter);
  }

  @Test
  public void test_build_withPodTemplateAdapterType_returnsKubernetesTaskRunnerWithPodTemplateTaskAdapter()
  {
    URL url = this.getClass().getClassLoader().getResource("basePodTemplate.yaml");

    Properties props = new Properties();
    props.setProperty("druid.indexer.runner.k8s.adapter.type", "customTemplateAdapter");
    props.setProperty("druid.indexer.runner.k8s.podTemplate.base", url.getPath());

    KubernetesTaskRunnerFactory factory = new KubernetesTaskRunnerFactory(
        objectMapper,
        null,
        kubernetesTaskRunnerConfig,
        startupLoggingConfig,
        taskQueueConfig,
        taskLogPusher,
        druidNode,
        taskConfig,
        props
    );

    KubernetesTaskRunner runner = factory.build();

    Assert.assertNotNull(runner);
    Assert.assertTrue(runner.adapter instanceof PodTemplateTaskAdapter);
  }
}
