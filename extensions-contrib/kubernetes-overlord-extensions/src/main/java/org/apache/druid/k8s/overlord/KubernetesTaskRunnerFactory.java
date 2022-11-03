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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.fabric8.kubernetes.client.Config;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.K8sTaskAdapter;
import org.apache.druid.k8s.overlord.common.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.common.SingleContainerTaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogPusher;

public class KubernetesTaskRunnerFactory implements TaskRunnerFactory<KubernetesTaskRunner>
{
  public static final String TYPE_NAME = "k8s";
  private final ObjectMapper smileMapper;
  private final KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private final TaskConfig taskConfig;
  private final StartupLoggingConfig startupLoggingConfig;
  private final TaskQueueConfig taskQueueConfig;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode druidNode;
  private KubernetesTaskRunner runner;


  @Inject
  public KubernetesTaskRunnerFactory(
      @Smile ObjectMapper smileMapper,
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      TaskConfig taskConfig,
      StartupLoggingConfig startupLoggingConfig,
      @JacksonInject TaskQueueConfig taskQueueConfig,
      TaskLogPusher taskLogPusher,
      @Self DruidNode druidNode
  )
  {

    this.smileMapper = smileMapper;
    this.kubernetesTaskRunnerConfig = kubernetesTaskRunnerConfig;
    this.taskConfig = taskConfig;
    this.startupLoggingConfig = startupLoggingConfig;
    this.taskQueueConfig = taskQueueConfig;
    this.taskLogPusher = taskLogPusher;
    this.druidNode = druidNode;
  }

  @Override
  public KubernetesTaskRunner build()
  {
    DruidKubernetesClient client;
    if (kubernetesTaskRunnerConfig.disableClientProxy) {
      Config config = Config.autoConfigure(null);
      config.setHttpsProxy(null);
      config.setHttpProxy(null);
      client = new DruidKubernetesClient(config);
    } else {
      client = new DruidKubernetesClient();
    }

    K8sTaskAdapter adapter;
    if (kubernetesTaskRunnerConfig.sidecarSupport) {
      adapter = new MultiContainerTaskAdapter(client, kubernetesTaskRunnerConfig, smileMapper);
    } else {
      adapter = new SingleContainerTaskAdapter(client, kubernetesTaskRunnerConfig, smileMapper);
    }

    runner = new KubernetesTaskRunner(
        taskConfig,
        startupLoggingConfig,
        adapter,
        kubernetesTaskRunnerConfig,
        taskQueueConfig,
        taskLogPusher,
        new DruidKubernetesPeonClient(client, kubernetesTaskRunnerConfig.namespace, kubernetesTaskRunnerConfig.debugJobs),
        druidNode
    );
    return runner;
  }

  @Override
  public KubernetesTaskRunner get()
  {
    return runner;
  }
}
