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
import com.google.inject.Inject;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogs;

import java.util.Locale;
import java.util.Properties;

public class KubernetesTaskRunnerFactory implements TaskRunnerFactory<KubernetesTaskRunner>
{
  public static final String TYPE_NAME = "k8s";
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private final StartupLoggingConfig startupLoggingConfig;
  private final TaskLogs taskLogs;
  private final DruidNode druidNode;
  private final TaskConfig taskConfig;
  private final Properties properties;
  private final DruidKubernetesClient druidKubernetesClient;
  private final ServiceEmitter emitter;
  private KubernetesTaskRunner runner;


  @Inject
  public KubernetesTaskRunnerFactory(
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal final HttpClient httpClient,
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      StartupLoggingConfig startupLoggingConfig,
      TaskLogs taskLogs,
      @Self DruidNode druidNode,
      TaskConfig taskConfig,
      Properties properties,
      DruidKubernetesClient druidKubernetesClient,
      ServiceEmitter emitter
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.kubernetesTaskRunnerConfig = kubernetesTaskRunnerConfig;
    this.startupLoggingConfig = startupLoggingConfig;
    this.taskLogs = taskLogs;
    this.druidNode = druidNode;
    this.taskConfig = taskConfig;
    this.properties = properties;
    this.druidKubernetesClient = druidKubernetesClient;
    this.emitter = emitter;
  }

  @Override
  public KubernetesTaskRunner build()
  {

    KubernetesPeonClient peonClient = new KubernetesPeonClient(
        druidKubernetesClient,
        kubernetesTaskRunnerConfig.getNamespace(),
        kubernetesTaskRunnerConfig.isDebugJobs(),
        emitter
    );

    runner = new KubernetesTaskRunner(
        buildTaskAdapter(druidKubernetesClient),
        kubernetesTaskRunnerConfig,
        peonClient,
        httpClient,
        new KubernetesPeonLifecycleFactory(peonClient, taskLogs, smileMapper),
        emitter
    );
    return runner;
  }

  @Override
  public KubernetesTaskRunner get()
  {
    return runner;
  }

  private TaskAdapter buildTaskAdapter(DruidKubernetesClient client)
  {
    String adapter = properties.getProperty(String.format(
        Locale.ROOT,
        "%s.%s.adapter.type",
        IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX,
        TYPE_NAME
    ));

    if (adapter != null && !MultiContainerTaskAdapter.TYPE.equals(adapter) && kubernetesTaskRunnerConfig.isSidecarSupport()) {
      throw new IAE(
          "Invalid pod adapter [%s], only pod adapter [%s] can be specified when sidecarSupport is enabled",
          adapter,
          MultiContainerTaskAdapter.TYPE
      );
    }

    if (MultiContainerTaskAdapter.TYPE.equals(adapter) || kubernetesTaskRunnerConfig.isSidecarSupport()) {
      return new MultiContainerTaskAdapter(
          client,
          kubernetesTaskRunnerConfig,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          smileMapper
      );
    } else if (PodTemplateTaskAdapter.TYPE.equals(adapter)) {
      return new PodTemplateTaskAdapter(
          kubernetesTaskRunnerConfig,
          taskConfig,
          druidNode,
          smileMapper,
          properties
      );
    } else {
      return new SingleContainerTaskAdapter(
          client,
          kubernetesTaskRunnerConfig,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          smileMapper
      );
    }
  }
}
