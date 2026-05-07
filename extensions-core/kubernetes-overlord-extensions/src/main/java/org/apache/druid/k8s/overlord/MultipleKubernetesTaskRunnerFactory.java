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
import com.google.inject.Inject;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.taskadapter.DynamicConfigPodTemplateSelector;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogs;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

public class MultipleKubernetesTaskRunnerFactory implements TaskRunnerFactory<TaskRunner>
{
  public static final String TYPE_NAME = "multik8s";
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final TaskLogs taskLogs;
  private final ServiceEmitter emitter;
  private final Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigSupplier;
  private final ConfigManager configManager;
  private final MultipleKubernetesTaskRunnerConfig runnerConfig;
  private final TaskConfig taskConfig;
  private final StartupLoggingConfig startupLoggingConfig;
  private final DruidNode druidNode;
  private final Properties properties;
  private final DruidKubernetesHttpClientFactory httpClientFactory;
  private TaskRunner runner;

  @Inject
  public MultipleKubernetesTaskRunnerFactory(
      @Json ObjectMapper objectMapper,
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal final HttpClient httpClient,
      TaskLogs taskLogs,
      Properties properties,
      ServiceEmitter emitter,
      Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigSupplier,
      @Nullable ConfigManager configManager,
      TaskConfig taskConfig,
      StartupLoggingConfig startupLoggingConfig,
      @Self DruidNode druidNode,
      DruidKubernetesHttpClientFactory httpClientFactory
  )
  {
    this.runnerConfig = MultipleKubernetesTaskRunnerConfig.fromProperties(objectMapper, properties);

    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.taskLogs = taskLogs;
    this.emitter = emitter;
    this.dynamicConfigSupplier = dynamicConfigSupplier;
    this.configManager = configManager;
    this.taskConfig = taskConfig;
    this.startupLoggingConfig = startupLoggingConfig;
    this.druidNode = druidNode;
    this.properties = properties;
    this.httpClientFactory = httpClientFactory;
  }

  @Override
  public TaskRunner build()
  {
    final List<MultipleKubernetesTaskRunnerConfig.KubernetesCluster> enabledClusters = this.runnerConfig.getClusters()
                                                                                                        .stream()
                                                                                                        .filter(cluster -> !cluster.isDisabled())
                                                                                                        .collect(Collectors.toList());

    if (enabledClusters.isEmpty()) {
      throw new IllegalArgumentException("At least one task runner must be enabled");
    }

    final int totalCapacity = new KubernetesTaskRunnerEffectiveConfig(this.runnerConfig, this.dynamicConfigSupplier).getCapacity();
    final AutoscalableThreadPoolExecutor sharedExecutor = new AutoscalableThreadPoolExecutor(totalCapacity, this.configManager);

    final List<MultipleKubernetesTaskRunnerDelegate> taskRunners = new ArrayList<>();
    for (MultipleKubernetesTaskRunnerConfig.KubernetesCluster kubernetesCluster : this.runnerConfig.getClusters()) {

      final KubernetesTaskRunnerStaticConfig clusterConfig = getPerClusterConfiguration(kubernetesCluster);
      final KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
          clusterConfig,
          this.dynamicConfigSupplier
      );

      final DruidKubernetesClient client = createClientForCluster(kubernetesCluster, clusterConfig);
      final TaskAdapter clusterTaskAdapter = buildTaskAdapter(client, effectiveConfig);
      final boolean useOverlordNamespace = PodTemplateTaskAdapter.TYPE.equals(clusterTaskAdapter.getAdapterType());

      final KubernetesPeonClient peonClient = new KubernetesPeonClient(
          client,
          effectiveConfig.getNamespace(),
          useOverlordNamespace ? effectiveConfig.getOverlordNamespace() : "",
          effectiveConfig.isDebugJobs(),
          emitter
      );

      final KubernetesTaskRunner clusterRunner = new KubernetesTaskRunner(
          clusterTaskAdapter,
          effectiveConfig,
          peonClient,
          httpClient,
          new KubernetesPeonLifecycleFactory(
              peonClient,
              taskLogs,
              smileMapper,
              effectiveConfig.getLogSaveTimeout().toStandardDuration().getMillis()
          ),
          emitter,
          sharedExecutor,
          configManager
      );

      taskRunners.add(
          new MultipleKubernetesTaskRunnerDelegate(
              clusterRunner,
              kubernetesCluster.getName(),
              kubernetesCluster.isDisabled(),
              client
          )
      );
    }

    this.runner = new MultipleKubernetesTaskRunner(
        new KubernetesTaskRunnerEffectiveConfig(
            this.runnerConfig,
            this.dynamicConfigSupplier
        ),
        runnerConfig.getClusterSelector(),
        taskRunners,
        sharedExecutor
    );
    return this.runner;
  }

  @Override
  public TaskRunner get()
  {
    return runner;
  }

  private DruidKubernetesClient createClientForCluster(
      MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster,
      KubernetesTaskRunnerStaticConfig clusterConfig
  )
  {
    Config config;
    if (cluster.getKubeconfigPath() != null && !cluster.getKubeconfigPath().trim().isEmpty()) {
      config = Config.fromKubeconfig(new File(cluster.getKubeconfigPath()));
    } else {
      config = new ConfigBuilder().build();
    }

    if (clusterConfig.isDisableClientProxy()) {
      config.setHttpsProxy(null);
      config.setHttpProxy(null);
    }

    config.setNamespace(clusterConfig.getNamespace());

    return new DruidKubernetesClient(httpClientFactory, config);
  }

  private TaskAdapter buildTaskAdapter(
      DruidKubernetesClient client,
      KubernetesTaskRunnerEffectiveConfig effectiveConfig
  )
  {
    final String adapter = properties.getProperty(String.format(
        Locale.ROOT,
        "%s.%s.adapter.type",
        IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX,
        KubernetesTaskRunnerFactory.TYPE_NAME
    ));

    if (adapter != null
        && !MultiContainerTaskAdapter.TYPE.equals(adapter)
        && effectiveConfig.isSidecarSupport()) {
      throw new IAE(
          "Invalid pod adapter [%s], only pod adapter [%s] can be specified when sidecarSupport is enabled",
          adapter,
          MultiContainerTaskAdapter.TYPE
      );
    }

    if (MultiContainerTaskAdapter.TYPE.equals(adapter) || effectiveConfig.isSidecarSupport()) {
      return new MultiContainerTaskAdapter(
          client,
          effectiveConfig,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          smileMapper,
          taskLogs
      );
    } else if (PodTemplateTaskAdapter.TYPE.equals(adapter)) {
      return new PodTemplateTaskAdapter(
          effectiveConfig,
          taskConfig,
          druidNode,
          smileMapper,
          taskLogs,
          new DynamicConfigPodTemplateSelector(properties, effectiveConfig)
      );
    } else {
      return new SingleContainerTaskAdapter(
          client,
          effectiveConfig,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          smileMapper,
          taskLogs
      );
    }
  }

  private KubernetesTaskRunnerStaticConfig getPerClusterConfiguration(
      MultipleKubernetesTaskRunnerConfig.KubernetesCluster cluster
  )
  {
    return new KubernetesTaskRunnerStaticConfig(
        cluster.getTaskNamespace(),
        cluster.getOverlordIdentifier(),
        this.runnerConfig.getK8sTaskPodNamePrefix(),
        this.runnerConfig.isDebugJobs(),
        this.runnerConfig.isSidecarSupport(),
        this.runnerConfig.getPrimaryContainerName(),
        this.runnerConfig.getKubexitImage(),
        this.runnerConfig.getGraceTerminationPeriodSeconds(),
        this.runnerConfig.isDisableClientProxy(),
        this.runnerConfig.getTaskTimeout(),
        this.runnerConfig.getTaskCleanupDelay(),
        this.runnerConfig.getTaskCleanupInterval(),
        this.runnerConfig.getTaskLaunchTimeout(),
        this.runnerConfig.getLogSaveTimeout(),
        this.runnerConfig.getPeonMonitors(),
        this.runnerConfig.getJavaOptsArray(),
        this.runnerConfig.getCpuCoreInMicro(),
        this.runnerConfig.getLabels(),
        this.runnerConfig.getAnnotations(),
        this.runnerConfig.getCapacity(),
        this.runnerConfig.getTaskJoinTimeout(),
        this.runnerConfig.isUseK8sSharedInformers(),
        this.runnerConfig.getK8sSharedInformerResyncPeriod()
    );
  }
}
