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
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.JacksonConfigProvider;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.DruidKubernetesCachingClient;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.jdk.DruidKubernetesJdkHttpClientConfig;
import org.apache.druid.k8s.overlord.common.httpclient.jdk.DruidKubernetesJdkHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.okhttp.DruidKubernetesOkHttpHttpClientConfig;
import org.apache.druid.k8s.overlord.common.httpclient.okhttp.DruidKubernetesOkHttpHttpClientFactory;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientConfig;
import org.apache.druid.k8s.overlord.common.httpclient.vertx.DruidKubernetesVertxHttpClientFactory;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskExecutionConfigResource;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.runnerstrategy.RunnerStrategy;
import org.apache.druid.k8s.overlord.taskadapter.DynamicConfigPodTemplateSelector;
import org.apache.druid.k8s.overlord.taskadapter.MultiContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.SingleContainerTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogs;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Properties;


@LoadScope(roles = NodeRole.OVERLORD_JSON_NAME)
public class KubernetesOverlordModule implements DruidModule
{
  private static final Logger log = new Logger(KubernetesOverlordModule.class);
  private static final String K8SANDWORKER_PROPERTIES_PREFIX = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
                                                               + ".k8sAndWorker";
  private static final String RUNNERSTRATEGY_PROPERTIES_FORMAT_STRING = K8SANDWORKER_PROPERTIES_PREFIX
                                                                        + ".runnerStrategy.%s";
  private static final String K8SANDWORKER_HTTPCLIENT_PROPERTIES_PREFIX = K8SANDWORKER_PROPERTIES_PREFIX + ".http";
  private static final String HTTPCLIENT_TYPE_PROPERTY = K8SANDWORKER_HTTPCLIENT_PROPERTIES_PREFIX + ".httpClientType";
  private static final String VERTX_HTTPCLIENT_PROPERITES_PREFIX = K8SANDWORKER_HTTPCLIENT_PROPERTIES_PREFIX + "." + DruidKubernetesVertxHttpClientFactory.TYPE_NAME;
  private static final String OKHTTP_HTTPCLIENT_PROPERITES_PREFIX = K8SANDWORKER_HTTPCLIENT_PROPERTIES_PREFIX + "." + DruidKubernetesOkHttpHttpClientFactory.TYPE_NAME;
  public static final String JDK_HTTPCLIENT_PROPERITES_PREFIX = K8SANDWORKER_HTTPCLIENT_PROPERTIES_PREFIX + "." + DruidKubernetesJdkHttpClientFactory.TYPE_NAME;

  @Override
  public void configure(Binder binder)
  {
    // druid.indexer.runner.type=k8s
    JsonConfigProvider.bind(binder, IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX, KubernetesTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, K8SANDWORKER_PROPERTIES_PREFIX, KubernetesAndWorkerTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.indexer.queue", TaskQueueConfig.class);
    JacksonConfigProvider.bind(binder, KubernetesTaskRunnerDynamicConfig.CONFIG_KEY, KubernetesTaskRunnerDynamicConfig.class, null);
    PolyBind.createChoice(
        binder,
        "druid.indexer.runner.type",
        Key.get(TaskRunnerFactory.class),
        Key.get(KubernetesTaskRunnerFactory.class)
    );
    final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(
        binder,
        Key.get(TaskRunnerFactory.class)
    );

    biddy.addBinding(KubernetesTaskRunnerFactory.TYPE_NAME)
         .to(KubernetesTaskRunnerFactory.class)
         .in(LazySingleton.class);
    biddy.addBinding(KubernetesAndWorkerTaskRunnerFactory.TYPE_NAME)
         .to(KubernetesAndWorkerTaskRunnerFactory.class)
         .in(LazySingleton.class);
    binder.bind(KubernetesTaskRunnerFactory.class).in(LazySingleton.class);
    binder.bind(KubernetesAndWorkerTaskRunnerFactory.class).in(LazySingleton.class);
    binder.bind(RunnerStrategy.class)
          .toProvider(RunnerStrategyProvider.class)
          .in(LazySingleton.class);

    Jerseys.addResource(binder, KubernetesTaskExecutionConfigResource.class);

    PolyBind.createChoiceWithDefault(
        binder,
        HTTPCLIENT_TYPE_PROPERTY,
        Key.get(DruidKubernetesHttpClientFactory.class),
        DruidKubernetesVertxHttpClientFactory.TYPE_NAME
    );

    final MapBinder<String, DruidKubernetesHttpClientFactory> factoryBinder =
        PolyBind.optionBinder(binder, Key.get(DruidKubernetesHttpClientFactory.class));

    factoryBinder.addBinding(DruidKubernetesVertxHttpClientFactory.TYPE_NAME)
                 .toProvider(VertxHttpClientFactoryProvider.class)
                 .in(LazySingleton.class);

    factoryBinder.addBinding(DruidKubernetesOkHttpHttpClientFactory.TYPE_NAME)
                 .toProvider(OkHttpHttpClientFactoryProvider.class)
                 .in(LazySingleton.class);
    factoryBinder.addBinding(DruidKubernetesJdkHttpClientFactory.TYPE_NAME)
                 .toProvider(JdkHttpClientFactoryProvider.class)
                 .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, VERTX_HTTPCLIENT_PROPERITES_PREFIX, DruidKubernetesVertxHttpClientConfig.class);
    JsonConfigProvider.bind(binder, OKHTTP_HTTPCLIENT_PROPERITES_PREFIX, DruidKubernetesOkHttpHttpClientConfig.class);
    JsonConfigProvider.bind(binder, JDK_HTTPCLIENT_PROPERITES_PREFIX, DruidKubernetesJdkHttpClientConfig.class);
  }

  /**
   * Provides the base Kubernetes client for direct API operations.
   * This is always created regardless of caching configuration.
   */
  @Provides
  @LazySingleton
  public DruidKubernetesClient makeBaseKubernetesClient(
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      DruidKubernetesHttpClientFactory httpClientFactory,
      Lifecycle lifecycle
  )
  {
    final Config config = new ConfigBuilder().build();

    if (kubernetesTaskRunnerConfig.isDisableClientProxy()) {
      config.setHttpsProxy(null);
      config.setHttpProxy(null);
    }

    config.setNamespace(kubernetesTaskRunnerConfig.getNamespace());

    final DruidKubernetesClient client = new DruidKubernetesClient(httpClientFactory, config);

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {

          }

          @Override
          public void stop()
          {
            log.info("Stopping base Kubernetes client");
            client.getClient().close();
          }
        }
    );

    return client;
  }

  /**
   * Provides the caching Kubernetes client that uses informers for efficient resource watching.
   * Only created when caching is enabled via configuration.
   */
  @Provides
  @LazySingleton
  @Nullable
  public DruidKubernetesCachingClient makeCachingKubernetesClient(
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      DruidKubernetesClient baseClient,
      Lifecycle lifecycle
  )
  {
    if (!kubernetesTaskRunnerConfig.isUseK8sSharedInformers()) {
      log.info("Kubernetes shared informers disabled, caching client will not be created");
      return null;
    }

    String namespace = kubernetesTaskRunnerConfig.getNamespace();
    long resyncPeriodMillis = kubernetesTaskRunnerConfig
        .getK8sSharedInformerResyncPeriod()
        .toStandardDuration()
        .getMillis();

    log.info("Creating Kubernetes caching client with informer resync period: %d ms", resyncPeriodMillis);
    final DruidKubernetesCachingClient cachingClient = new DruidKubernetesCachingClient(
        baseClient,
        namespace,
        resyncPeriodMillis
    );

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {

          }

          @Override
          public void stop()
          {
            log.info("Stopping Kubernetes caching client");
            cachingClient.stop();
          }
        }
    );

    return cachingClient;
  }

  /**
   * Provides a TaskRunnerFactory instance suitable for environments without Zookeeper.
   * In such environments, the standard RemoteTaskRunnerFactory may not be operational.
   * Depending on the workerType defined in KubernetesAndWorkerTaskRunnerConfig,
   * this method selects and returns an appropriate TaskRunnerFactory implementation.
   */
  @Provides
  @LazySingleton
  @Named("taskRunnerFactory")
  TaskRunnerFactory<? extends WorkerTaskRunner> provideWorkerTaskRunner(
      KubernetesAndWorkerTaskRunnerConfig runnerConfig,
      Injector injector
  )
  {
    String workerType = runnerConfig.getWorkerType();
    return HttpRemoteTaskRunnerFactory.TYPE_NAME.equals(workerType)
           ? injector.getInstance(HttpRemoteTaskRunnerFactory.class)
           : injector.getInstance(RemoteTaskRunnerFactory.class);
  }

  /**
   * Provides a TaskAdapter instance for the KubernetesTaskRunner.
   */
  @Provides
  @LazySingleton
  TaskAdapter provideTaskAdapter(
      DruidKubernetesClient client,
      Properties properties,
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      TaskConfig taskConfig,
      StartupLoggingConfig startupLoggingConfig,
      @Self DruidNode druidNode,
      @Smile ObjectMapper smileMapper,
      TaskLogs taskLogs,
      Supplier<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef
  )
  {
    String adapter = properties.getProperty(String.format(
        Locale.ROOT,
        "%s.%s.adapter.type",
        IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX,
        "k8s"
    ));

    if (adapter != null
        && !MultiContainerTaskAdapter.TYPE.equals(adapter)
        && kubernetesTaskRunnerConfig.isSidecarSupport()) {
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
          smileMapper,
          taskLogs
      );
    } else if (PodTemplateTaskAdapter.TYPE.equals(adapter)) {
      return new PodTemplateTaskAdapter(
          kubernetesTaskRunnerConfig,
          taskConfig,
          druidNode,
          smileMapper,
          taskLogs,
          new DynamicConfigPodTemplateSelector(properties, dynamicConfigRef)
      );
    } else {
      return new SingleContainerTaskAdapter(
          client,
          kubernetesTaskRunnerConfig,
          taskConfig,
          startupLoggingConfig,
          druidNode,
          smileMapper,
          taskLogs
      );
    }
  }

  private static class RunnerStrategyProvider implements Provider<RunnerStrategy>
  {
    private KubernetesAndWorkerTaskRunnerConfig runnerConfig;
    private Properties props;
    private JsonConfigurator configurator;

    @Inject
    public void inject(
        KubernetesAndWorkerTaskRunnerConfig runnerConfig,
        Properties props,
        JsonConfigurator configurator
    )
    {
      this.runnerConfig = runnerConfig;
      this.props = props;
      this.configurator = configurator;
    }

    @Override
    public RunnerStrategy get()
    {
      String runnerStrategy = runnerConfig.getRunnerStrategy();

      final String runnerStrategyPropertyBase = StringUtils.format(
          RUNNERSTRATEGY_PROPERTIES_FORMAT_STRING,
          runnerStrategy
      );
      final JsonConfigProvider<RunnerStrategy> provider = JsonConfigProvider.of(
          runnerStrategyPropertyBase,
          RunnerStrategy.class
      );

      props.put(runnerStrategyPropertyBase + ".type", runnerStrategy);
      provider.inject(props, configurator);

      return provider.get();
    }
  }

  private static class VertxHttpClientFactoryProvider implements Provider<DruidKubernetesHttpClientFactory>
  {
    private DruidKubernetesVertxHttpClientConfig config;

    @Inject
    public void inject(DruidKubernetesVertxHttpClientConfig config)
    {
      this.config = config;  // Guice injects the Vertx-specific config
    }

    @Override
    public DruidKubernetesHttpClientFactory get()
    {
      return new DruidKubernetesVertxHttpClientFactory(config);
    }
  }

  private static class OkHttpHttpClientFactoryProvider implements Provider<DruidKubernetesHttpClientFactory>
  {
    private DruidKubernetesOkHttpHttpClientConfig config;

    @Inject
    public void inject(DruidKubernetesOkHttpHttpClientConfig config)
    {
      this.config = config;  // Guice injects the OkHttp-specific config
    }

    @Override
    public DruidKubernetesHttpClientFactory get()
    {
      return new DruidKubernetesOkHttpHttpClientFactory(config);
    }
  }

  private static class JdkHttpClientFactoryProvider implements Provider<DruidKubernetesHttpClientFactory>
  {
    private DruidKubernetesJdkHttpClientConfig config;

    @Inject
    public void inject(DruidKubernetesJdkHttpClientConfig config)
    {
      this.config = config;
    }

    @Override
    public DruidKubernetesHttpClientFactory get()
    {
      return new DruidKubernetesJdkHttpClientFactory(config);
    }
  }
}
