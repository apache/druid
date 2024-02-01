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
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.indexing.common.tasklogs.FileTaskLogs;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.runnerstrategy.RunnerStrategy;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogKiller;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogs;

import java.util.Properties;


@LoadScope(roles = NodeRole.OVERLORD_JSON_NAME)
public class KubernetesOverlordModule implements DruidModule
{

  private static final Logger log = new Logger(KubernetesOverlordModule.class);
  private static final String K8SANDWORKER_PROPERTIES_PREFIX = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
                                                               + ".k8sAndWorker";
  private static final String RUNNERSTRATEGY_PROPERTIES_FORMAT_STRING = K8SANDWORKER_PROPERTIES_PREFIX
                                                                        + ".runnerStrategy.%s";

  @Override
  public void configure(Binder binder)
  {
    // druid.indexer.runner.type=k8s
    JsonConfigProvider.bind(binder, IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX, KubernetesTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, K8SANDWORKER_PROPERTIES_PREFIX, KubernetesAndWorkerTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.indexer.queue", TaskQueueConfig.class);
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
    configureTaskLogs(binder);
  }

  @Provides
  @LazySingleton
  public DruidKubernetesClient makeKubernetesClient(KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig, Lifecycle lifecycle)
  {
    DruidKubernetesClient client;
    if (kubernetesTaskRunnerConfig.isDisableClientProxy()) {
      Config config = new ConfigBuilder().build();
      config.setHttpsProxy(null);
      config.setHttpProxy(null);
      client = new DruidKubernetesClient(config);
    } else {
      client = new DruidKubernetesClient();
    }

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
            log.info("Stopping overlord Kubernetes client");
            client.getClient().close();
          }
        }
    );

    return client;
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

  private void configureTaskLogs(Binder binder)
  {
    PolyBind.createChoice(binder, "druid.indexer.logs.type", Key.get(TaskLogs.class), Key.get(FileTaskLogs.class));
    JsonConfigProvider.bind(binder, "druid.indexer.logs", FileTaskLogsConfig.class);

    final MapBinder<String, TaskLogs> taskLogBinder = Binders.taskLogsBinder(binder);
    taskLogBinder.addBinding("noop").to(NoopTaskLogs.class).in(LazySingleton.class);
    taskLogBinder.addBinding("file").to(FileTaskLogs.class).in(LazySingleton.class);
    binder.bind(NoopTaskLogs.class).in(LazySingleton.class);
    binder.bind(FileTaskLogs.class).in(LazySingleton.class);

    binder.bind(TaskLogPusher.class).to(TaskLogs.class);
    binder.bind(TaskLogKiller.class).to(TaskLogs.class);
  }

}
