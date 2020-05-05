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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import io.airlift.airline.Command;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClient;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.overlord.ForkingTaskRunner;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerCuratorCoordinator;
import org.apache.druid.indexing.worker.WorkerTaskMonitor;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.indexing.worker.http.ShuffleResource;
import org.apache.druid.indexing.worker.http.TaskManagementResource;
import org.apache.druid.indexing.worker.http.WorkerResource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.DummyForInjectionAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.timeline.PruneLastCompactionState;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 *
 */
@Command(
    name = "middleManager",
    description = "Runs a Middle Manager, this is a \"task\" node used as part of the remote indexing service, see https://druid.apache.org/docs/latest/design/middlemanager.html for a description"
)
public class CliMiddleManager extends ServerRunnable
{
  private static final Logger log = new Logger(CliMiddleManager.class);

  public CliMiddleManager()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/middlemanager");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8091);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8291);
            binder.bindConstant().annotatedWith(PruneLastCompactionState.class).to(true);

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            binder.bind(TaskRunner.class).to(ForkingTaskRunner.class);
            binder.bind(ForkingTaskRunner.class).in(LazySingleton.class);

            binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
            binder.bind(new TypeLiteral<IndexTaskClientFactory<ParallelIndexSupervisorTaskClient>>() {})
                  .toProvider(Providers.of(null));
            binder.bind(ShuffleClient.class).toProvider(Providers.of(null));
            binder.bind(ChatHandlerProvider.class).toProvider(Providers.of(null));
            PolyBind.createChoice(
                binder,
                "druid.indexer.task.rowIngestionMeters.type",
                Key.get(RowIngestionMetersFactory.class),
                Key.get(DropwizardRowIngestionMetersFactory.class)
            );
            final MapBinder<String, RowIngestionMetersFactory> rowIngestionMetersHandlerProviderBinder =
                PolyBind.optionBinder(binder, Key.get(RowIngestionMetersFactory.class));
            rowIngestionMetersHandlerProviderBinder
                .addBinding("dropwizard")
                .to(DropwizardRowIngestionMetersFactory.class)
                .in(LazySingleton.class);
            binder.bind(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);

            bindWorkerManagementClasses(binder);

            binder.bind(JettyServerInitializer.class)
                  .to(MiddleManagerJettyServerInitializer.class)
                  .in(LazySingleton.class);

            binder.bind(AppenderatorsManager.class)
                  .to(DummyForInjectionAppenderatorsManager.class)
                  .in(LazySingleton.class);

            Jerseys.addResource(binder, ShuffleResource.class);

            LifecycleModule.register(binder, Server.class);

            bindNodeRoleAndAnnouncer(
                binder,
                DiscoverySideEffectsProvider
                    .builder(NodeRole.MIDDLE_MANAGER)
                    .serviceClasses(ImmutableList.of(WorkerNodeService.class))
                    .build()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
          }

          @Provides
          @LazySingleton
          public Worker getWorker(@Self DruidNode node, WorkerConfig config)
          {
            return new Worker(
                node.getServiceScheme(),
                node.getHostAndPortToUse(),
                config.getIp(),
                config.getCapacity(),
                config.getVersion(),
                config.getCategory()
            );
          }

          @Provides
          @LazySingleton
          public WorkerNodeService getWorkerNodeService(WorkerConfig workerConfig)
          {
            return new WorkerNodeService(
                workerConfig.getIp(),
                workerConfig.getCapacity(),
                workerConfig.getVersion(),
                workerConfig.getCategory()
            );
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTaskLogsModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new LookupSerdeModule()
    );
  }

  public static void bindWorkerManagementClasses(Binder binder)
  {
    binder.bind(WorkerTaskMonitor.class).in(ManageLifecycle.class);
    binder.bind(WorkerCuratorCoordinator.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, WorkerTaskMonitor.class);
    Jerseys.addResource(binder, WorkerResource.class);
    Jerseys.addResource(binder, TaskManagementResource.class);
  }
}
