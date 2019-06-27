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
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.NodeTypeConfig;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.QueryablePeonModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskClient;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskClientFactory;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.ThreadingTaskRunner;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerCuratorCoordinator;
import org.apache.druid.indexing.worker.WorkerTaskMonitor;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.indexing.worker.http.TaskManagementResource;
import org.apache.druid.indexing.worker.http.WorkerResource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.OmniDataSegmentArchiver;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.apache.druid.segment.loading.OmniDataSegmentMover;
import org.apache.druid.segment.realtime.UnifiedIndexerLifecycleHandler;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierConfig;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierFactory;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.initialization.jetty.ChatHandlerServerModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;

/**
 *
 */
@Command(
    name = "indexer",
    description = "Runs an Indexer. Description TBD."
)
public class CliIndexer extends ServerRunnable
{
  private static final Logger log = new Logger(CliIndexer.class);

  @Inject
  private Properties properties;

  public CliIndexer()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/indexer");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8091);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8291);

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);
            JsonConfigProvider.bind(binder, "druid.indexer.auditlog", TaskAuditLogConfig.class);
            JsonConfigProvider.bind(binder, "druid.peon.taskActionClient.retry", RetryPolicyConfig.class);

            binder.bind(TaskReportFileWriter.class).toInstance(new TaskReportFileWriter());

            binder.bind(TaskRunner.class).to(ThreadingTaskRunner.class);
            binder.bind(QuerySegmentWalker.class).to(ThreadingTaskRunner.class);
            binder.bind(ThreadingTaskRunner.class).in(LazySingleton.class);

            binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

            configureTaskActionClient(binder);
            binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
            binder.bind(new TypeLiteral<IndexTaskClientFactory<ParallelIndexTaskClient>>(){})
                  .to(ParallelIndexTaskClientFactory.class)
                  .in(LazySingleton.class);

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

            PolyBind.createChoice(
                binder,
                "druid.indexer.task.chathandler.type",
                Key.get(ChatHandlerProvider.class),
                Key.get(ServiceAnnouncingChatHandlerProvider.class)
            );
            final MapBinder<String, ChatHandlerProvider> handlerProviderBinder =
                PolyBind.optionBinder(binder, Key.get(ChatHandlerProvider.class));
            handlerProviderBinder
                .addBinding("announce")
                .to(ServiceAnnouncingChatHandlerProvider.class)
                .in(LazySingleton.class);
            handlerProviderBinder
                .addBinding("noop")
                .to(NoopChatHandlerProvider.class)
                .in(LazySingleton.class);
            binder.bind(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
            binder.bind(NoopChatHandlerProvider.class).in(LazySingleton.class);

            binder.bind(WorkerTaskMonitor.class).in(ManageLifecycle.class);
            binder.bind(WorkerCuratorCoordinator.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, WorkerTaskMonitor.class);

            // Build it to make it bind even if nothing binds to it.
            Binders.dataSegmentKillerBinder(binder);
            binder.bind(DataSegmentKiller.class).to(OmniDataSegmentKiller.class).in(LazySingleton.class);
            Binders.dataSegmentMoverBinder(binder);
            binder.bind(DataSegmentMover.class).to(OmniDataSegmentMover.class).in(LazySingleton.class);
            Binders.dataSegmentArchiverBinder(binder);
            binder.bind(DataSegmentArchiver.class).to(OmniDataSegmentArchiver.class).in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.realtime.cache", CacheConfig.class);
            binder.install(new CacheModule());

            JsonConfigProvider.bind(
                binder,
                "druid.segment.handoff",
                CoordinatorBasedSegmentHandoffNotifierConfig.class
            );
            binder.bind(SegmentHandoffNotifierFactory.class)
                  .to(CoordinatorBasedSegmentHandoffNotifierFactory.class)
                  .in(LazySingleton.class);

            binder.bind(AppenderatorsManager.class)
                  .to(UnifiedIndexerAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(ServerType.INDEXER_EXECUTOR));

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            Jerseys.addResource(binder, WorkerResource.class);
            Jerseys.addResource(binder, TaskManagementResource.class);

            LifecycleModule.register(binder, Server.class);
            LifecycleModule.register(binder, UnifiedIndexerLifecycleHandler.class);

            bindAnnouncer(
                binder,
                DiscoverySideEffectsProvider.builder(NodeType.INDEXER)
                                            .serviceClasses(
                                                ImmutableList.of(LookupNodeService.class, WorkerNodeService.class)
                                            )
                                            .build()
            );
          }

          private void configureTaskActionClient(Binder binder)
          {
            PolyBind.createChoice(
                binder,
                "druid.peon.mode",
                Key.get(TaskActionClientFactory.class),
                Key.get(RemoteTaskActionClientFactory.class)
            );
            final MapBinder<String, TaskActionClientFactory> taskActionBinder =
                PolyBind.optionBinder(binder, Key.get(TaskActionClientFactory.class));
            taskActionBinder
                .addBinding("local")
                .to(LocalTaskActionClientFactory.class)
                .in(LazySingleton.class);
            // all of these bindings are so that we can run the peon in local mode
            JsonConfigProvider.bind(binder, "druid.indexer.storage", TaskStorageConfig.class);
            binder.bind(TaskStorage.class).to(HeapMemoryTaskStorage.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(IndexerMetadataStorageCoordinator.class)
                  .to(IndexerSQLMetadataStorageCoordinator.class)
                  .in(LazySingleton.class);
            taskActionBinder
                .addBinding("remote")
                .to(RemoteTaskActionClientFactory.class)
                .in(LazySingleton.class);
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
                config.getVersion()
            );
          }

          @Provides
          @LazySingleton
          public WorkerNodeService getWorkerNodeService(WorkerConfig workerConfig)
          {
            return new WorkerNodeService(
                workerConfig.getIp(),
                workerConfig.getCapacity(),
                workerConfig.getVersion()
            );
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule(),
        new QueryablePeonModule(),
        new ChatHandlerServerModule(properties, true),
        new LookupModule()
    );
  }
}
