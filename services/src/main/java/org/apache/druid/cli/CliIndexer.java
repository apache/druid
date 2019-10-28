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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import io.airlift.airline.Command;
import org.apache.druid.client.DruidServer;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.NodeTypeConfig;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.QueryablePeonModule;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.RemoteChatHandler;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.ThreadingTaskRunner;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.indexing.worker.http.ShuffleResource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.realtime.CliIndexerDataSegmentServerAnnouncerLifecycleHandler;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.initialization.jetty.CliIndexerServerModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;

/**
 *
 */
@Command(
    name = "indexer",
    description = "Runs an Indexer. The Indexer is a task execution process that runs each task in a separate thread."
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

            JsonConfigProvider.bind(binder, "druid", DruidNode.class, Parent.class);
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            CliPeon.bindTaskConfigAndClients(binder);

            binder.bind(TaskReportFileWriter.class).toInstance(new MultipleFileTaskReportFileWriter());

            binder.bind(TaskRunner.class).to(ThreadingTaskRunner.class);
            binder.bind(QuerySegmentWalker.class).to(ThreadingTaskRunner.class);
            binder.bind(ThreadingTaskRunner.class).in(LazySingleton.class);

            CliPeon.bindRowIngestionMeters(binder);

            CliPeon.bindChatHandler(binder);

            CliPeon.bindPeonDataSegmentHandlers(binder);

            CliPeon.bindRealtimeCache(binder);

            CliPeon.bindCoordinatorHandoffNotiferAndClient(binder);

            CliMiddleManager.bindWorkerManagementClasses(binder);

            binder.bind(AppenderatorsManager.class)
                  .to(UnifiedIndexerAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(ServerType.INDEXER_EXECUTOR));

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);

            LifecycleModule.register(binder, CliIndexerDataSegmentServerAnnouncerLifecycleHandler.class);

            Jerseys.addResource(binder, ShuffleResource.class);

            LifecycleModule.register(binder, Server.class, RemoteChatHandler.class);

            binder.bind(SegmentLoadDropHandler.class).toProvider(Providers.of(null));

            bindAnnouncer(
                binder,
                DiscoverySideEffectsProvider.builder(NodeType.INDEXER)
                                            .serviceClasses(
                                                ImmutableList.of(
                                                    LookupNodeService.class,
                                                    WorkerNodeService.class,
                                                    DataNodeService.class
                                                )
                                            )
                                            .build()
            );
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
                WorkerConfig.DEFAULT_CATEGORY
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
                WorkerConfig.DEFAULT_CATEGORY
            );
          }

          @Provides
          @LazySingleton
          public DataNodeService getDataNodeService()
          {
            return new DataNodeService(
                DruidServer.DEFAULT_TIER,
                0L,
                ServerType.INDEXER_EXECUTOR,
                DruidServer.DEFAULT_PRIORITY
            );
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule(),
        new QueryablePeonModule(),
        new CliIndexerServerModule(properties),
        new LookupModule()
    );
  }
}
