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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.netty.util.SuppressForbidden;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.QueryablePeonModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.HttpShuffleClient;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClient;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycle;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.OmniDataSegmentArchiver;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.apache.druid.segment.loading.OmniDataSegmentMover;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.PeonAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierConfig;
import org.apache.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierFactory;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.ZkCoordinator;
import org.apache.druid.server.http.HistoricalResource;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.initialization.jetty.ChatHandlerServerModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.DataSourceTaskIdHolder;
import org.eclipse.jetty.server.Server;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
@Command(
    name = "peon",
    description = "Runs a Peon, this is an individual forked \"task\" used as part of the indexing service. "
                  + "This should rarely, if ever, be used directly. "
                  + "See https://druid.apache.org/docs/latest/design/peons.html for a description"
)
public class CliPeon extends GuiceRunnable
{
  @SuppressWarnings("WeakerAccess")
  @Arguments(description = "task.json status.json report.json", required = true)
  public List<String> taskAndStatusFile;

  // path to store the task's stdout log
  private String taskLogPath;

  // path to store the task's TaskStatus
  private String taskStatusPath;

  // path to store the task's TaskReport objects
  private String taskReportPath;

  /**
   * Still using --nodeType as the flag for backward compatibility, although the concept is now more precisely called
   * "serverType".
   */
  @Option(name = "--nodeType", title = "nodeType", description = "Set the node type to expose on ZK")
  public String serverType = "indexer-executor";


  /**
   * If set to "true", the peon will bind classes necessary for loading broadcast segments. This is used for
   * queryable tasks, such as streaming ingestion tasks.
   */
  @Option(name = "--loadBroadcastSegments", title = "loadBroadcastSegments", description = "Enable loading of broadcast segments")
  public String loadBroadcastSegments = "false";

  private static final Logger log = new Logger(CliPeon.class);

  @Inject
  private Properties properties;

  public CliPeon()
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
        new JoinableFactoryModule(),
        new Module()
        {
          @SuppressForbidden(reason = "System#out, System#err")
          @Override
          public void configure(Binder binder)
          {
            taskLogPath = taskAndStatusFile.get(0);
            taskStatusPath = taskAndStatusFile.get(1);
            taskReportPath = taskAndStatusFile.get(2);

            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/peon");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));

            JsonConfigProvider.bind(binder, "druid.task.executor", DruidNode.class, Parent.class);

            bindRowIngestionMeters(binder);
            bindChatHandler(binder);
            bindTaskConfigAndClients(binder);
            bindPeonDataSegmentHandlers(binder);

            binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ExecutorLifecycle.class);
            binder.bind(ExecutorLifecycleConfig.class).toInstance(
                new ExecutorLifecycleConfig()
                    .setTaskFile(new File(taskLogPath))
                    .setStatusFile(new File(taskStatusPath))
            );

            binder.bind(TaskReportFileWriter.class)
                  .toInstance(new SingleFileTaskReportFileWriter(new File(taskReportPath)));

            binder.bind(TaskRunner.class).to(SingleTaskBackgroundRunner.class);
            binder.bind(QuerySegmentWalker.class).to(SingleTaskBackgroundRunner.class);
            binder.bind(SingleTaskBackgroundRunner.class).in(ManageLifecycle.class);

            bindRealtimeCache(binder);
            bindCoordinatorHandoffNotiferAndClient(binder);

            binder.bind(AppenderatorsManager.class)
                  .to(PeonAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.fromString(serverType)));
            LifecycleModule.register(binder, Server.class);

            if ("true".equals(loadBroadcastSegments)) {
              binder.bind(SegmentManager.class).in(LazySingleton.class);
              binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
              Jerseys.addResource(binder, HistoricalResource.class);
              LifecycleModule.register(binder, ZkCoordinator.class);
            }
          }

          @Provides
          @LazySingleton
          public Task readTask(@Json ObjectMapper mapper, ExecutorLifecycleConfig config)
          {
            try {
              return mapper.readValue(config.getTaskFile(), Task.class);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.DATA_SOURCE_BINDING)
          public String getDataSourceFromTask(final Task task)
          {
            return task.getDataSource();
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.TASK_ID_BINDING)
          public String getTaskIDFromTask(final Task task)
          {
            return task.getId();
          }
        },
        new QueryablePeonModule(),
        new IndexingServiceFirehoseModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new ChatHandlerServerModule(properties),
        new LookupModule()
    );
  }

  @SuppressForbidden(reason = "System#out, System#err")
  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();
      try {
        final Lifecycle lifecycle = initLifecycle(injector);
        final Thread hook = new Thread(
            () -> {
              log.info("Running shutdown hook");
              lifecycle.stop();
            }
        );
        Runtime.getRuntime().addShutdownHook(hook);
        injector.getInstance(ExecutorLifecycle.class).join();

        // Sanity check to help debug unexpected non-daemon threads
        final Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thread : threadSet) {
          if (!thread.isDaemon() && thread != Thread.currentThread()) {
            log.info("Thread [%s] is non daemon.", thread);
          }
        }

        // Explicitly call lifecycle stop, dont rely on shutdown hook.
        lifecycle.stop();
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        }
        catch (IllegalStateException e) {
          System.err.println("Cannot remove shutdown hook, already shutting down!");
        }
      }
      catch (Throwable t) {
        System.err.println("Error!");
        System.err.println(Throwables.getStackTraceAsString(t));
        System.exit(1);
      }
      System.out.println("Finished peon task");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void bindRowIngestionMeters(Binder binder)
  {
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
  }

  static void bindChatHandler(Binder binder)
  {
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
  }

  static void bindPeonDataSegmentHandlers(Binder binder)
  {
    // Build it to make it bind even if nothing binds to it.
    Binders.dataSegmentKillerBinder(binder);
    binder.bind(DataSegmentKiller.class).to(OmniDataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder);
    binder.bind(DataSegmentMover.class).to(OmniDataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder);
    binder.bind(DataSegmentArchiver.class).to(OmniDataSegmentArchiver.class).in(LazySingleton.class);
  }

  private static void configureTaskActionClient(Binder binder)
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

    binder.bind(NodeRole.class).annotatedWith(Self.class).toInstance(NodeRole.PEON);
  }

  static void bindTaskConfigAndClients(Binder binder)
  {
    binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
    JsonConfigProvider.bind(binder, "druid.indexer.auditlog", TaskAuditLogConfig.class);
    JsonConfigProvider.bind(binder, "druid.peon.taskActionClient.retry", RetryPolicyConfig.class);

    configureTaskActionClient(binder);
    binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
    binder.bind(ShuffleClient.class).to(HttpShuffleClient.class).in(LazySingleton.class);

    binder.bind(new TypeLiteral<IndexTaskClientFactory<ParallelIndexSupervisorTaskClient>>(){})
          .to(ParallelIndexTaskClientFactory.class)
          .in(LazySingleton.class);

    binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);
  }

  static void bindRealtimeCache(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.realtime.cache", CacheConfig.class);
    binder.install(new CacheModule());
  }

  static void bindCoordinatorHandoffNotiferAndClient(Binder binder)
  {
    JsonConfigProvider.bind(
        binder,
        "druid.segment.handoff",
        CoordinatorBasedSegmentHandoffNotifierConfig.class
    );
    binder.bind(SegmentHandoffNotifierFactory.class)
          .to(CoordinatorBasedSegmentHandoffNotifierFactory.class)
          .in(LazySingleton.class);

    binder.bind(CoordinatorClient.class).in(LazySingleton.class);
  }

}
