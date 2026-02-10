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
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.netty.util.SuppressForbidden;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ManageLifecycleServer;
import org.apache.druid.guice.PeonProcessingModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.QueryablePeonModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.guice.annotations.AttemptId;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.HadoopIndexTaskModule;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReportFileWriter;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.DeepStorageShuffleClient;
import org.apache.druid.indexing.common.task.batch.parallel.HttpShuffleClient;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProviderImpl;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycle;
import org.apache.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import org.apache.druid.indexing.worker.shuffle.DeepStorageIntermediaryDataManager;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.indexing.worker.shuffle.LocalIntermediaryDataManager;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.PeonMemoryManagementModule;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.handoff.CoordinatorBasedSegmentHandoffNotifierConfig;
import org.apache.druid.segment.handoff.CoordinatorBasedSegmentHandoffNotifierFactory;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.OmniDataSegmentArchiver;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.apache.druid.segment.loading.OmniDataSegmentMover;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.ServiceAnnouncingChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.PeonAppenderatorsManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.coordination.SegmentCacheBootstrapper;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.ZkCoordinator;
import org.apache.druid.server.http.HistoricalResource;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.initialization.jetty.ChatHandlerServerModule;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.ServiceStatusMonitor;
import org.apache.druid.storage.local.LocalTmpStorageConfig;
import org.apache.druid.tasklogs.TaskPayloadManager;
import org.eclipse.jetty.server.Server;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
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
  @Required
  @Arguments(description = "taskDirPath attemptId")
  public List<String> taskAndStatusFile;

  // path to the task Directory
  private String taskDirPath;

  // the attemptId
  private String attemptId;

  /**
   * Still using --nodeType as the flag for backward compatibility, although the concept is now more precisely called
   * "serverType".
   */
  @Option(name = "--nodeType", title = "nodeType", description = "Set the node type to expose on ZK")
  public String serverType = "indexer-executor";

  private boolean isZkEnabled = true;

  /**
   * <p> This option is deprecated, see {@link #loadBroadcastDatasourcesMode} option. </p>
   *
   * If set to "true", the peon will bind classes necessary for loading broadcast segments. This is used for
   * queryable tasks, such as streaming ingestion tasks.
   *
   */
  @Deprecated
  @Option(name = "--loadBroadcastSegments", title = "loadBroadcastSegments",
      description = "Enable loading of broadcast segments. This option is deprecated and will be removed in a"
                    + " future release. Use --loadBroadcastDatasourceMode instead.")
  public String loadBroadcastSegments = "false";

  /**
   * Broadcast datasource loading mode. The peon will bind classes necessary required for loading broadcast segments if
   * the mode is {@link BroadcastDatasourceLoadingSpec.Mode#ALL} or {@link BroadcastDatasourceLoadingSpec.Mode#ONLY_REQUIRED}.
   */
  @Option(name = "--loadBroadcastDatasourceMode", title = "loadBroadcastDatasourceMode",
      description = "Specify the broadcast datasource loading mode for the peon. Supported values are ALL, NONE, ONLY_REQUIRED.")
  public String loadBroadcastDatasourcesMode = BroadcastDatasourceLoadingSpec.Mode.ALL.toString();

  @Option(name = "--taskId", title = "taskId", description = "TaskId for fetching task.json remotely")
  public String taskId = "";

  private static final Logger log = new Logger(CliPeon.class);

  private Properties properties;

  public CliPeon()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    isZkEnabled = ZkEnablementConfig.isEnabled(properties);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new PeonProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new IndexingServiceTaskLogsModule(properties),
        new Module()
        {
          @SuppressForbidden(reason = "System#out, System#err")
          @Override
          public void configure(Binder binder)
          {
            ServerRunnable.validateCentralizedDatasourceSchemaConfig(getProperties());
            Preconditions.checkArgument(
                taskAndStatusFile.size() >= 2,
                "taskAndStatusFile array should contain 2 or more elements. Current array elements: [%s]",
                taskAndStatusFile.toString()
            );

            taskDirPath = taskAndStatusFile.get(0);
            attemptId = taskAndStatusFile.get(1);

            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/peon");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));
            binder.bindConstant().annotatedWith(AttemptId.class).to(attemptId);

            JsonConfigProvider.bind(binder, "druid.task.executor", DruidNode.class, Parent.class);

            bindRowIngestionMeters(binder);
            bindChatHandler(binder);
            configureIntermediaryData(binder);
            bindTaskConfigAndClients(binder);
            bindPeonDataSegmentHandlers(binder);

            binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ExecutorLifecycle.class);
            ExecutorLifecycleConfig executorLifecycleConfig = new ExecutorLifecycleConfig()
                .setTaskFile(Paths.get(taskDirPath, "task.json").toFile())
                .setStatusFile(Paths.get(taskDirPath, "attempt", attemptId, "status.json").toFile());

            binder.bind(TaskHolder.class).to(PeonTaskHolder.class).in(LazySingleton.class);
            binder.bind(LoadSpecHolder.class).to(PeonLoadSpecHolder.class).in(LazySingleton.class);

            binder.bind(Properties.class).toInstance(properties);
            if (properties.getProperty("druid.indexer.runner.type", "").contains("k8s")) {
              log.info("Running peon in k8s mode");
              executorLifecycleConfig.setParentStreamDefined(false);
            }

            binder.bind(ExecutorLifecycleConfig.class).toInstance(executorLifecycleConfig);

            binder.bind(TaskReportFileWriter.class)
                  .toInstance(
                      new SingleFileTaskReportFileWriter(
                          Paths.get(taskDirPath, "attempt", attemptId, "report.json").toFile()
                      ));

            binder.bind(TaskRunner.class).to(SingleTaskBackgroundRunner.class);
            binder.bind(QuerySegmentWalker.class).to(SingleTaskBackgroundRunner.class);
            // Bind to ManageLifecycleServer to ensure SingleTaskBackgroundRunner is closed before
            // its dependent services, such as DiscoveryServiceLocator and OverlordClient.
            // This order ensures that tasks can finalize their cleanup operations before service location closure.
            binder.bind(SingleTaskBackgroundRunner.class).in(ManageLifecycleServer.class);

            bindRealtimeCache(binder);
            bindCoordinatorHandoffNotifer(binder);

            binder.bind(AppenderatorsManager.class)
                  .to(PeonAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.fromString(serverType)));
            LifecycleModule.register(binder, Server.class);

            final BroadcastDatasourceLoadingSpec.Mode mode =
                BroadcastDatasourceLoadingSpec.Mode.valueOf(loadBroadcastDatasourcesMode);
            if ("true".equals(loadBroadcastSegments)
                || mode == BroadcastDatasourceLoadingSpec.Mode.ALL
                || mode == BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED) {
              binder.install(new BroadcastSegmentLoadingModule());
            }
          }

          @Provides
          @LazySingleton
          @Named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING)
          public Supplier<Map<String, Object>> heartbeatDimensions(Task task)
          {
            return () -> CliPeon.heartbeatDimensions(task);
          }

          @Provides
          @LazySingleton
          public Task readTask(@Json ObjectMapper mapper, ExecutorLifecycleConfig config, TaskPayloadManager taskPayloadManager)
          {
            try {
              if (!config.getTaskFile().exists() || config.getTaskFile().length() == 0) {
                log.info("Task file not found, trying to pull task payload from deep storage");
                String task = IOUtils.toString(taskPayloadManager.streamTaskPayload(taskId).get(), Charset.defaultCharset());
                // write the remote task.json to the task file location for ExecutorLifecycle to pickup
                FileUtils.write(config.getTaskFile(), task, Charset.defaultCharset());
              }
              return mapper.readValue(config.getTaskFile(), Task.class);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Provides
          @LazySingleton
          public LocalTmpStorageConfig getLocalTmpStorage()
          {
            File tmpDir = new File(taskDirPath, "tmp");
            try {
              org.apache.druid.java.util.common.FileUtils.mkdirp(tmpDir);
            }
            catch (IOException e) {
              log.error("Failed to create tmp directory for the task");
              throw new RuntimeException(e);
            }
            return () -> tmpDir;
          }
        },
        new QueryablePeonModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new HadoopIndexTaskModule(),
        new ChatHandlerServerModule(properties),
        new LookupModule(),
        new MSQIndexingModule(),
        new MSQDurableStorageModule(),
        new MSQExternalDataSourceModule(),
        new PeonMemoryManagementModule()
    );
  }

  @SuppressForbidden(reason = "System#out, System#err")
  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector(ImmutableSet.of(NodeRole.PEON));
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
    bindDataSegmentKiller(binder);
    Binders.dataSegmentMoverBinder(binder);
    binder.bind(DataSegmentMover.class).to(OmniDataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder);
    binder.bind(DataSegmentArchiver.class).to(OmniDataSegmentArchiver.class).in(LazySingleton.class);
  }

  static void bindDataSegmentKiller(Binder binder)
  {
    Binders.dataSegmentKillerBinder(binder);
    binder.bind(DataSegmentKiller.class).to(OmniDataSegmentKiller.class).in(LazySingleton.class);
  }

  private static void configureTaskActionClient(Binder binder)
  {
    binder.bind(TaskActionClientFactory.class)
          .to(RemoteTaskActionClientFactory.class)
          .in(LazySingleton.class);

    binder.bind(NodeRole.class).annotatedWith(Self.class).toInstance(NodeRole.PEON);
  }

  static void bindTaskConfigAndClients(Binder binder)
  {
    binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
    JsonConfigProvider.bind(binder, "druid.peon.taskActionClient.retry", RetryPolicyConfig.class);

    configureTaskActionClient(binder);

    binder.bind(ParallelIndexSupervisorTaskClientProvider.class)
          .to(ParallelIndexSupervisorTaskClientProviderImpl.class)
          .in(LazySingleton.class);

    binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);
  }

  static void bindRealtimeCache(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.realtime.cache", CacheConfig.class);
    binder.install(new CacheModule());
  }

  static void bindCoordinatorHandoffNotifer(Binder binder)
  {
    JsonConfigProvider.bind(
        binder,
        "druid.segment.handoff",
        CoordinatorBasedSegmentHandoffNotifierConfig.class
    );
    binder.bind(SegmentHandoffNotifierFactory.class)
          .to(CoordinatorBasedSegmentHandoffNotifierFactory.class)
          .in(LazySingleton.class);
  }

  static void configureIntermediaryData(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        "druid.processing.intermediaryData.storage.type",
        Key.get(IntermediaryDataManager.class),
        Key.get(LocalIntermediaryDataManager.class)
    );
    final MapBinder<String, IntermediaryDataManager> intermediaryDataManagerBiddy = PolyBind.optionBinder(
        binder,
        Key.get(IntermediaryDataManager.class)
    );
    intermediaryDataManagerBiddy.addBinding("local").to(LocalIntermediaryDataManager.class).in(LazySingleton.class);
    intermediaryDataManagerBiddy.addBinding("deepstore").to(DeepStorageIntermediaryDataManager.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        "druid.processing.intermediaryData.storage.type",
        Key.get(ShuffleClient.class),
        Key.get(HttpShuffleClient.class)
    );
    final MapBinder<String, ShuffleClient> shuffleClientBiddy = PolyBind.optionBinder(
        binder,
        Key.get(ShuffleClient.class)
    );
    shuffleClientBiddy.addBinding("local").to(HttpShuffleClient.class).in(LazySingleton.class);
    shuffleClientBiddy.addBinding("deepstore").to(DeepStorageShuffleClient.class).in(LazySingleton.class);
  }

  static Map<String, Object> heartbeatDimensions(Task task)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(DruidMetrics.TASK_ID, task.getId());
    builder.put(DruidMetrics.DATASOURCE, task.getDataSource());
    builder.put(DruidMetrics.TASK_TYPE, task.getType());
    builder.put(DruidMetrics.GROUP_ID, task.getGroupId());
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
    if (tags != null && !tags.isEmpty()) {
      builder.put(DruidMetrics.TAGS, tags);
    }

    if (task instanceof SeekableStreamIndexTask) {
      SeekableStreamIndexTask streamingTask = (SeekableStreamIndexTask) task;
      String status = streamingTask.getCurrentRunnerStatus();
      if (status != null) {
        builder.put(DruidMetrics.STATUS, status);
      }
    }

    return builder.build();
  }

  public class BroadcastSegmentLoadingModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(SegmentManager.class).in(LazySingleton.class);
      binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
      Jerseys.addResource(binder, HistoricalResource.class);

      if (isZkEnabled) {
        LifecycleModule.register(binder, ZkCoordinator.class);
      }
      LifecycleModule.register(binder, SegmentCacheBootstrapper.class);
    }

    @Provides
    @LazySingleton
    public List<StorageLocation> getCliPeonStorageLocations(TaskConfig config)
    {
      File broadcastStorage = new File(new File(taskDirPath, "broadcast"), "segments");

      return ImmutableList.of(new StorageLocation(broadcastStorage, config.getTmpStorageBytesPerTask(), null));
    }
  }
}
