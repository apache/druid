/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.client.cache.CacheConfig;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.guice.Binders;
import io.druid.guice.CacheModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.NodeTypeConfig;
import io.druid.guice.PolyBind;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.QueryablePeonModule;
import io.druid.guice.annotations.Json;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.lookup.LookupModule;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.OmniDataSegmentArchiver;
import io.druid.segment.loading.OmniDataSegmentKiller;
import io.druid.segment.loading.OmniDataSegmentMover;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import io.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierConfig;
import io.druid.segment.realtime.plumber.CoordinatorBasedSegmentHandoffNotifierFactory;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.coordination.ServerType;
import io.druid.server.http.SegmentListerResource;
import io.druid.server.initialization.jetty.ChatHandlerServerModule;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.metrics.DataSourceTaskIdHolder;
import org.eclipse.jetty.server.Server;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
@Command(
    name = "peon",
    description = "Runs a Peon, this is an individual forked \"task\" used as part of the indexing service. "
                  + "This should rarely, if ever, be used directly."
)
public class CliPeon extends GuiceRunnable
{
  @Arguments(description = "task.json status.json", required = true)
  public List<String> taskAndStatusFile;

  @Option(name = "--nodeType", title = "nodeType", description = "Set the node type to expose on ZK")
  public String nodeType = "indexer-executor";

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
    return ImmutableList.<Module>of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/peon");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(-1);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

            PolyBind.createChoice(
                binder,
                "druid.indexer.task.chathandler.type",
                Key.get(ChatHandlerProvider.class),
                Key.get(ServiceAnnouncingChatHandlerProvider.class)
            );
            final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
                binder, Key.get(ChatHandlerProvider.class)
            );
            handlerProviderBinder.addBinding("announce")
                                 .to(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
            handlerProviderBinder.addBinding("noop")
                                 .to(NoopChatHandlerProvider.class).in(LazySingleton.class);
            binder.bind(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);

            binder.bind(NoopChatHandlerProvider.class).in(LazySingleton.class);

            binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "druid.peon.taskActionClient.retry", RetryPolicyConfig.class);

            configureTaskActionClient(binder);

            binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);

            // Build it to make it bind even if nothing binds to it.
            Binders.dataSegmentKillerBinder(binder);
            binder.bind(DataSegmentKiller.class).to(OmniDataSegmentKiller.class).in(LazySingleton.class);
            Binders.dataSegmentMoverBinder(binder);
            binder.bind(DataSegmentMover.class).to(OmniDataSegmentMover.class).in(LazySingleton.class);
            Binders.dataSegmentArchiverBinder(binder);
            binder.bind(DataSegmentArchiver.class).to(OmniDataSegmentArchiver.class).in(LazySingleton.class);

            binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ExecutorLifecycle.class);
            binder.bind(ExecutorLifecycleConfig.class).toInstance(
                new ExecutorLifecycleConfig()
                    .setTaskFile(new File(taskAndStatusFile.get(0)))
                    .setStatusFile(new File(taskAndStatusFile.get(1)))
            );

            binder.bind(TaskRunner.class).to(ThreadPoolTaskRunner.class);
            binder.bind(QuerySegmentWalker.class).to(ThreadPoolTaskRunner.class);
            binder.bind(ThreadPoolTaskRunner.class).in(ManageLifecycle.class);

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

            // Override the default SegmentLoaderConfig because we don't actually care about the
            // configuration based locations.  This will override them anyway.  This is also stopping
            // configuration of other parameters, but I don't think that's actually a problem.
            // Note, if that is actually not a problem, then that probably means we have the wrong abstraction.
            binder.bind(SegmentLoaderConfig.class)
                  .toInstance(new SegmentLoaderConfig().withLocations(Arrays.<StorageLocationConfig>asList()));
            binder.bind(CoordinatorClient.class).in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(ServerType.fromString(nodeType)));
            LifecycleModule.register(binder, Server.class);
          }

          private void configureTaskActionClient(Binder binder)
          {
            PolyBind.createChoice(
                binder,
                "druid.peon.mode",
                Key.get(TaskActionClientFactory.class),
                Key.get(RemoteTaskActionClientFactory.class)
            );
            final MapBinder<String, TaskActionClientFactory> taskActionBinder = PolyBind.optionBinder(
                binder, Key.get(TaskActionClientFactory.class)
            );
            taskActionBinder.addBinding("local")
                            .to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
            // all of these bindings are so that we can run the peon in local mode
            JsonConfigProvider.bind(binder, "druid.indexer.storage", TaskStorageConfig.class);
            binder.bind(TaskStorage.class).to(HeapMemoryTaskStorage.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(IndexerMetadataStorageCoordinator.class).to(IndexerSQLMetadataStorageCoordinator.class).in(
                LazySingleton.class
            );
            taskActionBinder.addBinding("remote")
                            .to(RemoteTaskActionClientFactory.class).in(LazySingleton.class);

          }

          @Provides
          @LazySingleton
          public Task readTask(@Json ObjectMapper mapper, ExecutorLifecycleConfig config)
          {
            try {
              return mapper.readValue(config.getTaskFile(), Task.class);
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
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
        new ChatHandlerServerModule(properties),
        new LookupModule()
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();
      try {
        final Lifecycle lifecycle = initLifecycle(injector);
        final Thread hook = new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Running shutdown hook");
                lifecycle.stop();
              }
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
        Runtime.getRuntime().removeShutdownHook(hook);
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }
      log.info("Finished peon task");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
