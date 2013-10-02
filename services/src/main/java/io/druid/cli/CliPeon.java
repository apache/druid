/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.NodeTypeConfig;
import io.druid.guice.PolyBind;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.index.ChatHandlerProvider;
import io.druid.indexing.common.index.NoopChatHandlerProvider;
import io.druid.indexing.common.index.ServiceAnnouncingChatHandlerProvider;
import io.druid.indexing.coordinator.HeapMemoryTaskStorage;
import io.druid.indexing.coordinator.IndexerDBCoordinator;
import io.druid.indexing.coordinator.TaskQueue;
import io.druid.indexing.coordinator.TaskRunner;
import io.druid.indexing.coordinator.TaskStorage;
import io.druid.indexing.coordinator.ThreadPoolTaskRunner;
import io.druid.indexing.worker.executor.ChatHandlerResource;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.S3DataSegmentKiller;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.io.File;
import java.util.Arrays;
import java.util.List;

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

  public CliPeon()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            PolyBind.createChoice(
                binder,
                "druid.indexer.task.chathandler.type",
                Key.get(ChatHandlerProvider.class),
                Key.get(NoopChatHandlerProvider.class)
            );
            final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
                binder, Key.get(ChatHandlerProvider.class)
            );
            handlerProviderBinder.addBinding("announce")
                                 .to(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
            handlerProviderBinder.addBinding("noop")
                                 .to(NoopChatHandlerProvider.class).in(LazySingleton.class);

            binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "druid.worker.taskActionClient.retry", RetryPolicyConfig.class);

            configureTaskActionClient(binder);

            binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);

            binder.bind(DataSegmentKiller.class).to(S3DataSegmentKiller.class).in(LazySingleton.class);

            binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
            binder.bind(ExecutorLifecycleConfig.class).toInstance(
                new ExecutorLifecycleConfig()
                    .setTaskFile(new File(taskAndStatusFile.get(0)))
                    .setStatusFile(new File(taskAndStatusFile.get(1)))
            );

            binder.bind(TaskRunner.class).to(ThreadPoolTaskRunner.class);
            binder.bind(QuerySegmentWalker.class).to(ThreadPoolTaskRunner.class);
            binder.bind(ThreadPoolTaskRunner.class).in(ManageLifecycle.class);

            // Override the default SegmentLoaderConfig because we don't actually care about the
            // configuration based locations.  This will override them anyway.  This is also stopping
            // configuration of other parameters, but I don't think that's actually a problem.
            // Note, if that is actually not a problem, then that probably means we have the wrong abstraction.
            binder.bind(SegmentLoaderConfig.class)
                  .toInstance(new SegmentLoaderConfig().withLocations(Arrays.<StorageLocationConfig>asList()));

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, ChatHandlerResource.class);
            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(nodeType));

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
            binder.bind(TaskStorage.class).to(HeapMemoryTaskStorage.class).in(LazySingleton.class);
            binder.bind(TaskQueue.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(IndexerDBCoordinator.class).in(LazySingleton.class);
            taskActionBinder.addBinding("remote")
                            .to(RemoteTaskActionClientFactory.class).in(LazySingleton.class);

          }
        }
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();

      try {
        Lifecycle lifecycle = initLifecycle(injector);

        injector.getInstance(ExecutorLifecycle.class).join();
        lifecycle.stop();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
