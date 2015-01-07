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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.util.Providers;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.client.indexing.IndexingServiceSelectorConfig;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.guice.IndexingServiceModuleHelper;
import io.druid.guice.IndexingServiceTaskLogsModule;
import io.druid.guice.JacksonConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ListProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.tasklogs.SwitchingTaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskRunnerTaskLogStreamer;
import io.druid.indexing.overlord.ForkingTaskRunnerFactory;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.MetadataTaskStorage;
import io.druid.indexing.overlord.RemoteTaskRunnerFactory;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerFactory;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerFactoryImpl;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.SimpleResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.SimpleResourceManagementStrategy;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.http.OverlordRedirectInfo;
import io.druid.indexing.overlord.http.OverlordResource;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;
import io.druid.server.initialization.BaseJettyServerInitializer;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.tasklogs.TaskLogs;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.ResourceCollection;

import java.util.List;

/**
 */
@Command(
    name = "overlord",
    description = "Runs an Overlord node, see http://druid.io/docs/latest/Indexing-Service.html for a description"
)
public class CliOverlord extends ServerRunnable
{
  private static Logger log = new Logger(CliOverlord.class);

  public CliOverlord()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant()
                  .annotatedWith(Names.named("serviceName"))
                  .to(IndexingServiceSelectorConfig.DEFAULT_SERVICE_NAME);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8090);

            JsonConfigProvider.bind(binder, "druid.indexer.queue", TaskQueueConfig.class);
            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);

            binder.bind(TaskMaster.class).in(ManageLifecycle.class);

            binder.bind(TaskLogStreamer.class).to(SwitchingTaskLogStreamer.class).in(LazySingleton.class);
            binder.bind(
                new TypeLiteral<List<TaskLogStreamer>>()
                {
                }
            )
                  .toProvider(
                      new ListProvider<TaskLogStreamer>()
                          .add(TaskRunnerTaskLogStreamer.class)
                          .add(TaskLogs.class)
                  )
                  .in(LazySingleton.class);

            binder.bind(TaskActionClientFactory.class).to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(TaskLockbox.class).in(LazySingleton.class);
            binder.bind(TaskStorageQueryAdapter.class).in(LazySingleton.class);
            binder.bind(ResourceManagementSchedulerFactory.class)
                  .to(ResourceManagementSchedulerFactoryImpl.class)
                  .in(LazySingleton.class);

            binder.bind(ChatHandlerProvider.class).toProvider(Providers.<ChatHandlerProvider>of(null));

            configureTaskStorage(binder);
            configureRunners(binder);
            configureAutoscale(binder);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).toInstance(new OverlordJettyServerInitializer());
            Jerseys.addResource(binder, OverlordResource.class);

            LifecycleModule.register(binder, Server.class);
          }

          private void configureTaskStorage(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.storage", TaskStorageConfig.class);

            PolyBind.createChoice(
                binder, "druid.indexer.storage.type", Key.get(TaskStorage.class), Key.get(HeapMemoryTaskStorage.class)
            );
            final MapBinder<String, TaskStorage> storageBinder = PolyBind.optionBinder(
                binder,
                Key.get(TaskStorage.class)
            );

            storageBinder.addBinding("local").to(HeapMemoryTaskStorage.class);
            binder.bind(HeapMemoryTaskStorage.class).in(LazySingleton.class);

            storageBinder.addBinding("metadata").to(MetadataTaskStorage.class).in(ManageLifecycle.class);
            binder.bind(MetadataTaskStorage.class).in(LazySingleton.class);
          }

          private void configureRunners(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.runner.type",
                Key.get(TaskRunnerFactory.class),
                Key.get(ForkingTaskRunnerFactory.class)
            );
            final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(
                binder,
                Key.get(TaskRunnerFactory.class)
            );

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
            biddy.addBinding("local").to(ForkingTaskRunnerFactory.class);
            binder.bind(ForkingTaskRunnerFactory.class).in(LazySingleton.class);

            biddy.addBinding("remote").to(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
            binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);

            JacksonConfigProvider.bind(binder, WorkerSetupData.CONFIG_KEY, WorkerSetupData.class, null);
            JacksonConfigProvider.bind(binder, WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class, null);
          }

          private void configureAutoscale(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ResourceManagementSchedulerConfig.class);
            binder.bind(ResourceManagementStrategy.class)
                  .to(SimpleResourceManagementStrategy.class)
                  .in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleResourceManagementConfig.class);
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule()
    );
  }

  /**
   */
  private static class OverlordJettyServerInitializer extends BaseJettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

      ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

      root.addServlet(holderPwd, "/");
      root.setBaseResource(
          new ResourceCollection(
              new String[]{
                  TaskMaster.class.getClassLoader().getResource("static").toExternalForm(),
                  TaskMaster.class.getClassLoader().getResource("indexer_static").toExternalForm()
              }
          )
      );
      root.addFilter(defaultGzipFilterHolder(), "/*", null);

      // /status should not redirect, so add first
      root.addFilter(GuiceFilter.class, "/status/*", null);

      // redirect anything other than status to the current lead
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);

      // Can't use /* here because of Guice and Jetty static content conflicts
      root.addFilter(GuiceFilter.class, "/druid/*", null);

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});

      server.setHandler(handlerList);
    }
  }
}
