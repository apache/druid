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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.util.Providers;
import io.airlift.airline.Command;
import io.druid.audit.AuditManager;
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
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerProvisioningConfig;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerProvisioningStrategy;
import io.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningStrategy;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.helpers.OverlordHelper;
import io.druid.indexing.overlord.helpers.TaskLogAutoCleaner;
import io.druid.indexing.overlord.helpers.TaskLogAutoCleanerConfig;
import io.druid.indexing.overlord.http.OverlordRedirectInfo;
import io.druid.indexing.overlord.http.OverlordResource;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.overlord.supervisor.SupervisorResource;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.audit.AuditManagerProvider;
import io.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
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
    return getModules(true);
  }

  protected List<? extends Module> getModules(final boolean standalone)
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            if (standalone) {
              binder.bindConstant()
                    .annotatedWith(Names.named("serviceName"))
                    .to(IndexingServiceSelectorConfig.DEFAULT_SERVICE_NAME);
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8090);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8290);
            }

            JsonConfigProvider.bind(binder, "druid.coordinator.asOverlord", CoordinatorOverlordServiceConfig.class);
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
            binder.bind(SupervisorManager.class).in(LazySingleton.class);

            binder.bind(ChatHandlerProvider.class).toProvider(Providers.<ChatHandlerProvider>of(null));

            configureTaskStorage(binder);
            configureAutoscale(binder);
            configureRunners(binder);
            configureOverlordHelpers(binder);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            if (standalone) {
              binder.bind(RedirectFilter.class).in(LazySingleton.class);
              binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);
              binder.bind(JettyServerInitializer.class).toInstance(new OverlordJettyServerInitializer());
            }

            Jerseys.addResource(binder, OverlordResource.class);
            Jerseys.addResource(binder, SupervisorResource.class);

            if (standalone) {
              LifecycleModule.register(binder, Server.class);
            }
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

            biddy.addBinding(RemoteTaskRunnerFactory.TYPE_NAME).to(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
            binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);

            JacksonConfigProvider.bind(binder, WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class, null);
          }

          private void configureAutoscale(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ProvisioningSchedulerConfig.class);
            JsonConfigProvider.bind(
                binder,
                "druid.indexer.autoscale",
                PendingTaskBasedWorkerProvisioningConfig.class
            );
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleWorkerProvisioningConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.autoscale.strategy.type",
                Key.get(ProvisioningStrategy.class),
                Key.get(SimpleWorkerProvisioningStrategy.class)
            );
            final MapBinder<String, ProvisioningStrategy> biddy = PolyBind.optionBinder(
                binder,
                Key.get(ProvisioningStrategy.class)
            );
            biddy.addBinding("simple").to(SimpleWorkerProvisioningStrategy.class);
            biddy.addBinding("pendingTaskBased").to(PendingTaskBasedWorkerProvisioningStrategy.class);

          }

          private void configureOverlordHelpers(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.logs.kill", TaskLogAutoCleanerConfig.class);
            Multibinder.newSetBinder(binder, OverlordHelper.class)
                       .addBinding()
                       .to(TaskLogAutoCleaner.class);
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule()
    );
  }

  /**
   */
  private static class OverlordJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
      root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
      root.setWelcomeFiles(new String[]{"index.html", "console.html"});

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
      JettyServerInitUtils.addExtensionFilters(root, injector);

      // /status should not redirect, so add first
      root.addFilter(GuiceFilter.class, "/status/*", null);

      // redirect anything other than status to the current lead
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);

      // Can't use /* here because of Guice and Jetty static content conflicts
      root.addFilter(GuiceFilter.class, "/druid/*", null);

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(
          new Handler[]{
              JettyServerInitUtils.getJettyRequestLogHandler(),
              JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
          }
      );

      server.setHandler(handlerList);
    }
  }
}
