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

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.logger.Logger;
import druid.examples.flights.FlightsFirehoseFactory;
import druid.examples.rand.RandomFirehoseFactory;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import druid.examples.web.WebFirehoseFactory;
import io.airlift.command.Command;
import io.druid.guice.IndexingServiceModuleHelper;
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
import io.druid.indexing.common.index.EventReceiverFirehoseFactory;
import io.druid.indexing.common.index.StaticS3FirehoseFactory;
import io.druid.indexing.common.tasklogs.SwitchingTaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskLogs;
import io.druid.indexing.common.tasklogs.TaskRunnerTaskLogStreamer;
import io.druid.indexing.coordinator.DbTaskStorage;
import io.druid.indexing.coordinator.ForkingTaskRunnerFactory;
import io.druid.indexing.coordinator.HeapMemoryTaskStorage;
import io.druid.indexing.coordinator.IndexerDBCoordinator;
import io.druid.indexing.coordinator.RemoteTaskRunnerFactory;
import io.druid.indexing.coordinator.TaskLockbox;
import io.druid.indexing.coordinator.TaskMaster;
import io.druid.indexing.coordinator.TaskQueue;
import io.druid.indexing.coordinator.TaskRunnerFactory;
import io.druid.indexing.coordinator.TaskStorage;
import io.druid.indexing.coordinator.TaskStorageQueryAdapter;
import io.druid.indexing.coordinator.http.IndexerCoordinatorResource;
import io.druid.indexing.coordinator.http.OldIndexerCoordinatorResource;
import io.druid.indexing.coordinator.http.OverlordRedirectInfo;
import io.druid.indexing.coordinator.scaling.AutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.EC2AutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.NoopAutoScalingStrategy;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerFactory;
import io.druid.indexing.coordinator.scaling.ResourceManagementSchedulerFactoryImpl;
import io.druid.indexing.coordinator.scaling.ResourceManagementStrategy;
import io.druid.indexing.coordinator.scaling.SimpleResourceManagementConfig;
import io.druid.indexing.coordinator.scaling.SimpleResourceManagementStrategy;
import io.druid.indexing.coordinator.setup.WorkerSetupData;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.IrcFirehoseFactory;
import io.druid.segment.realtime.firehose.KafkaFirehoseFactory;
import io.druid.segment.realtime.firehose.RabbitMQFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.resource.ResourceCollection;

import java.util.Arrays;
import java.util.List;

/**
 */
@Command(
    name = "overlord",
    description = "Runs an Overlord node, see https://github.com/metamx/druid/wiki/Indexing-Service for a description"
)
public class CliOverlord extends ServerRunnable
{
  private static Logger log = new Logger(CliOverlord.class);

  public CliOverlord()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new DruidModule()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TaskMaster.class).in(ManageLifecycle.class);

            binder.bind(TaskLogStreamer.class).to(SwitchingTaskLogStreamer.class).in(LazySingleton.class);
            binder.bind(new TypeLiteral<List<TaskLogStreamer>>(){})
                  .toProvider(
                      new ListProvider<TaskLogStreamer>()
                          .add(TaskRunnerTaskLogStreamer.class)
                          .add(TaskLogs.class)
                  )
                  .in(LazySingleton.class);

            binder.bind(TaskActionClientFactory.class).to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(TaskQueue.class).in(LazySingleton.class); // Lifecycle managed by TaskMaster instead
            binder.bind(IndexerDBCoordinator.class).in(LazySingleton.class);
            binder.bind(TaskLockbox.class).in(LazySingleton.class);
            binder.bind(TaskStorageQueryAdapter.class).in(LazySingleton.class);
            binder.bind(ResourceManagementSchedulerFactory.class)
                  .to(ResourceManagementSchedulerFactoryImpl.class)
                  .in(LazySingleton.class);

            configureTaskStorage(binder);
            configureRunners(binder);
            configureAutoscale(binder);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).toInstance(new OverlordJettyServerInitializer());
            Jerseys.addResource(binder, IndexerCoordinatorResource.class);
            Jerseys.addResource(binder, OldIndexerCoordinatorResource.class);

            LifecycleModule.register(binder, Server.class);
          }

          private void configureTaskStorage(Binder binder)
          {
            PolyBind.createChoice(
                binder, "druid.indexer.storage.type", Key.get(TaskStorage.class), Key.get(HeapMemoryTaskStorage.class)
            );
            final MapBinder<String, TaskStorage> storageBinder = PolyBind.optionBinder(binder, Key.get(TaskStorage.class));

            storageBinder.addBinding("local").to(HeapMemoryTaskStorage.class);
            binder.bind(HeapMemoryTaskStorage.class).in(LazySingleton.class);

            storageBinder.addBinding("db").to(DbTaskStorage.class);
            binder.bind(DbTaskStorage.class).in(LazySingleton.class);
          }

          private void configureRunners(Binder binder)
          {
            PolyBind.createChoice(
                binder, "druid.indexer.runner.type", Key.get(TaskRunnerFactory.class), Key.get(ForkingTaskRunnerFactory.class)
            );
            final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(binder, Key.get(TaskRunnerFactory.class));

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
            biddy.addBinding("local").to(ForkingTaskRunnerFactory.class);
            binder.bind(ForkingTaskRunnerFactory.class).in(LazySingleton.class);

            biddy.addBinding("remote").to(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
            binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
          }

          private void configureAutoscale(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ResourceManagementSchedulerConfig.class);
            binder.bind(ResourceManagementStrategy.class).to(SimpleResourceManagementStrategy.class).in(LazySingleton.class);

            JacksonConfigProvider.bind(binder, WorkerSetupData.CONFIG_KEY, WorkerSetupData.class, null);

            PolyBind.createChoice(
                binder,
                "druid.indexer.autoscale.strategy",
                Key.get(AutoScalingStrategy.class),
                Key.get(NoopAutoScalingStrategy.class)
            );

            final MapBinder<String, AutoScalingStrategy> autoScalingBinder = PolyBind.optionBinder(
                binder, Key.get(AutoScalingStrategy.class)
            );
            autoScalingBinder.addBinding("ec2").to(EC2AutoScalingStrategy.class);
            binder.bind(EC2AutoScalingStrategy.class).in(LazySingleton.class);

            autoScalingBinder.addBinding("noop").to(NoopAutoScalingStrategy.class);
            binder.bind(NoopAutoScalingStrategy.class).in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleResourceManagementConfig.class);
          }

          @Override
          public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
          {
            return Arrays.<com.fasterxml.jackson.databind.Module>asList(
                new SimpleModule("RealtimeModule")
                    .registerSubtypes(
                        new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
                        new NamedType(FlightsFirehoseFactory.class, "flights"),
                        new NamedType(RandomFirehoseFactory.class, "rand"),
                        new NamedType(WebFirehoseFactory.class, "webstream"),
                        new NamedType(KafkaFirehoseFactory.class, "kafka-0.7.2"),
                        new NamedType(RabbitMQFirehoseFactory.class, "rabbitmq"),
                        new NamedType(ClippedFirehoseFactory.class, "clipped"),
                        new NamedType(TimedShutoffFirehoseFactory.class, "timed"),
                        new NamedType(IrcFirehoseFactory.class, "irc"),
                        new NamedType(StaticS3FirehoseFactory.class, "s3"),
                        new NamedType(EventReceiverFirehoseFactory.class, "receiver")
                    )
            );
          }
        }
    );
  }

  /**
  */
  private static class OverlordJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setBaseResource(
          new ResourceCollection(
              new String[]{
                  TaskMaster.class.getClassLoader().getResource("static").toExternalForm(),
                  TaskMaster.class.getClassLoader().getResource("indexer_static").toExternalForm()
              }
          )
      );

      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.setContextPath("/");

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{resourceHandler, root, new DefaultHandler()});
      server.setHandler(handlerList);

      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);
    }
  }
}
