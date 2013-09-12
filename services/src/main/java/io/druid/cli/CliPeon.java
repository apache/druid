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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.DataSegmentPullerModule;
import io.druid.guice.DataSegmentPusherModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.IndexingServiceDiscoveryModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.PeonModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StorageNodeModule;
import io.druid.indexing.coordinator.ThreadPoolTaskRunner;
import io.druid.indexing.worker.executor.ChatHandlerResource;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.initialization.LogLevelAdjuster;
import io.druid.server.StatusResource;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.Initialization;
import io.druid.server.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;

import java.io.File;
import java.util.List;

/**
 */
@Command(
    name = "peon",
    description = "Runs a Peon, this is an individual forked \"task\" used as part of the indexing service. "
                  + "This should rarely, if ever, be used directly."
)
public class CliPeon implements Runnable
{
  @Arguments(description = "task.json status.json", required = true)
  public List<String> taskAndStatusFile;

  @Option(name = "--nodeType", title = "nodeType", description = "Set the node type to expose on ZK")
  public String nodeType = "indexer-executor";

  private Injector injector;

  @Inject
  public void configure(Injector injector)
  {
    this.injector = injector;
  }

  private static final Logger log = new Logger(CliPeon.class);

  protected Injector getInjector()
  {
    return Initialization.makeInjectorWithModules(
        injector,
        ImmutableList.of(
            new LifecycleModule(),
            EmitterModule.class,
            HttpClientModule.global(),
            CuratorModule.class,
            new MetricsModule(),
            new ServerModule(),
            new JettyServerModule(new QueryJettyServerInitializer())
                .addResource(StatusResource.class)
                .addResource(ChatHandlerResource.class),
            new DiscoveryModule(),
            new ServerViewModule(),
            new StorageNodeModule(nodeType),
            new DataSegmentPullerModule(),
            new DataSegmentPusherModule(),
            new AnnouncerModule(),
            new DruidProcessingModule(),
            new QueryableModule(ThreadPoolTaskRunner.class),
            new QueryRunnerFactoryModule(),
            new IndexingServiceDiscoveryModule(),
            new AWSModule(),
            new PeonModule(
                new ExecutorLifecycleConfig()
                    .setTaskFile(new File(taskAndStatusFile.get(0)))
                    .setStatusFile(new File(taskAndStatusFile.get(1)))
            )
        )
    );
  }

  @Override
  public void run()
  {
    try {
      LogLevelAdjuster.register();

      final Injector injector = getInjector();
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

      try {
        lifecycle.start();
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
