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
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.curator.discovery.DiscoveryModule;
import com.metamx.druid.guice.AWSModule;
import com.metamx.druid.guice.AnnouncerModule;
import com.metamx.druid.guice.DataSegmentPusherModule;
import com.metamx.druid.guice.DruidProcessingModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.IndexingServiceDiscoveryModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.PeonModule;
import com.metamx.druid.guice.QueryRunnerFactoryModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.ServerViewModule;
import com.metamx.druid.guice.StorageNodeModule;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.indexing.coordinator.ThreadPoolTaskRunner;
import com.metamx.druid.indexing.worker.executor.ChatHandlerResource;
import com.metamx.druid.indexing.worker.executor.ExecutorLifecycle;
import com.metamx.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

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

  private static final Logger log = new Logger(CliPeon.class);

  protected Injector getInjector()
  {
    return Initialization.makeInjector(
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
