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

import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.curator.CuratorModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.DataSegmentPullerModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.HistoricalModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.StorageNodeModule;
import io.druid.server.StatusResource;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.initialization.initialization.EmitterModule;
import io.druid.server.initialization.initialization.Initialization;
import io.druid.server.initialization.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import io.druid.server.metrics.ServerMonitor;

/**
 */
@Command(
    name = "historical",
    description = "Runs a Historical node, see https://github.com/metamx/druid/wiki/Compute for a description"
)
public class CliHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliHistorical.class);

  public CliHistorical()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule().register(ZkCoordinator.class),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        AnnouncerModule.class,
        DruidProcessingModule.class,
        AWSModule.class,
        DataSegmentPullerModule.class,
        new MetricsModule().register(ServerMonitor.class),
        new ServerModule(),
        new StorageNodeModule("historical"),
        new JettyServerModule(new QueryJettyServerInitializer())
            .addResource(StatusResource.class),
        new QueryableModule(ServerManager.class),
        new QueryRunnerFactoryModule(),
        HistoricalModule.class
    );
  }
}
