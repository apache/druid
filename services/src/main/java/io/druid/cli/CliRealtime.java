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
import io.druid.guice.DataSegmentPusherModule;
import io.druid.guice.DbConnectorModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.RealtimeModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StorageNodeModule;
import io.druid.segment.realtime.RealtimeManager;
import io.druid.server.StatusResource;
import io.druid.server.initialization.initialization.EmitterModule;
import io.druid.server.initialization.initialization.Initialization;
import io.druid.server.initialization.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;

/**
 */
@Command(
    name = "realtime",
    description = "Runs a realtime node, see https://github.com/metamx/druid/wiki/Realtime for a description"
)
public class CliRealtime extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliRealtime()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
        new LifecycleModule(),
        EmitterModule.class,
        DbConnectorModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        AnnouncerModule.class,
        DruidProcessingModule.class,
        AWSModule.class,
        DataSegmentPusherModule.class,
        new MetricsModule(),
        new ServerModule(),
        new StorageNodeModule("realtime"),
        new JettyServerModule(new QueryJettyServerInitializer())
            .addResource(StatusResource.class),
        new ServerViewModule(),
        new QueryableModule(RealtimeManager.class),
        new QueryRunnerFactoryModule(),
        RealtimeModule.class
    );
  }
}
