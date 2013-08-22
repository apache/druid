/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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
import com.metamx.druid.DruidProcessingConfig;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.guice.AnnouncerModule;
import com.metamx.druid.guice.DataSegmentPusherModule;
import com.metamx.druid.guice.DbConnectorModule;
import com.metamx.druid.guice.DruidProcessingModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.QueryRunnerFactoryModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.RealtimeModule;
import com.metamx.druid.guice.S3Module;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.ServerViewModule;
import com.metamx.druid.guice.StorageNodeModule;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.metrics.MetricsModule;
import com.metamx.druid.realtime.RealtimeManager;
import io.airlift.command.Command;

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
        S3Module.class,
        DataSegmentPusherModule.class,
        new MetricsModule(),
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
