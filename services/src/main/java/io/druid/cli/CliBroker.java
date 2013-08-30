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
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;
import io.druid.client.cache.CacheMonitor;
import io.druid.curator.CuratorModule;
import io.druid.guice.QueryToolChestModule;
import io.druid.guice.guice.BrokerModule;
import io.druid.guice.guice.HttpClientModule;
import io.druid.guice.guice.LifecycleModule;
import io.druid.guice.guice.QueryableModule;
import io.druid.guice.guice.ServerModule;
import io.druid.guice.guice.ServerViewModule;
import io.druid.guice.guice.annotations.Client;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.StatusResource;
import io.druid.server.initialization.initialization.EmitterModule;
import io.druid.server.initialization.initialization.Initialization;
import io.druid.server.initialization.initialization.JettyServerModule;

/**
 */
@Command(
    name = "broker",
    description = "Runs a broker node, see https://github.com/metamx/druid/wiki/Broker for a description"
)
public class CliBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliBroker()
  {
    super(log);
  }

  @Override
  protected Injector getInjector()
  {
    return Initialization.makeInjector(
            new LifecycleModule(),
            EmitterModule.class,
            HttpClientModule.global(),
            CuratorModule.class,
            new MetricsModule().register(CacheMonitor.class),
            ServerModule.class,
            new JettyServerModule(new QueryJettyServerInitializer())
                .addResource(StatusResource.class),
            new QueryableModule(ClientQuerySegmentWalker.class),
            new QueryToolChestModule(),
            new ServerViewModule(),
            new HttpClientModule("druid.broker.http", Client.class),
            new BrokerModule()
    );
  }
}
