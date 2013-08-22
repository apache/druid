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
import com.metamx.druid.client.cache.CacheMonitor;
import com.metamx.druid.curator.CuratorModule;
import com.metamx.druid.guice.BrokerModule;
import com.metamx.druid.guice.HttpClientModule;
import com.metamx.druid.guice.LifecycleModule;
import com.metamx.druid.guice.QueryToolChestModule;
import com.metamx.druid.guice.QueryableModule;
import com.metamx.druid.guice.ServerModule;
import com.metamx.druid.guice.ServerViewModule;
import com.metamx.druid.guice.annotations.Client;
import com.metamx.druid.http.ClientQuerySegmentWalker;
import com.metamx.druid.http.StatusResource;
import com.metamx.druid.initialization.EmitterModule;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.JettyServerModule;
import com.metamx.druid.metrics.MetricsModule;
import io.airlift.command.Command;

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
