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
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.client.cache.CacheMonitor;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.BrokerModule;
import io.druid.guice.HttpClientModule;
import io.druid.guice.LifecycleModule;
import io.druid.guice.QueryToolChestModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Self;
import io.druid.server.ClientInfoResource;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.StatusResource;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;

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
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new LifecycleModule(),
        EmitterModule.class,
        HttpClientModule.global(),
        CuratorModule.class,
        new MetricsModule().register(CacheMonitor.class),
        new DiscoveryModule().register(Self.class),
        new ServerModule(),
        new JettyServerModule(new BrokerJettyServerInitializer())
            .addResource(ClientInfoResource.class)
            .addResource(StatusResource.class),
        new QueryableModule(ClientQuerySegmentWalker.class),
        new QueryToolChestModule(),
        new ServerViewModule(),
        new HttpClientModule("druid.broker.http", Client.class),
        new BrokerModule()
    );
  }

  private static class BrokerJettyServerInitializer extends QueryJettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      super.initialize(server, injector);

      final ServletContextHandler resources = new ServletContextHandler(ServletContextHandler.SESSIONS);
      resources.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      resources.addFilter(GuiceFilter.class, "/druid/v2/datasources/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{resources});
      server.setHandler(handlerList);
    }
  }
}
