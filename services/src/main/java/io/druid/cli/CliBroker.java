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
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.client.BrokerServerView;
import io.druid.client.CachingClusteredClient;
import io.druid.client.TimelineServerView;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheMonitor;
import io.druid.client.cache.CacheProvider;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.initialization.JettyServerInitializer;
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
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

            binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
            binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);

            binder.bind(Cache.class).toProvider(CacheProvider.class).in(ManageLifecycle.class);
            JsonConfigProvider.bind(binder, "druid.broker.cache", CacheProvider.class);

            binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            DiscoveryModule.register(binder, Self.class);
            MetricsModule.register(binder, CacheMonitor.class);
          }
        }
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
