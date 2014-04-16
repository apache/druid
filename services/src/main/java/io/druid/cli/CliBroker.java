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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.client.BrokerServerView;
import io.druid.client.CachingClusteredClient;
import io.druid.client.TimelineServerView;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CacheMonitor;
import io.druid.client.cache.CacheProvider;
import io.druid.client.selector.ServerSelectorStrategy;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.ClientInfoResource;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.QueryResource;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.metrics.MetricsModule;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "broker",
    description = "Runs a broker node, see http://druid.io/docs/latest/Broker.html for a description"
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
            JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
            JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);

            binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            Jerseys.addResource(binder, QueryResource.class);
            Jerseys.addResource(binder, ClientInfoResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            DiscoveryModule.register(binder, Self.class);
            MetricsModule.register(binder, CacheMonitor.class);

            LifecycleModule.register(binder, Server.class);
          }
        }
    );
  }
}
