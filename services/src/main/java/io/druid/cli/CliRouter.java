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
import com.google.inject.Provides;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.QueryResource;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.router.BrokerSelector;
import io.druid.server.router.CoordinatorRuleManager;
import io.druid.server.router.RouterQuerySegmentWalker;
import io.druid.server.router.TierConfig;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "router",
    description = "Experimental! Understands tiers and routes things to different brokers"
)
public class CliRouter extends ServerRunnable
{
  private static final Logger log = new Logger(CliRouter.class);

  public CliRouter()
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
            JsonConfigProvider.bind(binder, "druid.router", TierConfig.class);

            binder.bind(CoordinatorRuleManager.class);
            LifecycleModule.register(binder, CoordinatorRuleManager.class);

            binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

            binder.bind(BrokerSelector.class).in(ManageLifecycle.class);
            binder.bind(QuerySegmentWalker.class).to(RouterQuerySegmentWalker.class).in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            Jerseys.addResource(binder, QueryResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            LifecycleModule.register(binder, Server.class);
            DiscoveryModule.register(binder, Self.class);
          }

          @Provides
          @ManageLifecycle
          public ServerDiscoverySelector getCoordinatorServerDiscoverySelector(
              TierConfig config,
              ServerDiscoveryFactory factory

          )
          {
            return factory.createSelector(config.getCoordinatorServiceName());
          }
        }
    );
  }
}
