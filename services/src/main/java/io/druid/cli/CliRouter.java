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
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.guice.http.JettyHttpClientModule;
import io.druid.server.initialization.JettyServerInitializer;
import io.druid.server.router.CoordinatorRuleManager;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.server.router.TieredBrokerHostSelector;
import io.druid.server.router.TieredBrokerSelectorStrategiesProvider;
import io.druid.server.router.TieredBrokerSelectorStrategy;
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
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new JettyHttpClientModule("druid.router.http", Router.class),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/router");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8888);

            JsonConfigProvider.bind(binder, "druid.router", TieredBrokerConfig.class);

            binder.bind(CoordinatorRuleManager.class);
            LifecycleModule.register(binder, CoordinatorRuleManager.class);

            binder.bind(TieredBrokerHostSelector.class).in(ManageLifecycle.class);
            binder.bind(QueryHostFinder.class).in(LazySingleton.class);
            binder.bind(new TypeLiteral<List<TieredBrokerSelectorStrategy>>(){})
                      .toProvider(TieredBrokerSelectorStrategiesProvider.class)
                      .in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(RouterJettyServerInitializer.class).in(LazySingleton.class);

            LifecycleModule.register(binder, Server.class);
            DiscoveryModule.register(binder, Self.class);
          }

          @Provides
          @ManageLifecycle
          public ServerDiscoverySelector getCoordinatorServerDiscoverySelector(
              TieredBrokerConfig config,
              ServerDiscoveryFactory factory

          )
          {
            return factory.createSelector(config.getCoordinatorServiceName());
          }
        }
    );
  }
}
