/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import io.airlift.airline.Command;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.guice.http.JettyHttpClientModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.LookupModule;
import io.druid.server.initialization.jetty.JettyServerInitializer;
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
    return ImmutableList.of(
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
            binder.bind(new TypeLiteral<List<TieredBrokerSelectorStrategy>>()
            {
            })
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
        },
        new LookupModule()
    );
  }
}
