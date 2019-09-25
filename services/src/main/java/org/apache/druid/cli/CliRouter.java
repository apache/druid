/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.RouterProcessingModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.http.JettyHttpClientModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.server.AsyncQueryForwardingServlet;
import org.apache.druid.server.http.RouterResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.router.AvaticaConnectionBalancer;
import org.apache.druid.server.router.CoordinatorRuleManager;
import org.apache.druid.server.router.ManagementProxyConfig;
import org.apache.druid.server.router.QueryHostFinder;
import org.apache.druid.server.router.Router;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.server.router.TieredBrokerHostSelector;
import org.apache.druid.server.router.TieredBrokerSelectorStrategiesProvider;
import org.apache.druid.server.router.TieredBrokerSelectorStrategy;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "router",
    description = "Experimental! Understands tiers and routes things to different brokers, see https://druid.apache.org/docs/latest/development/router.html for a description"
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
        new RouterProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new JettyHttpClientModule("druid.router.http", Router.class),
        JettyHttpClientModule.global(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/router");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8888);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(9088);

          JsonConfigProvider.bind(binder, "druid.router", TieredBrokerConfig.class);
          JsonConfigProvider.bind(binder, "druid.router.avatica.balancer", AvaticaConnectionBalancer.class);
          JsonConfigProvider.bind(binder, "druid.router.managementProxy", ManagementProxyConfig.class);

          binder.bind(CoordinatorRuleManager.class);
          LifecycleModule.register(binder, CoordinatorRuleManager.class);

          binder.bind(TieredBrokerHostSelector.class).in(ManageLifecycle.class);
          binder.bind(QueryHostFinder.class).in(LazySingleton.class);
          binder.bind(new TypeLiteral<List<TieredBrokerSelectorStrategy>>() {})
                .toProvider(TieredBrokerSelectorStrategiesProvider.class)
                .in(LazySingleton.class);

          binder.bind(QueryCountStatsProvider.class).to(AsyncQueryForwardingServlet.class).in(LazySingleton.class);
          binder.bind(JettyServerInitializer.class).to(RouterJettyServerInitializer.class).in(LazySingleton.class);

          Jerseys.addResource(binder, RouterResource.class);

          LifecycleModule.register(binder, RouterResource.class);
          LifecycleModule.register(binder, Server.class);
          DiscoveryModule.register(binder, Self.class);

          bindAnnouncer(
              binder,
              DiscoverySideEffectsProvider.builder(NodeType.ROUTER).build()
          );
        },
        new LookupSerdeModule()
    );
  }
}
