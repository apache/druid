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
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.server.BrokerQueryResource;
import org.apache.druid.server.ClientInfoResource;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.http.BrokerResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.timeline.PruneLastCompactionState;
import org.apache.druid.timeline.PruneLoadSpec;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "broker",
    description = "Runs a broker node, see https://druid.apache.org/docs/latest/Broker.html for a description"
)
public class CliBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  public CliBroker()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
          binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true);
          binder.bindConstant().annotatedWith(PruneLastCompactionState.class).to(true);

          binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
          LifecycleModule.register(binder, BrokerServerView.class);
          binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);

          JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
          binder.install(new CacheModule());

          JsonConfigProvider.bind(binder, "druid.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.segment", BrokerSegmentWatcherConfig.class);

          binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

          Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

          LifecycleModule.register(binder, Server.class);


          bindAnnouncer(
              binder,
              DiscoverySideEffectsProvider.builder(NodeType.BROKER)
                                          .serviceClasses(ImmutableList.of(LookupNodeService.class))
                                          .useLegacyAnnouncer(true)
                                          .build()
          );
        },
        new LookupModule(),
        new SqlModule()
    );
  }
}
