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
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.druid.client.BrokerSegmentWatcherConfig;
import io.druid.client.BrokerServerView;
import io.druid.client.CachingClusteredClient;
import io.druid.client.TimelineServerView;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CacheMonitor;
import io.druid.client.selector.CustomTierSelectorStrategyConfig;
import io.druid.client.selector.ServerSelectorStrategy;
import io.druid.client.selector.TierSelectorStrategy;
import io.druid.guice.CacheModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.lookup.LookupModule;
import io.druid.server.BrokerQueryResource;
import io.druid.server.ClientInfoResource;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.coordination.broker.DruidBroker;
import io.druid.server.http.BrokerResource;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.metrics.MetricsModule;
import io.druid.server.metrics.QueryCountStatsProvider;
import io.druid.server.router.TieredBrokerConfig;
import io.druid.sql.guice.SqlModule;
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
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
                TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
            );
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);

            binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
            binder.bind(BrokerServerView.class).in(LazySingleton.class);
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

            Jerseys.addResource(binder, BrokerQueryResource.class);
            binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
            Jerseys.addResource(binder, BrokerResource.class);
            Jerseys.addResource(binder, ClientInfoResource.class);
            LifecycleModule.register(binder, BrokerQueryResource.class);
            LifecycleModule.register(binder, DruidBroker.class);

            MetricsModule.register(binder, CacheMonitor.class);

            LifecycleModule.register(binder, Server.class);
          }
        },
        new LookupModule(),
        new SqlModule()
    );
  }
}
