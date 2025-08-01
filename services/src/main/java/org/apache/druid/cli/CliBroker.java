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

import com.github.rvesse.airline.annotations.Command;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.HttpServerInventoryViewResource;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.PreferredTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.BrokerProcessingModule;
import org.apache.druid.guice.BrokerServiceModule;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.server.BrokerDynamicConfigResource;
import org.apache.druid.server.BrokerQueryResource;
import org.apache.druid.server.ClientInfoResource;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SubqueryGuardrailHelper;
import org.apache.druid.server.SubqueryGuardrailHelperProvider;
import org.apache.druid.server.coordination.SegmentBootstrapper;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.ZkCoordinator;
import org.apache.druid.server.http.BrokerResource;
import org.apache.druid.server.http.HistoricalResource;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.metrics.SubqueryCountStatsProvider;
import org.apache.druid.server.router.TieredBrokerConfig;
import org.apache.druid.sql.calcite.schema.MetadataSegmentView;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.storage.local.LocalTmpStorageConfig;
import org.apache.druid.timeline.PruneLoadSpec;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;
import java.util.Set;

@Command(
    name = "broker",
    description = "Runs a broker node, see https://druid.apache.org/docs/latest/Broker.html for a description"
)
public class CliBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  private boolean isZkEnabled = true;

  public CliBroker()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    isZkEnabled = ZkEnablementConfig.isEnabled(properties);
  }

  @Override
  protected Set<NodeRole> getNodeRoles(Properties properties)
  {
    return ImmutableSet.of(NodeRole.BROKER);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new BrokerProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new BrokerServiceModule(),
        binder -> {
          validateCentralizedDatasourceSchemaConfig(getProperties());

          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
          binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true);
          binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(false));

          binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
          LifecycleModule.register(binder, BrokerServerView.class);
          LifecycleModule.register(binder, MetadataSegmentView.class);
          binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);
          binder.bind(QueryableDruidServer.Maker.class).to(DirectDruidClientFactory.class).in(LazySingleton.class);

          JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
          binder.install(new CacheModule());

          JsonConfigProvider.bind(binder, "druid.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.select.tier.preferred", PreferredTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "druid.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.segment", BrokerSegmentWatcherConfig.class);
          JsonConfigProvider.bind(binder, "druid.broker.internal.query.config", InternalQueryConfig.class);

          binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
          binder.bind(SubqueryGuardrailHelper.class).toProvider(SubqueryGuardrailHelperProvider.class);
          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
          binder.bind(SubqueryCountStatsProvider.class).toInstance(new SubqueryCountStatsProvider());
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);
          Jerseys.addResource(binder, BrokerDynamicConfigResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

          Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

          LifecycleModule.register(binder, Server.class);
          binder.bind(SegmentManager.class).in(LazySingleton.class);
          binder.bind(BrokerViewOfCoordinatorConfig.class).in(ManageLifecycle.class);
          binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.BROKER));
          Jerseys.addResource(binder, HistoricalResource.class);
          Jerseys.addResource(binder, SegmentListerResource.class);

          if (isZkEnabled) {
            LifecycleModule.register(binder, ZkCoordinator.class);
          }
          LifecycleModule.register(binder, SegmentBootstrapper.class);

          bindAnnouncer(
              binder,
              DiscoverySideEffectsProvider.withLegacyAnnouncer()
          );

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

          binder.bind(LocalTmpStorageConfig.class)
                .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("broker"))
                .in(LazySingleton.class);
        },
        new LookupModule(),
        new SqlModule()
    );
  }
}
