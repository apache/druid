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
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerManager;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.ZkCoordinator;
import org.apache.druid.server.http.HistoricalResource;
import org.apache.druid.server.http.SegmentListerResource;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.timeline.PruneLastCompactionState;
import org.eclipse.jetty.server.Server;

import java.util.List;

@Command(
    name = "historical",
    description = "Runs a Historical node, see https://druid.apache.org/docs/latest/Historical.html for a description"
)
public class CliHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliHistorical.class);

  public CliHistorical()
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
        new JoinableFactoryModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/historical");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8083);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8283);
          binder.bindConstant().annotatedWith(PruneLastCompactionState.class).to(true);
          binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));

          // register Server before binding ZkCoordinator to ensure HTTP endpoints are available immediately
          LifecycleModule.register(binder, Server.class);
          binder.bind(ServerManager.class).in(LazySingleton.class);
          binder.bind(SegmentManager.class).in(LazySingleton.class);
          binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
          binder.bind(QuerySegmentWalker.class).to(ServerManager.class).in(LazySingleton.class);

          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.HISTORICAL));
          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
          binder.bind(QueryCountStatsProvider.class).to(QueryResource.class);
          Jerseys.addResource(binder, QueryResource.class);
          Jerseys.addResource(binder, HistoricalResource.class);
          Jerseys.addResource(binder, SegmentListerResource.class);
          LifecycleModule.register(binder, QueryResource.class);
          LifecycleModule.register(binder, ZkCoordinator.class);

          JsonConfigProvider.bind(binder, "druid.historical.cache", CacheConfig.class);
          binder.install(new CacheModule());

          bindNodeRoleAndAnnouncer(
              binder,
              DiscoverySideEffectsProvider
                  .builder(NodeRole.HISTORICAL)
                  .serviceClasses(ImmutableList.of(DataNodeService.class, LookupNodeService.class))
                  .build()
          );

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
        },
        new LookupModule()
    );
  }
}
