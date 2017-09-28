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
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CacheMonitor;
import io.druid.guice.CacheModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.NodeTypeConfig;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.lookup.LookupModule;
import io.druid.server.QueryResource;
import io.druid.server.SegmentManager;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.ServerType;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.http.HistoricalResource;
import io.druid.server.http.SegmentListerResource;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.metrics.MetricsModule;
import io.druid.server.metrics.QueryCountStatsProvider;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "historical",
    description = "Runs a Historical node, see http://druid.io/docs/latest/Historical.html for a description"
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
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/historical");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8083);

            LifecycleModule.register(binder, Server.class);
            binder.bind(ServerManager.class).in(LazySingleton.class);

            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig(ServerType.HISTORICAL));
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
          }
        },
        // Start lookups before loading segments, to minimize harm from lookups unavailability when segments are already
        // loaded
        new LookupModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(SegmentManager.class).in(LazySingleton.class);
            binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
            binder.bind(QuerySegmentWalker.class).to(ServerManager.class).in(LazySingleton.class);


            binder.bind(QueryCountStatsProvider.class).to(QueryResource.class);
            Jerseys.addResource(binder, QueryResource.class);
            Jerseys.addResource(binder, HistoricalResource.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            LifecycleModule.register(binder, QueryResource.class);
            LifecycleModule.register(binder, ZkCoordinator.class);

            JsonConfigProvider.bind(binder, "druid.historical.cache", CacheConfig.class);
            binder.install(new CacheModule());
            MetricsModule.register(binder, CacheMonitor.class);
          }
        }
    );
  }
}
