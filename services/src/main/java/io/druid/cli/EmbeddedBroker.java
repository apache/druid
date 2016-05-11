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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.guava.Sequence;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
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
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.server.ClientQuerySegmentWalker;
import io.druid.server.DruidNode;
import io.druid.server.QueryManager;
import io.druid.server.QueryResource;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.metrics.MetricsModule;
import io.druid.server.router.TieredBrokerConfig;
import org.eclipse.jetty.server.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 */
public class EmbeddedBroker extends ServerRunnable
{
  private static final Logger log = new Logger(EmbeddedBroker.class);

  public EmbeddedBroker()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class),
                new DruidNode(TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME, null, null)
            );

            binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

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
            Jerseys.addResource(binder, QueryResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            MetricsModule.register(binder, CacheMonitor.class);

            LifecycleModule.register(binder, Server.class);
          }
        }
    );
  }

  public static EmbeddedResource create() throws Exception
  {
    final Injector injector = GuiceInjectors.makeStartupInjector();
    EmbeddedBroker broker = new EmbeddedBroker();
    injector.injectMembers(broker);

    Injector brokerInjector = broker.makeInjector();
    broker.initLifecycle(brokerInjector);

    return brokerInjector.getInstance(EmbeddedResource.class);
  }

  public static class EmbeddedResource implements Closeable
  {
    private final ServerConfig config;
    private final QuerySegmentWalker texasRanger;
    private final QueryManager queryManager;

    private final Lifecycle lifecycle;

    @Inject
    public EmbeddedResource(
        ServerConfig config,
        QuerySegmentWalker texasRanger,
        QueryManager queryManager,
        Lifecycle lifecycle
    )
    {
      this.config = config;
      this.texasRanger = texasRanger;
      this.queryManager = queryManager;
      this.lifecycle = lifecycle;
    }

    public Sequence runQuery(Query query, Map<String, Object> context)
    {
      String queryId = query.getId();
      if (queryId == null) {
        queryId = UUID.randomUUID().toString();
        query = query.withId(queryId);
      }
      if (query.getContextValue(QueryContextKeys.TIMEOUT) == null) {
        query = query.withOverriddenContext(
            ImmutableMap.of(
                QueryContextKeys.TIMEOUT,
                config.getMaxIdleTime().toStandardDuration().getMillis()
            )
        );
      }

      return query.run(texasRanger, context);
    }

    public boolean cancelQuery(String queryId)
    {
      return queryManager.cancelQuery(queryId);
    }

    @Override
    public void close() throws IOException
    {
      lifecycle.stop();
    }
  }
}
