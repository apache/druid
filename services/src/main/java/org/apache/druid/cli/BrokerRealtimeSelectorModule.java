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

import com.google.common.base.Strings;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.selector.CustomTierSelectorStrategy;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.PooledTierSelectorStrategy;
import org.apache.druid.client.selector.PooledTierSelectorStrategyConfig;
import org.apache.druid.client.selector.PreferredTierSelectorStrategy;
import org.apache.druid.client.selector.PreferredTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.StrictTierSelectorStrategy;
import org.apache.druid.client.selector.StrictTierSelectorStrategyConfig;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.inject.Named;
import java.util.Properties;

/**
 * Guice module that configures the {@link ServerSelectorStrategy} and {@link TierSelectorStrategy}
 * for realtime servers in the Broker. These can be configured on the Broker as follows:
 * <ul>
 *   <li>
 *     If {@value #REALTIME_BALANCER_PROPERTY} is configured, the {@link ServerSelectorStrategy} for realtime servers
 *     is provided via {@link RealtimeServerSelectorStrategyProvider}. If not set, it defaults to {@code druid.broker.balancer.type}.
 *   </li>
 *   <li>
 *     If {@value #REALTIME_SELECT_TIER_PROPERTY} is configured, the {@link TierSelectorStrategy} for realtime servers
 *     is provided via {@link RealtimeTierSelectorStrategyProvider}. If not set, it defaults to {@code druid.broker.select.tier}.
 *   </li>
 * </ul>
 */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class BrokerRealtimeSelectorModule implements DruidModule
{
  private static final Logger log = new Logger(BrokerRealtimeSelectorModule.class);

  private static final String REALTIME_BALANCER_PROPERTY = "druid.broker.realtime.balancer.type";
  private static final String REALTIME_SELECT_TIER_PROPERTY = "druid.broker.realtime.select.tier";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(ServerSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
          .toProvider(RealtimeServerSelectorStrategyProvider.class)
          .in(LazySingleton.class);

    binder.bind(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
          .toProvider(RealtimeTierSelectorStrategyProvider.class)
          .in(LazySingleton.class);
  }

  private static class RealtimeServerSelectorStrategyProvider implements Provider<ServerSelectorStrategy>
  {
    private final Injector injector;
    private final Properties properties;
    private final JsonConfigurator configurator;

    @Inject
    public RealtimeServerSelectorStrategyProvider(final Injector injector, final Properties properties, final JsonConfigurator configurator)
    {
      this.injector = injector;
      this.properties = properties;
      this.configurator = configurator;
    }

    @Override
    public ServerSelectorStrategy get()
    {
      final String realtimeSelector = properties.getProperty(REALTIME_BALANCER_PROPERTY);
      if (Strings.isNullOrEmpty(realtimeSelector)) {
        log.info("[%s] is not configured. Using realtime ServerSelectorStrategy from default [druid.broker.balancer]", REALTIME_BALANCER_PROPERTY);
        return injector.getInstance(ServerSelectorStrategy.class);
      } else {
        log.info("Using realtime ServerSelectorStrategy from [%s]", REALTIME_BALANCER_PROPERTY);
        return configurator.configurate(properties, "druid.broker.realtime.balancer", ServerSelectorStrategy.class);
      }
    }
  }

  private static class RealtimeTierSelectorStrategyProvider implements Provider<TierSelectorStrategy>
  {
    private final Properties properties;
    private final JsonConfigurator configurator;
    private final ServerSelectorStrategy realtimeServerSelectorStrategy;
    private final Injector injector;

    @Inject
    public RealtimeTierSelectorStrategyProvider(
        final Injector injector,
        final Properties properties,
        final JsonConfigurator configurator,
        final @Named(BrokerServerView.REALTIME_SELECTOR) ServerSelectorStrategy realtimeServerSelectorStrategy
    )
    {
      this.injector = injector;
      this.properties = properties;
      this.configurator = configurator;
      this.realtimeServerSelectorStrategy = realtimeServerSelectorStrategy;
    }

    @Override
    public TierSelectorStrategy get()
    {
      final String realtimeTier = properties.getProperty(REALTIME_SELECT_TIER_PROPERTY);
      if (Strings.isNullOrEmpty(realtimeTier)) {
        log.info("[%s] is not configured, using default TierSelectorStrategy from default [druid.broker.select]", REALTIME_SELECT_TIER_PROPERTY);
        return injector.getInstance(TierSelectorStrategy.class);
      }

      if (CustomTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final CustomTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            REALTIME_SELECT_TIER_PROPERTY + "." + realtimeTier,
            CustomTierSelectorStrategyConfig.class
        );

        log.info("Creating CustomTierSelectorStrategy for realtime servers with config[%s]", config);
        return new CustomTierSelectorStrategy(realtimeServerSelectorStrategy, config);
      } else if (PreferredTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final PreferredTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            REALTIME_SELECT_TIER_PROPERTY + "." + realtimeTier,
            PreferredTierSelectorStrategyConfig.class
        );

        log.info("Creating PreferredTierSelectorStrategy for realtime servers with config[%s]", config);
        return new PreferredTierSelectorStrategy(realtimeServerSelectorStrategy, config);
      } else if (StrictTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final StrictTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            REALTIME_SELECT_TIER_PROPERTY + "." + realtimeTier,
            StrictTierSelectorStrategyConfig.class
        );

        log.info("Creating StrictTierSelectorStrategy for realtime servers with config[%s]", config);
        return new StrictTierSelectorStrategy(realtimeServerSelectorStrategy, config, injector.getInstance(ServiceEmitter.class));
      } else if (PooledTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final PooledTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            REALTIME_SELECT_TIER_PROPERTY + "." + realtimeTier,
            PooledTierSelectorStrategyConfig.class
        );

        log.info("Creating PooledTierSelectorStrategy for realtime servers with config[%s]", config);
        return new PooledTierSelectorStrategy(realtimeServerSelectorStrategy, config);
      } else {
        return configurator.configurate(properties, "druid.broker.realtime.select", TierSelectorStrategy.class);
      }
    }
  }
}
