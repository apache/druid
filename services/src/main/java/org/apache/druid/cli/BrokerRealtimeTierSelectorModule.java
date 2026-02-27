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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.selector.CustomTierSelectorStrategy;
import org.apache.druid.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.druid.client.selector.PreferredTierSelectorStrategy;
import org.apache.druid.client.selector.PreferredTierSelectorStrategyConfig;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Guice module that configures the {@link TierSelectorStrategy} for {@link BrokerServerView#REALTIME_SELECTOR} tiers
 * in the Broker. The strategy is determined by {@link BrokerRealtimeTierSelectorModule#PROPERTY}. If
 * the property is not configured, then {@link RealtimeTierSelectorStrategyProvider#get()} returns
 * null, which then fallsback to the {@code druid.broker.select.tier} in {@link BrokerServerView}.
 */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class BrokerRealtimeTierSelectorModule implements DruidModule
{
  private static final Logger log = new Logger(BrokerRealtimeTierSelectorModule.class);

  private static final String PROPERTY = "druid.broker.select.realtime.tier";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
          .toProvider(RealtimeTierSelectorStrategyProvider.class)
          .in(LazySingleton.class);
  }

  private static class RealtimeTierSelectorStrategyProvider implements Provider<TierSelectorStrategy>
  {
    private final Properties properties;
    private final JsonConfigurator configurator;
    private final ServerSelectorStrategy serverSelectorStrategy;

    @Inject
    public RealtimeTierSelectorStrategyProvider(
        Properties properties,
        JsonConfigurator configurator,
        ServerSelectorStrategy serverSelectorStrategy
    )
    {
      this.properties = properties;
      this.configurator = configurator;
      this.serverSelectorStrategy = serverSelectorStrategy;
    }

    @Nullable
    @Override
    public TierSelectorStrategy get()
    {
      final String realtimeTier = properties.getProperty(PROPERTY);
      if (realtimeTier == null) {
        log.info("[%s] is not configured.", PROPERTY);
        return null;
      }

      if (CustomTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final CustomTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            "druid.broker.select.realtime.tier.custom",
            CustomTierSelectorStrategyConfig.class
        );

        log.info("Creating CustomTierSelectorStrategy for realtime servers with config[%s]", config);
        return new CustomTierSelectorStrategy(serverSelectorStrategy, config);
      } else if (PreferredTierSelectorStrategy.TYPE.equals(realtimeTier)) {
        final PreferredTierSelectorStrategyConfig config = configurator.configurate(
            properties,
            "druid.broker.select.realtime.tier.preferred",
            PreferredTierSelectorStrategyConfig.class
        );

        log.info("Creating PreferredTierSelectorStrategy for realtime servers with config[%s]", config);
        return new PreferredTierSelectorStrategy(serverSelectorStrategy, config);
      } else {
        // For other strategies that don't need config, just fallback to this
        return configurator.configurate(
            properties,
            "druid.broker.select.realtime",
            TierSelectorStrategy.class
        );
      }
    }
  }
}
