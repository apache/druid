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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.selector.ConnectionCountServerSelectorStrategy;
import org.apache.druid.client.selector.CustomTierSelectorStrategy;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.LowestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.jackson.JacksonModule;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class CliBrokerTest
{

  @Test
  public void testDefaultServerSelectorStrategy()
  {
    final Injector injector = makeBrokerInjector(new Properties());

    final ServerSelectorStrategy historicalBalancer = injector.getInstance(ServerSelectorStrategy.class);
    Assert.assertNotNull(historicalBalancer);
    Assert.assertTrue(historicalBalancer instanceof RandomServerSelectorStrategy);

    final ServerSelectorStrategy realtimeBalancer = injector.getInstance(
        Key.get(ServerSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );
    Assert.assertNotNull(realtimeBalancer);
    Assert.assertTrue(realtimeBalancer instanceof RandomServerSelectorStrategy);
    Assert.assertSame(realtimeBalancer, historicalBalancer);
  }

  @Test
  public void testDefaultTierSelectorStrategy()
  {
    final Injector injector = makeBrokerInjector(new Properties());
    TierSelectorStrategy historicalTierSelector = injector.getInstance(TierSelectorStrategy.class);
    TierSelectorStrategy realtimeTierSelector = injector.getInstance(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)));
    Assert.assertTrue(historicalTierSelector instanceof HighestPriorityTierSelectorStrategy);
    Assert.assertTrue(realtimeTierSelector instanceof HighestPriorityTierSelectorStrategy);
    Assert.assertSame(realtimeTierSelector, historicalTierSelector);
  }

  @Test
  public void testHistoricalLowestPriorityStrategy()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "lowestPriority");

    final Injector injector = makeBrokerInjector(properties);
    Assert.assertTrue(injector.getInstance(TierSelectorStrategy.class) instanceof LowestPriorityTierSelectorStrategy);
    Assert.assertTrue(
        injector.getInstance(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
        instanceof LowestPriorityTierSelectorStrategy
    );
  }

  @Test
  public void testRealtimeCustomStrategy()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.realtime.select.tier", "custom");
    properties.setProperty("druid.broker.realtime.select.tier.custom.priorities", "[2,1,0]");
    properties.setProperty("druid.broker.balancer.type", "random");

    final Injector injector = makeBrokerInjector(properties);

    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    Assert.assertTrue(realtime instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(2, 1, 0), ((CustomTierSelectorStrategy) realtime).getConfig().getPriorities());

    Assert.assertTrue(injector.getInstance(TierSelectorStrategy.class) instanceof HighestPriorityTierSelectorStrategy);

    final ServerSelectorStrategy realtimeBalancer = injector.getInstance(
        Key.get(ServerSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );
    Assert.assertTrue(realtimeBalancer instanceof RandomServerSelectorStrategy);
  }

  @Test
  public void testHistoricalAndRealtimeCustomStrategies()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "custom");
    properties.setProperty("druid.broker.select.tier.custom.priorities", "[0]");
    properties.setProperty("druid.broker.balancer.type", "random");

    properties.setProperty("druid.broker.realtime.select.tier", "custom");
    properties.setProperty("druid.broker.realtime.select.tier.custom.priorities", "[2,1]");
    properties.setProperty("druid.broker.realtime.balancer.type", "connectionCount");

    final Injector injector = makeBrokerInjector(properties);
    final TierSelectorStrategy historical = injector.getInstance(TierSelectorStrategy.class);
    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    // Verify tier selector strategies with strategy-specific config paths
    Assert.assertTrue(historical instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(0), ((CustomTierSelectorStrategy) historical).getConfig().getPriorities());

    Assert.assertTrue(realtime instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(2, 1), ((CustomTierSelectorStrategy) realtime).getConfig().getPriorities());

    // Verify different server selector strategies
    final ServerSelectorStrategy historicalBalancer = injector.getInstance(ServerSelectorStrategy.class);
    final ServerSelectorStrategy realtimeBalancer = injector.getInstance(
        Key.get(ServerSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    Assert.assertTrue(historicalBalancer instanceof RandomServerSelectorStrategy);
    Assert.assertTrue(realtimeBalancer instanceof ConnectionCountServerSelectorStrategy);
  }


  @Test
  public void testHistoricalAndRealtimeDifferentStrategies()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "custom");
    properties.setProperty("druid.broker.select.tier.custom.priorities", "[0]");

    properties.setProperty("druid.broker.realtime.select.tier", "lowestPriority");

    final Injector injector = makeBrokerInjector(properties);

    final TierSelectorStrategy historical = injector.getInstance(TierSelectorStrategy.class);
    Assert.assertTrue(historical instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(0), ((CustomTierSelectorStrategy) historical).getConfig().getPriorities());

    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );
    Assert.assertTrue(realtime instanceof LowestPriorityTierSelectorStrategy);
  }


  @Test
  public void testServerSelectorStrategyFallbackToGlobal()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "custom");
    properties.setProperty("druid.broker.select.tier.custom.priorities", "[0]");
    properties.setProperty("druid.broker.balancer.type", "random");

    final Injector injector = makeBrokerInjector(properties);

    final ServerSelectorStrategy historicalBalancer = injector.getInstance(ServerSelectorStrategy.class);
    final ServerSelectorStrategy realtimeBalancer = injector.getInstance(
        Key.get(ServerSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    Assert.assertTrue(historicalBalancer instanceof RandomServerSelectorStrategy);
    Assert.assertTrue(realtimeBalancer instanceof RandomServerSelectorStrategy);

    final TierSelectorStrategy historical = injector.getInstance(TierSelectorStrategy.class);
    Assert.assertTrue(historical instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(0), ((CustomTierSelectorStrategy) historical).getConfig().getPriorities());
  }

  private Injector makeBrokerInjector(final Properties props)
  {
    final Injector baseInjector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
        }
    );

    final CliBroker broker = new CliBroker();
    broker.configure(props);
    broker.configure(props, baseInjector);
    return broker.makeInjector(Set.of(NodeRole.BROKER));
  }
}
