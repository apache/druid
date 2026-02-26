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
import org.apache.druid.client.selector.CustomTierSelectorStrategy;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.LowestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.PreferredTierSelectorStrategy;
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
  public void testDefaultTierSelectorStrategy()
  {
    final Injector injector = makeBrokerInjector(new Properties());
    Assert.assertTrue(injector.getInstance(TierSelectorStrategy.class) instanceof HighestPriorityTierSelectorStrategy);
    Assert.assertNull(
        injector.getInstance(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
    );
  }

  @Test
  public void testHistoricalLowestPriorityStrategy()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "lowestPriority");

    final Injector injector = makeBrokerInjector(properties);
    Assert.assertTrue(injector.getInstance(TierSelectorStrategy.class) instanceof LowestPriorityTierSelectorStrategy);
    Assert.assertNull(
        injector.getInstance(Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR)))
    );
  }

  @Test
  public void testRealtimeCustomStrategy()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.realtime.tier", "custom");
    properties.setProperty("druid.broker.select.realtime.tier.custom.priorities", "[2,1,0]");

    final Injector injector = makeBrokerInjector(properties);

    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    Assert.assertTrue(realtime instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(2, 1, 0), ((CustomTierSelectorStrategy) realtime).getConfig().getPriorities());

    Assert.assertTrue(injector.getInstance(TierSelectorStrategy.class) instanceof HighestPriorityTierSelectorStrategy);
  }

  @Test
  public void testHistoricalAndRealtimeCustomStrategies()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "custom");
    properties.setProperty("druid.broker.select.tier.custom.priorities", "[0]");

    properties.setProperty("druid.broker.select.realtime.tier", "custom");
    properties.setProperty("druid.broker.select.realtime.tier.custom.priorities", "[2,1]");

    final Injector injector = makeBrokerInjector(properties);
    final TierSelectorStrategy historical = injector.getInstance(TierSelectorStrategy.class);
    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );

    Assert.assertTrue(historical instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(0), ((CustomTierSelectorStrategy) historical).getConfig().getPriorities());

    Assert.assertTrue(realtime instanceof CustomTierSelectorStrategy);
    Assert.assertEquals(List.of(2, 1), ((CustomTierSelectorStrategy) realtime).getConfig().getPriorities());
  }


  @Test
  public void testHistoricalAndRealtimeDifferentStrategies()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "custom");
    properties.setProperty("druid.broker.select.tier.custom.priorities", "[0]");

    properties.setProperty("druid.broker.select.realtime.tier", "lowestPriority");

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
  public void testPreferredTierDifferentPreferredStrategies()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.broker.select.tier", "preferred");
    properties.setProperty("druid.broker.select.tier.preferred.tier", "_default_tier");
    properties.setProperty("druid.broker.select.tier.preferred.priority", "lowest");

    properties.setProperty("druid.broker.select.realtime.tier", "preferred");
    properties.setProperty("druid.broker.select.realtime.tier.preferred.tier", "realtime_tier");
    properties.setProperty("druid.broker.select.realtime.tier.preferred.priority", "highest");

    final Injector injector = makeBrokerInjector(properties);
    final TierSelectorStrategy historical = injector.getInstance(TierSelectorStrategy.class);
    Assert.assertTrue(historical instanceof PreferredTierSelectorStrategy);
    final PreferredTierSelectorStrategy historicalPreferredStrategy = (PreferredTierSelectorStrategy) historical;
    Assert.assertEquals("_default_tier", historicalPreferredStrategy.getConfig().getTier());
    Assert.assertEquals("lowest", historicalPreferredStrategy.getConfig().getPriority());

    final TierSelectorStrategy realtime = injector.getInstance(
        Key.get(TierSelectorStrategy.class, Names.named(BrokerServerView.REALTIME_SELECTOR))
    );
    Assert.assertTrue(realtime instanceof PreferredTierSelectorStrategy);
    PreferredTierSelectorStrategy realtimePreferredStrategy = (PreferredTierSelectorStrategy) realtime;
    Assert.assertEquals("realtime_tier", realtimePreferredStrategy.getConfig().getTier());
    Assert.assertEquals("highest", realtimePreferredStrategy.getConfig().getPriority());
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
