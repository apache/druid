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

package org.apache.druid.server.router;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.Server;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;

public class TieredBrokerHostSelectorDeploymentGroupTest
{
  private TieredBrokerHostSelector brokerSelector;
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  @After
  public void tearDown()
  {
    if (brokerSelector != null) {
      brokerSelector.stop();
    }
    EasyMock.verify(druidNodeDiscoveryProvider);
  }

  @Test
  public void testFilterExcludesBrokersWithNonMatchingDeploymentGroup()
  {
    final DiscoveryDruidNode blackBroker = makeBroker("black-broker", "blackHost", "black");
    final DiscoveryDruidNode redBroker = makeBroker("black-broker", "redHost", "red");
    final DiscoveryDruidNode untaggedBroker = makeBroker("black-broker", "untaggedHost", null);

    setupSelector(ImmutableSet.of("black"), blackBroker, redBroker, untaggedBroker);

    final Pair<String, Server> picked = brokerSelector.select(simpleQuery());
    Assert.assertEquals("black-broker", picked.lhs);
    Assert.assertEquals("blackHost:8080", picked.rhs.getHost());

    // Round-robin should keep returning the only matching broker.
    Assert.assertEquals("blackHost:8080", brokerSelector.select(simpleQuery()).rhs.getHost());
  }

  @Test
  public void testFilterUnsetIncludesAllBrokers()
  {
    final DiscoveryDruidNode b1 = makeBroker("default-broker", "host1", "black");
    final DiscoveryDruidNode b2 = makeBroker("default-broker", "host2", "red");

    setupSelector(null, b1, b2);

    final Set<String> seenHosts = ImmutableSet.of(
        brokerSelector.select(simpleQuery()).rhs.getHost(),
        brokerSelector.select(simpleQuery()).rhs.getHost()
    );
    Assert.assertEquals(ImmutableSet.of("host1:8080", "host2:8080"), seenHosts);
  }

  @Test
  public void testFilterEliminatingAllBrokersReturnsNullServer()
  {
    final DiscoveryDruidNode redBroker = makeBroker("default-broker", "redHost", "red");

    setupSelector(ImmutableSet.of("black"), redBroker);

    final Pair<String, Server> picked = brokerSelector.select(simpleQuery());
    Assert.assertEquals("default-broker", picked.lhs);
    Assert.assertNull("Filter should fail closed when no broker matches", picked.rhs);
  }

  private DiscoveryDruidNode makeBroker(String serviceName, String host, String deploymentGroup)
  {
    return new DiscoveryDruidNode(
        new DruidNode(serviceName, host, false, 8080, null, null, true, false, null, deploymentGroup),
        NodeRole.BROKER,
        ImmutableMap.of()
    );
  }

  private void setupSelector(Set<String> acceptableDeploymentGroups, DiscoveryDruidNode... brokers)
  {
    druidNodeDiscoveryProvider = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);

    final Collection<DiscoveryDruidNode> brokerSet = Arrays.asList(brokers);
    final DruidNodeDiscovery druidNodeDiscovery = new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return brokerSet;
      }

      @Override
      public void registerListener(Listener listener)
      {
        listener.nodesAdded(ImmutableList.copyOf(brokerSet));
        listener.nodeViewInitialized();
      }
    };

    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    final String defaultBrokerName = brokers[0].getDruidNode().getServiceName();
    brokerSelector = new TieredBrokerHostSelector(
        new NoopRuleManager(),
        new TieredBrokerConfig()
        {
          @Override
          public LinkedHashMap<String, String> getTierToBrokerMap()
          {
            return new LinkedHashMap<>(ImmutableMap.of(DruidServer.DEFAULT_TIER, defaultBrokerName));
          }

          @Override
          public String getDefaultBrokerServiceName()
          {
            return defaultBrokerName;
          }

          @Override
          public Set<String> getAcceptableDeploymentGroups()
          {
            return acceptableDeploymentGroups;
          }
        },
        druidNodeDiscoveryProvider,
        ImmutableList.of()
    );
    brokerSelector.start();
  }

  private TimeseriesQuery simpleQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test")
                 .granularity("all")
                 .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
                 .intervals(Collections.singletonList(Intervals.of("2024-01-01/2024-01-02")))
                 .build();
  }

  private static class NoopRuleManager extends CoordinatorRuleManager
  {
    NoopRuleManager()
    {
      super(null, null);
    }

    // Returning false short-circuits select() to the default-lookup path, which is what
    // these tests want to exercise — the filter is applied when nodes are added to the holder.
    @Override
    public boolean isStarted()
    {
      return false;
    }
  }
}
