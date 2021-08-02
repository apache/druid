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

package org.apache.druid.client.selector;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CustomTierSelectorStrategyTest
{
  private static final QueryRunner QUERY_RUNNER = Mockito.mock(QueryRunner.class);

  @Test
  public void testConfigSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    // Verify empty config
    String json = "{}";
    CustomTierSelectorStrategyConfig config = objectMapper
        .readValue(json, CustomTierSelectorStrategyConfig.class);
    assertTrue(config.getPriorities().isEmpty());
    assertTrue(config.getAllowedTiers().isEmpty());

    // Verify config with empty priorities, non-empty allowedTiers
    json = "{\"allowedTiers\":[\"t1\",\"t2\"]}";
    config = objectMapper
        .readValue(json, CustomTierSelectorStrategyConfig.class);
    assertTrue(config.getPriorities().isEmpty());
    assertEquals(Arrays.asList("t1", "t2"), config.getAllowedTiers());

    // Verify config with non-empty priorities, empty allowedTiers
    json = "{\"priorities\":[1, 2]}";
    config = objectMapper
        .readValue(json, CustomTierSelectorStrategyConfig.class);
    assertEquals(Arrays.asList(1, 2), config.getPriorities());
    assertTrue(config.getAllowedTiers().isEmpty());

    // Verify config with non-empty priorities and non-empty allowedTiers
    json = "{\"priorities\": [1, 2], \"allowedTiers\":[\"t1\",\"t2\"]}";
    config = objectMapper
        .readValue(json, CustomTierSelectorStrategyConfig.class);
    assertEquals(Arrays.asList(1, 2), config.getPriorities());
    assertEquals(Arrays.asList("t1", "t2"), config.getAllowedTiers());
  }

  @Test
  public void testPickFromTier()
  {
    // Strategy that picks only from Tier 1
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        createStrategyConfig(Names.TIER_1)
    );

    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 1);

    // Verify that a server from Tier 1 is picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21),
        server11
    );
  }

  @Test
  public void testPickFromAnyTier()
  {
    // Strategy that can pick from all tiers
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        createStrategyConfig()
    );

    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 2);

    // Verify that the higher priority server is picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21),
        server21
    );

    // Verify that the order of adding the servers does not matter
    verifyPickedServers(
        strategy,
        Arrays.asList(server21, server11),
        server21
    );
  }

  @Test
  public void testPickFromTierWithPriority()
  {
    // Strategy that can pick from Tier 2
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        createStrategyConfig(Names.TIER_2)
    );

    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 1);
    DruidServer server22 = createHistorical(Names.TIER_2, "hist22", 2);

    // Verify that the higher priority server from Tier 2 is picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21, server22),
        server22
    );

    // Verify that servers from Tier 2 are picked in the right order of priority
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21, server22),
        server22, server21
    );
  }

  @Test
  public void testPickFromMultipleTiers()
  {
    // Strategy that can pick from Tier 1 and Tier 2
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        createStrategyConfig(Names.TIER_1, Names.TIER_2)
    );

    // Create a prioritized set of historical servers
    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server22 = createHistorical(Names.TIER_2, "hist22", 2);
    DruidServer server13 = createHistorical(Names.TIER_1, "hist13", 3);
    DruidServer server34 = createHistorical(Names.TIER_3, "hist34", 4);

    // Verify that servers from both Tier 2 and Tier 1 are picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server22, server13, server34),
        server13
    );

    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server22, server13, server34),
        server13, server22
    );

    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server22, server13, server34),
        server13, server22, server11
    );
  }

  @Test
  public void testPickWithCustomPriority()
  {
    // Strategy that can pick from all tiers, priority order: 1, 2
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        new CustomTierSelectorStrategyConfig()
        {
          @Override
          public List<Integer> getPriorities()
          {
            return Arrays.asList(1, 2);
          }
        }
    );

    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 2);

    // Verify that the server with priority 1 is picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21),
        server11
    );
  }

  @Test
  public void testPickFromTierWithCustomPriority()
  {
    // Strategy that can pick from Tier 2, priority order: 1, 2
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        new CustomTierSelectorStrategyConfig()
        {
          @Override
          public List<String> getAllowedTiers()
          {
            return Collections.singletonList(Names.TIER_2);
          }

          @Override
          public List<Integer> getPriorities()
          {
            return Arrays.asList(1, 2);
          }
        }
    );

    DruidServer server11 = createHistorical(Names.TIER_1, "hist11", 1);
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 1);
    DruidServer server22 = createHistorical(Names.TIER_2, "hist22", 2);

    // Verify that the higher priority server from Tier 2 is picked
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21, server22),
        server21
    );

    // Verify that servers from Tier 2 are picked in the right order of priority
    verifyPickedServers(
        strategy,
        Arrays.asList(server11, server21, server22),
        server21, server22
    );
  }

  @Test
  public void testPickWhenNoServerIsEligible()
  {
    // Strategy that can pick from Tier 1
    CustomTierSelectorStrategy strategy = new CustomTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        createStrategyConfig(Names.TIER_1)
    );

    // Create servers only for Tier 2
    DruidServer server21 = createHistorical(Names.TIER_2, "hist21", 1);
    DruidServer server22 = createHistorical(Names.TIER_2, "hist22", 2);

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServerMap =
        createPrioritizedServerMap(strategy, Arrays.asList(server21, server22));

    // Verify that single server picked is null
    assertNull(strategy.pick(null, prioritizedServerMap, createSegment()));
    assertTrue(strategy.pick(null, prioritizedServerMap, createSegment(), 3).isEmpty());
  }

  /**
   * Verifies that for the given strategy, the expected servers are picked in
   * the right order.
   */
  private void verifyPickedServers(
      CustomTierSelectorStrategy strategy,
      List<DruidServer> allServers,
      DruidServer... expectedPickedServers
  )
  {
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers =
        createPrioritizedServerMap(strategy, allServers);

    // Verify that the expected servers are picked
    if (expectedPickedServers.length == 1) {
      // Use the 3-argument pick() call if a single server is to be picked
      QueryableDruidServer actualPickedServer =
          strategy.pick(null, prioritizedServers, createSegment());
      assertEquals(expectedPickedServers[0], actualPickedServer.getServer());
    } else {
      // Use the 4-argument pick() call if multiple servers are to be picked
      List<QueryableDruidServer> actualPickedServers =
          strategy.pick(null, prioritizedServers, createSegment(), expectedPickedServers.length);
      assertEquals(expectedPickedServers.length, actualPickedServers.size());

      int index = 0;
      for (QueryableDruidServer actualPickedServer : actualPickedServers) {
        assertEquals(expectedPickedServers[index++], actualPickedServer.getServer());
      }
    }
  }

  /**
   * Adds all the given servers to a prioritized map using the comparator from
   * the strategy.
   */
  private Int2ObjectRBTreeMap<Set<QueryableDruidServer>> createPrioritizedServerMap(
      CustomTierSelectorStrategy strategy,
      List<DruidServer> allServers
  )
  {
    // Add all the servers to a prioritized map
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServerMap =
        new Int2ObjectRBTreeMap<>(strategy.getComparator());
    for (DruidServer server : allServers) {
      prioritizedServerMap
          .computeIfAbsent(server.getPriority(), s -> new HashSet<>())
          .add(new QueryableDruidServer(server, QUERY_RUNNER));
    }

    return prioritizedServerMap;
  }

  private DruidServer createHistorical(String tier, String name, int priority)
  {
    return new DruidServer(
        name,
        "host:" + tier + ":" + name,
        null,
        0,
        ServerType.HISTORICAL,
        tier,
        priority
    );
  }

  private DataSegment createSegment()
  {
    return new DataSegment(
        "test",
        Intervals.of("2010/2011"),
        DateTimes.of("2010-01-01").toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        0L
    );
  }

  private CustomTierSelectorStrategyConfig createStrategyConfig(String... tierNames)
  {
    return new CustomTierSelectorStrategyConfig()
    {
      @Override
      public List<String> getAllowedTiers()
      {
        return Arrays.asList(tierNames);
      }
    };
  }

  /**
   * Constant values used in the test.
   */
  private static class Names
  {
    static final String TIER_1 = "_tier_1";
    static final String TIER_2 = "_tier_2";
    static final String TIER_3 = "_tier_3";
  }
}
