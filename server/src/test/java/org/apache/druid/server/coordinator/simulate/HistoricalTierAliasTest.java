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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.Stats;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Verifies that when historical tiers are grouped under an alias (e.g. a blue/green
 * pair), the coordinator tags the per-tier capacity metrics with the
 * {@link Dimension#TIER_ALIAS} dimension so they can be aggregated by alias.
 */
public class HistoricalTierAliasTest extends CoordinatorSimulationBaseTest
{
  private static final long SIZE_1TB = 1_000_000;
  private static final String ALIAS = "hot";

  private DruidServer historicalT1;
  private DruidServer historicalT2;

  private final String datasource = TestDataSource.WIKI;

  @Override
  public void setUp()
  {
    // T1 and T2 are interchangeable physical tiers grouped under the alias "hot"
    historicalT1 = createHistorical(1, Tier.T1, SIZE_1TB);
    historicalT2 = createHistorical(1, Tier.T2, SIZE_1TB);
  }

  @Test
  public void testCapacityMetricsAreTaggedWithAlias()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT1, historicalT2)
                             // Rule targets the virtual alias, which expands to T1 and T2
                             .withRules(datasource, Load.on(ALIAS, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withHistoricalTierAliases(
                                                             Map.of(ALIAS, Set.of(Tier.T1, Tier.T2))
                                                         )
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    final long expectedCapacity = SIZE_1TB << 20;

    // tier/total/capacity is emitted per physical tier AND tagged with the alias
    verifyValue(
        Stats.Tier.TOTAL_CAPACITY.getMetricName(),
        Map.of(Dimension.TIER.reportedName(), Tier.T1, Dimension.TIER_ALIAS.reportedName(), ALIAS),
        expectedCapacity
    );
    verifyValue(
        Stats.Tier.TOTAL_CAPACITY.getMetricName(),
        Map.of(Dimension.TIER.reportedName(), Tier.T2, Dimension.TIER_ALIAS.reportedName(), ALIAS),
        expectedCapacity
    );

    // tier/historical/count carries the alias too, so it can be summed across the pair
    verifyValue(
        Stats.Tier.HISTORICAL_COUNT.getMetricName(),
        Map.of(Dimension.TIER.reportedName(), Tier.T1, Dimension.TIER_ALIAS.reportedName(), ALIAS),
        1L
    );
    verifyValue(
        Stats.Tier.HISTORICAL_COUNT.getMetricName(),
        Map.of(Dimension.TIER.reportedName(), Tier.T2, Dimension.TIER_ALIAS.reportedName(), ALIAS),
        1L
    );
  }

  @Test
  public void testCapacityMetricsHaveNoAliasWhenNotConfigured()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT1, historicalT2)
                             .withRules(datasource, Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    final long expectedCapacity = SIZE_1TB << 20;

    // Without an alias configured, capacity is reported against the physical tier only
    verifyValue(
        Stats.Tier.TOTAL_CAPACITY.getMetricName(),
        filterByTier(Tier.T1),
        expectedCapacity
    );
    verifyValue(
        Stats.Tier.TOTAL_CAPACITY.getMetricName(),
        filterByTier(Tier.T2),
        expectedCapacity
    );
  }
}
