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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * Coordinator simulation test to verify behaviour of segment loading.
 */
public class SegmentLoadingTest extends CoordinatorSimulationBaseTest
{
  @Override
  public void setUp()
  {

  }

  @Test
  @Ignore("Fix #12881 to enable this test. "
          + "Current behaviour is to throttle globally and not per tier.")
  public void testFirstReplicaOnAnyTierIsNotThrottled()
  {
    // historicals = 1(in T1) + 1(in T2), segments = 10*1day
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 10_000);
    final DruidServer historicalT21 = createHistorical(1, Tier.T2, 10_000);
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .andPartitionsPerInterval(10)
                          .eachOfSizeMb(500);

    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // strategy = cost, replicas = 1(on T1) + 1(on T2)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .segments(segments)
                             .servers(historicalT11, historicalT21)
                             .dynamicConfig(dynamicConfig)
                             .balancer(new CostBalancerStrategyFactory())
                             .rules(
                                 DS.WIKI,
                                 Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever()
                             )
                             .build();

    // Put the first replica of all the segments on T1
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that all the missing replicas for T2 have been assigned
    verifyLatestMetricValue("segment/assigned/count", 10L);

    loadQueuedSegments();
    syncInventoryView();

    final DruidServer historicalViewT11 = getInventoryView(historicalT11.getName());
    final DruidServer historicalViewT21 = getInventoryView(historicalT21.getName());
    Assert.assertEquals(10, historicalViewT11.getTotalSegments());
    Assert.assertEquals(10, historicalViewT21.getTotalSegments());
    verifyDatasourceIsFullyLoaded(DS.WIKI);
  }

  @Test
  public void testSecondReplicaOnAnyTierIsThrottled()
  {
    // historicals = 2(in T1), segments = 10*1day
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 10_000);
    final DruidServer historicalT12 = createHistorical(2, Tier.T1, 10_000);
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .andPartitionsPerInterval(10)
                          .eachOfSizeMb(500);

    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // strategy = cost, replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .segments(segments)
                             .servers(historicalT11, historicalT12)
                             .rules(DS.WIKI, Load.on(Tier.T1, 2).forever())
                             .dynamicConfig(dynamicConfig)
                             .balancer(new CostBalancerStrategyFactory())
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that that replicationThrottleLimit is honored
    verifyLatestMetricValue("segment/assigned/count", 2L);

    loadQueuedSegments();
    syncInventoryView();

    final DruidServer historicalViewT11 = getInventoryView(historicalT11.getName());
    final DruidServer historicalViewT12 = getInventoryView(historicalT12.getName());
    Assert.assertEquals(10, historicalViewT11.getTotalSegments());
    Assert.assertEquals(2, historicalViewT12.getTotalSegments());
  }

  @Test
  @Ignore("Fix #12881 to enable this test. "
          + "Current impl allows throttle limit to be violated if loading is fast.")
  public void testReplicationThrottleWithImmediateLoading()
  {
    // historicals = 2(in T1), segments = 10*1day
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 10_000);
    final DruidServer historicalT12 = createHistorical(2, Tier.T1, 10_000);
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .andPartitionsPerInterval(10)
                          .eachOfSizeMb(500);

    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // strategy = cost, replicas = 2(on T1), immediate segment loading
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .balancer(new CostBalancerStrategyFactory())
                             .segments(segments)
                             .servers(historicalT11, historicalT12)
                             .rules(DS.WIKI, Load.on(Tier.T1, 2).forever())
                             .immediateSegmentLoading()
                             .dynamicConfig(dynamicConfig)
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that number of replicas assigned is higher than replicationThrottleLimit
    verifyLatestMetricValue("segment/assigned/count", 10L);

    syncInventoryView();
    final DruidServer historicalViewT11 = getInventoryView(historicalT11.getName());
    final DruidServer historicalViewT12 = getInventoryView(historicalT12.getName());
    Assert.assertEquals(10, historicalViewT11.getTotalSegments());
    Assert.assertEquals(10, historicalViewT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(DS.WIKI);
  }

  @Test
  public void testHistoricalsAreNotOverAssigned()
  {
    // some stuff is assigned
    // immediate loading
    // historical is now full
    // but we keep adding stuff to it
    // verify that we assign too many
    // and that historical size was much less
  }

  @Test
  public void testDropHappensAfterAllPrimaryLoads()
  {

  }

  @Test
  public void testLoadingOfFullyReplicatedSegment()
  {
    // a couple of segments
    // all of them are fully loaded
    // something will be chosen for balancing
    // verify that the segments in the load queue do count towards over-replication
    // this will require 2 coordinator runs
  }

}
