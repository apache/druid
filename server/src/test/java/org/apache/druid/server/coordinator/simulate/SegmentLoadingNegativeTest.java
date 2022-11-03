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
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Contains negative tests that verify existing erroneous behaviour of segment
 * loading. The underlying issues should be fixed and the modified tests
 * should be migrated to {@link SegmentLoadingTest}.
 * <p>
 * Identified issues:
 * <a href="https://github.com/apache/druid/issues/12881">Apache #12881</a>
 */
public class SegmentLoadingNegativeTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;
  private DruidServer historicalT21;

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
    historicalT21 = createHistorical(1, Tier.T2, 10_000);
  }

  /**
   * Correct behaviour: replicationThrottleLimit should not be violated even if
   * segment loading is fast.
   * <p>
   * Fix Apache #12881 to fix this test.
   */
  @Test
  public void testImmediateLoadingViolatesThrottleLimit()
  {
    // Disable balancing, infinite load queue size, replicationThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // historicals = 2(in T1), segments = 10*1day
    // replicas = 2(on T1), immediate segment loading
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withImmediateSegmentLoading(true)
                             .withDynamicConfig(dynamicConfig)
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that number of replicas assigned exceeds the replicationThrottleLimit
    verifyValue(Metric.ASSIGNED_COUNT, 10L);

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

  /**
   * Correct behaviour: The first replica on any tier should not be throttled.
   * <p>
   * Fix Apache #12881 to fix this test.
   */
  @Test
  public void testFirstReplicaOnAnyTierIsThrottled()
  {
    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // historicals = 1(in T1) + 1(in T2)
    // replicas = 1(on T1) + 1(on T2)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT21)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(
                                 datasource,
                                 Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever()
                             )
                             .build();

    // Put the first replica of all the segments on T1
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that num replicas assigned to T2 are equal to the replicationthrottleLimit
    verifyValue(
        Metric.ASSIGNED_COUNT,
        filter(DruidMetrics.TIER, Tier.T2),
        2L
    );

    loadQueuedSegments();

    verifyDatasourceIsFullyLoaded(datasource);
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(2, historicalT21.getTotalSegments());
  }

  /**
   * Correct behaviour: Historical should not get overassigned even if loading is fast.
   * <p>
   * Fix Apache #12881 to fix this test.
   */
  @Test
  public void testImmediateLoadingOverassignsHistorical()
  {
    // historicals = 1(in T1), size 1 GB
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 1000);

    // disable balancing, unlimited load queue, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 10);

    // segments = 10*1day, size 500 MB
    // strategy = cost, replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withImmediateSegmentLoading(true)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    // The historical is assigned several segments but loads only upto its capacity
    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    Assert.assertEquals(2, historicalT11.getTotalSegments());
  }

  /**
   * Correct behaviour: For a fully replicated segment, items that are in the load
   * queue should get cancelled so that the coordinator does not have to wait
   * for the loads to finish and then take remedial action.
   * <p>
   * Fix Apache #12881 to fix this test case.
   */
  @Test
  public void testLoadOfFullyReplicatedSegmentIsNotCancelled()
  {
    // disable balancing, unlimited load queue, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 10);

    // historicals = 2(in T1), replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that there are segments in the load queue
    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    verifyValue(
        Metric.LOAD_QUEUE_COUNT,
        filter(DruidMetrics.SERVER, historicalT12.getName()),
        10
    );

    // Put the second replica of all the segments on histT12
    segments.forEach(historicalT12::addDataSegment);

    runCoordinatorCycle();

    // Verify that the segments are still in the load queue
    verifyValue(
        Metric.LOAD_QUEUE_COUNT,
        filter(DruidMetrics.SERVER, historicalT12.getName()),
        10
    );
  }

}
