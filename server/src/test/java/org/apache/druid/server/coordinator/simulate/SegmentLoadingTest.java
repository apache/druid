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

import java.util.Collections;
import java.util.List;

/**
 * Coordinator simulation test to verify behaviour of segment loading.
 */
public class SegmentLoadingTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;
  private DruidServer historicalT13;
  private DruidServer historicalT21;
  private DruidServer historicalT22;

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
    historicalT13 = createHistorical(3, Tier.T1, 10_000);

    historicalT21 = createHistorical(1, Tier.T2, 10_000);
    historicalT22 = createHistorical(2, Tier.T2, 10_000);
  }

  @Test
  public void testSecondReplicaOnAnyTierIsThrottled()
  {
    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // historicals = 2(in T1)
    // replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withDynamicConfig(dynamicConfig)
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that that replicationThrottleLimit is honored
    verifyValue(Metric.ASSIGNED_COUNT, 2L);

    loadQueuedSegments();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(2, historicalT12.getTotalSegments());
  }

  @Test
  public void testLoadingDoesNotOverassignHistorical()
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
                             .withImmediateSegmentLoading(false)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that the number of segments assigned is within the historical capacity
    verifyValue(Metric.ASSIGNED_COUNT, 2L);
    loadQueuedSegments();
    Assert.assertEquals(2, historicalT11.getTotalSegments());
  }

  @Test
  public void testTierShiftDoesNotCauseUnderReplication()
  {
    // disable balancing
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 10);

    // historicals = 2(in T1) + 3(in T2)
    // segments = 1, replicas = 3(T2)
    final DataSegment segment = segments.get(0);
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Collections.singletonList(segment))
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T2, 3).forever())
                             .withServers(
                                 historicalT11,
                                 historicalT12,
                                 historicalT21,
                                 historicalT22
                             )
                             .build();

    // At the start, T1 has the segment fully replicated
    historicalT11.addDataSegment(segment);
    historicalT12.addDataSegment(segment);

    // Run 1: Nothing is dropped from T1 but 2 replicas are assigned to T2
    startSimulation(sim);
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2), 2L);

    // Run 2: Replicas still queued
    // nothing new is assigned to T2, nothing is dropped from T1
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2), 0L);

    loadQueuedSegments();
    Assert.assertEquals(2, getNumLoadedSegments(historicalT21, historicalT22));
    Assert.assertEquals(2, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 3: total loaded replicas (4) > total required replicas (3)
    // no server to assign third replica in T2, one replica is dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filter(DruidMetrics.TIER, Tier.T1), 1L);
    verifyValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2), 0L);

    loadQueuedSegments();
    Assert.assertEquals(2, getNumLoadedSegments(historicalT21, historicalT22));
    Assert.assertEquals(1, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 4: another server added to T2, third replica can now be assigned
    // nothing is dropped from T1
    final DruidServer historicalT23 = createHistorical(3, Tier.T2, 10_000);
    addServer(historicalT23);
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2), 1L);

    loadQueuedSegments();
    Assert.assertEquals(3, getNumLoadedSegments(historicalT21, historicalT22, historicalT23));
    Assert.assertEquals(1, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 5: segment is fully replicated on T2, all replicas will now be dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filter(DruidMetrics.TIER, Tier.T1), 1L);
    verifyNoEvent(Metric.ASSIGNED_COUNT);

    loadQueuedSegments();
    Assert.assertEquals(3, getNumLoadedSegments(historicalT21, historicalT22, historicalT23));
    Assert.assertEquals(0, getNumLoadedSegments(historicalT11, historicalT12));
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testTierAddDoesNotCauseUnderReplication()
  {
    // disable balancing
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 10);

    // historicals = 2(in T1) + 1(in T2)
    // current replicas = 2(T1)
    // required replicas = 1(T1) + 1(T2)
    final DataSegment segment = segments.get(0);
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Collections.singletonList(segment))
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever())
                             .withServers(historicalT11, historicalT12, historicalT21)
                             .build();

    // At the start, T1 has 2 replicas of the segment
    historicalT11.addDataSegment(segment);
    historicalT12.addDataSegment(segment);

    // Run 1: Nothing is dropped from T1 but 1 replica is assigned to T2
    startSimulation(sim);
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2), 1L);

    // Run 2: Replicas still queued
    // nothing new is assigned to T2, nothing is dropped from T1
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    verifyNoEvent(Metric.ASSIGNED_COUNT);

    loadQueuedSegments();
    Assert.assertEquals(1, getNumLoadedSegments(historicalT21));
    Assert.assertEquals(2, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 3: total loaded replicas (3) > total required replicas (2)
    // one replica is dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filter(DruidMetrics.TIER, Tier.T1), 1L);

    loadQueuedSegments();
    Assert.assertEquals(1, getNumLoadedSegments(historicalT21));
    Assert.assertEquals(1, getNumLoadedSegments(historicalT11, historicalT12));
  }

  @Test
  public void testImmediateLoadingDoesNotOverassignHistorical()
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

    // The historical is only assigned segments that it can load
    verifyValue(Metric.ASSIGNED_COUNT, 2L);
    Assert.assertEquals(2, historicalT11.getTotalSegments());
  }

  @Test
  public void testFirstReplicaOnTierIsNotThrottled()
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

    // Verify that primary replica on T2 are not throttled
    verifyValue(
        Metric.ASSIGNED_COUNT,
        filter(DruidMetrics.TIER, Tier.T2),
        10L
    );

    loadQueuedSegments();

    verifyDatasourceIsFullyLoaded(datasource);
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT21.getTotalSegments());
  }

  @Test
  public void testLoadOfFullyReplicatedSegmentGetsCancelled()
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

    // Add a new historical with the second replica of all the segments
    addServer(historicalT13);
    segments.forEach(historicalT13::addDataSegment);

    runCoordinatorCycle();

    // Verify that the loading of the extra replicas is cancelled
    verifyValue(Metric.CANCELLED_LOADS, 10L);
    verifyValue(
        Metric.LOAD_QUEUE_COUNT,
        filter(DruidMetrics.SERVER, historicalT12.getName()),
        0
    );
  }

  @Test
  public void testBroadcastIsNotThrottled()
  {
    // disable balancing, unlimited load queue, replicationThrottleLimit = 1
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 0);

    // historicals = 3(in T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12, historicalT13)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Broadcast.forever())
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that all the segments are broadcast to all historicals
    // irrespective of throttle limit
    verifyValue(Metric.BROADCAST_LOADS, filter(DruidMetrics.DATASOURCE, DS.WIKI), 30L);
    verifyNoEvent(Metric.BROADCAST_DROPS);
  }

  private int getNumLoadedSegments(DruidServer... servers)
  {
    int numLoaded = 0;
    for (DruidServer server : servers) {
      numLoaded += server.getTotalSegments();
    }
    return numLoaded;
  }

}
