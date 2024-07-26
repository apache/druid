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
    // historicals = 2(in T1)
    // replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withDynamicConfig(withReplicationThrottleLimit(2))
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

    // segments = 10*1day, size 500 MB
    // strategy = cost, replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withDynamicConfig(withReplicationThrottleLimit(10))
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
    // historicals = 2(in T1) + 3(in T2)
    // segments = 1, replicas = 3(T2)
    final DataSegment segment = segments.get(0);
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Collections.singletonList(segment))
                             .withDynamicConfig(withReplicationThrottleLimit(10))
                             .withRules(datasource, Load.on(Tier.T2, 3).forever())
                             .withServers(historicalT11, historicalT12, historicalT21, historicalT22)
                             .build();

    // At the start, T1 has the segment fully replicated
    historicalT11.addDataSegment(segment);
    historicalT12.addDataSegment(segment);

    // Run 1: Nothing is dropped from T1 but 2 replicas are assigned to T2
    startSimulation(sim);
    runCoordinatorCycle();

    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 2L);

    // Run 2: Replicas still queued
    // nothing new is assigned to T2, nothing is dropped from T1
    runCoordinatorCycle();

    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 0L);

    loadQueuedSegments();
    Assert.assertEquals(2, getNumLoadedSegments(historicalT21, historicalT22));
    Assert.assertEquals(2, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 3: total loaded replicas (4) > total required replicas (3) > total loadable replicas (2)
    // no server to assign third replica in T2, but all replicas are dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filterByTier(Tier.T1), 2L);
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 0L);

    loadQueuedSegments();
    Assert.assertEquals(2, getNumLoadedSegments(historicalT21, historicalT22));
    Assert.assertEquals(0, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 4: Add 3rd server to T2, third replica can now be assigned
    // Add 3rd server to T1 with replica loaded, but it will not be dropped
    final DruidServer historicalT23 = createHistorical(3, Tier.T2, 10_000);
    addServer(historicalT23);
    historicalT13.addDataSegment(segment);
    addServer(historicalT13);
    runCoordinatorCycle();

    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 1L);

    loadQueuedSegments();
    Assert.assertEquals(3, getNumLoadedSegments(historicalT21, historicalT22, historicalT23));
    Assert.assertEquals(1, historicalT13.getTotalSegments());

    // Run 5: segment is fully replicated on T2, remaining replica will now be dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filterByTier(Tier.T1), 1L);
    verifyNotEmitted(Metric.ASSIGNED_COUNT);

    loadQueuedSegments();
    Assert.assertEquals(3, getNumLoadedSegments(historicalT21, historicalT22, historicalT23));
    Assert.assertEquals(0, getNumLoadedSegments(historicalT11, historicalT12, historicalT13));
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testTierAddDoesNotCauseUnderReplication()
  {
    // historicals = 2(in T1) + 1(in T2)
    // current replicas = 2(T1)
    // required replicas = 1(T1) + 1(T2)
    final DataSegment segment = segments.get(0);
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Collections.singletonList(segment))
                             .withDynamicConfig(withReplicationThrottleLimit(10))
                             .withRules(datasource, Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever())
                             .withServers(historicalT11, historicalT12, historicalT21)
                             .build();

    // At the start, T1 has 2 replicas of the segment
    historicalT11.addDataSegment(segment);
    historicalT12.addDataSegment(segment);

    // Run 1: Nothing is dropped from T1 but 1 replica is assigned to T2
    startSimulation(sim);
    runCoordinatorCycle();

    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 1L);

    // Run 2: Replicas still queued
    // nothing new is assigned to T2, nothing is dropped from T1
    runCoordinatorCycle();

    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyNotEmitted(Metric.ASSIGNED_COUNT);

    loadQueuedSegments();
    Assert.assertEquals(1, getNumLoadedSegments(historicalT21));
    Assert.assertEquals(2, getNumLoadedSegments(historicalT11, historicalT12));

    // Run 3: total loaded replicas (3) > total required replicas (2)
    // one replica is dropped from T1
    runCoordinatorCycle();

    verifyValue(Metric.DROPPED_COUNT, filterByTier(Tier.T1), 1L);

    loadQueuedSegments();
    Assert.assertEquals(1, getNumLoadedSegments(historicalT21));
    Assert.assertEquals(1, getNumLoadedSegments(historicalT11, historicalT12));
  }

  @Test
  public void testImmediateLoadingDoesNotOverassignHistorical()
  {
    // historicals = 1(in T1), size 1 GB
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 1000);

    // segments = 10*1day, size 500 MB
    // strategy = cost, replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withDynamicConfig(withReplicationThrottleLimit(10))
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
  public void testMaxSegmentsInNodeLoadingQueue()
  {
    // disable balancing, maxSegmentsInNodeLoadingQueue = 5, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(0)
                                .withReplicationThrottleLimit(10)
                                .withMaxSegmentsInNodeLoadingQueue(5)
                                .withUseRoundRobinSegmentAssignment(false)
                                .withSmartSegmentLoading(false)
                                .build();

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Run 1: Only some segments are assigned as load queue size is limited
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 5L);

    // Run 2: No more segments are assigned as queue is already full
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 0L);

    // Instantly load some of the queued segments on the historical
    historicalT11.addDataSegment(segments.get(9));
    historicalT11.addDataSegment(segments.get(8));

    // Run 3: No segments are assigned, extra loads are cancelled
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 0L);
    verifyValue(Metric.CANCELLED_ACTIONS, 2L);

    // Run 4: Some segments are assigned as load queue is still partially full
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 2L);
  }

  @Test
  public void testFirstReplicaOnTierIsNotThrottled()
  {
    // historicals = 1(in T1) + 1(in T2)
    // replicas = 1(on T1) + 1(on T2)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT21)
                             .withDynamicConfig(withReplicationThrottleLimit(2))
                             .withRules(datasource, Load.on(Tier.T1, 1).andOn(Tier.T2, 1).forever())
                             .build();

    // Put the first replica of all the segments on T1
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that primary replica on T2 are not throttled
    verifyValue(Metric.ASSIGNED_COUNT, filterByTier(Tier.T2), 10L);

    loadQueuedSegments();

    verifyDatasourceIsFullyLoaded(datasource);
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT21.getTotalSegments());
  }

  @Test
  public void testImmediateLoadingDoesNotViolateThrottleLimit()
  {
    // historicals = 2(in T1), segments = 10*1day
    // replicas = 2(on T1), immediate segment loading
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withImmediateSegmentLoading(true)
                             .withDynamicConfig(withReplicationThrottleLimit(2))
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that number of replicas does not exceed the replicationThrottleLimit
    verifyValue(Metric.ASSIGNED_COUNT, 2L);

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(2, historicalT12.getTotalSegments());
  }

  @Test
  public void testLoadOfFullyReplicatedSegmentGetsCancelled()
  {
    // historicals = 2(in T1), replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withDynamicConfig(withReplicationThrottleLimit(10))
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
        filterByServer(historicalT12),
        10L
    );

    // Add a new historical with the second replica of all the segments
    addServer(historicalT13);
    segments.forEach(historicalT13::addDataSegment);

    runCoordinatorCycle();

    // Verify that the loading of the extra replicas is cancelled
    verifyValue(Metric.CANCELLED_ACTIONS, 10L);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT12), 0L);
  }

  @Test
  public void testBroadcastReplicasAreNotThrottled()
  {
    // historicals = 3(in T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12, historicalT13)
                             .withDynamicConfig(withReplicationThrottleLimit(0))
                             .withRules(datasource, Broadcast.forever())
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that all the segments are broadcast to all historicals
    // irrespective of throttle limit
    verifyValue(Metric.ASSIGNED_COUNT, filterByDatasource(DS.WIKI), 30L);
    verifyNotEmitted(Metric.DROPPED_COUNT);
  }

  @Test
  public void testReplicasAreNotAssignedIfTierIsBusy()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withDynamicConfig(withReplicationThrottleLimit(5))
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .build();

    startSimulation(sim);

    // All segments are loaded on histT11
    segments.forEach(historicalT11::addDataSegment);

    // Run 1: Some replicas are assigned to histT12
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 5L);

    // Run 2: No more replicas are assigned because tier is already busy with replication
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 0L);

    // Run 3: Remaining replicas are assigned
    loadQueuedSegments();
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 5L);

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
  }

  @Test
  public void testAllLoadsOnDecommissioningServerAreCancelled()
  {
    final CoordinatorDynamicConfig dynamicConfig = withReplicationThrottleLimit(100);

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .build();

    startSimulation(sim);

    // All segments are loaded on histT11
    segments.forEach(historicalT11::addDataSegment);

    // Run 1: Some replicas are assigned to histT12
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 10L);

    // Run 2: histT12 is marked as decommissioning, all loads on it are cancelled
    setDynamicConfig(
        CoordinatorDynamicConfig.builder()
                                .withDecommissioningNodes(Collections.singleton(historicalT12.getName()))
                                .build(dynamicConfig)
    );
    runCoordinatorCycle();
    verifyValue(Metric.CANCELLED_ACTIONS, 10L);

    loadQueuedSegments();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());
  }

  @Test
  public void testLoadOfUnusedSegmentIsCancelled()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Run 1: All segments are assigned
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 10L);

    // Run 2: Update rules, all segments are marked as unused
    setRetentionRules(datasource, Drop.forever());
    runCoordinatorCycle();
    verifyValue(Metric.DELETED_COUNT, 10L);

    // Run 3: Loads of unused segments are cancelled
    runCoordinatorCycle();
    verifyValue(Metric.LOAD_QUEUE_COUNT, 0L);
    verifyValue(Metric.CANCELLED_ACTIONS, 10L);
  }

  @Test
  public void testSegmentsAreDroppedFromFullServersFirst()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withServers(historicalT11, historicalT12)
                             .withDynamicConfig(withReplicationThrottleLimit(100))
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withRules(DS.KOALA, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // All wiki segments are loaded on both historicals
    addSegments(segments);
    segments.forEach(historicalT11::addDataSegment);
    segments.forEach(historicalT12::addDataSegment);

    // Add extra koala segments to histT11
    final List<DataSegment> koalaSegments = Segments.KOALA_100X100D.subList(0, 2);
    addSegments(koalaSegments);
    koalaSegments.forEach(historicalT11::addDataSegment);

    // More segments are dropped from histT11 as it was more full before the run
    runCoordinatorCycle();
    verifyValue(Metric.DROP_QUEUE_COUNT, filterByServer(historicalT11), 6L);
    verifyValue(Metric.DROP_QUEUE_COUNT, filterByServer(historicalT12), 4L);

    loadQueuedSegments();
    Assert.assertEquals(historicalT11.getCurrSize(), historicalT12.getCurrSize());
  }

  private int getNumLoadedSegments(DruidServer... servers)
  {
    int numLoaded = 0;
    for (DruidServer server : servers) {
      numLoaded += server.getTotalSegments();
    }
    return numLoaded;
  }

  /**
   * Creates a dynamic config with unlimited load queue, balancing disabled and
   * the given {@code replicationThrottleLimit}.
   */
  private CoordinatorDynamicConfig withReplicationThrottleLimit(int replicationThrottleLimit)
  {
    return CoordinatorDynamicConfig.builder()
                                   .withSmartSegmentLoading(false)
                                   .withMaxSegmentsToMove(0)
                                   .withMaxSegmentsInNodeLoadingQueue(0)
                                   .withReplicationThrottleLimit(replicationThrottleLimit)
                                   .build();
  }
}
