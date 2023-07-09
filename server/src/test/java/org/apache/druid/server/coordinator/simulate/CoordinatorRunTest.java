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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorTest;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Performs basic coordinator testing using simulations.
 * <p>
 * All the tests in {@link DruidCoordinatorTest} should eventually be moved here.
 */
public class CoordinatorRunTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
  }

  @Test
  public void testDutiesRunOnEmptyCluster()
  {
    CoordinatorSimulation sim = CoordinatorSimulation.builder().build();
    startSimulation(sim);

    runCoordinatorCycle();
    verifyEmitted(Metric.DUTY_GROUP_RUN_TIME, filter(Dimension.DUTY_GROUP, "HistoricalManagementDuties"), 1);
    verifyEmitted(Metric.DUTY_GROUP_RUN_TIME, filter(Dimension.DUTY_GROUP, "MetadataStoreManagementDuties"), 1);
  }

  @Test
  public void testReplicationStatusAfterRun()
  {
    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .build();
    startSimulation(sim);

    // Run coordinator and load segments
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyDatasourceIsFullyLoaded(datasource);

    final DataSegment segment = segments.get(0);

    // Verify that replication state is not updated yet
    final DruidCoordinator coordinator = druidCoordinator();
    Object2IntMap<String> unavailableSegmentCounts
        = coordinator.getDatasourceToUnavailableSegmentCount();
    Assert.assertEquals(1, unavailableSegmentCounts.size());
    Assert.assertEquals(segments.size(), unavailableSegmentCounts.getInt(datasource));

    Assert.assertEquals(Integer.valueOf(2), coordinator.getReplicationFactor(segment.getId()));

    // Verify that replication state is updated after next coordinator run
    runCoordinatorCycle();

    unavailableSegmentCounts = coordinator.getDatasourceToUnavailableSegmentCount();
    Assert.assertEquals(1, unavailableSegmentCounts.size());
    Assert.assertEquals(0, unavailableSegmentCounts.getInt(datasource));

    final Map<String, Object2LongMap<String>> tierToUnderReplicatedCounts
        = coordinator.getTierToDatasourceToUnderReplicatedCount(false);
    Assert.assertNotNull(tierToUnderReplicatedCounts);
    Assert.assertEquals(1, tierToUnderReplicatedCounts.size());

    Object2LongMap<String> datasourceToUnderReplicatedCounts = tierToUnderReplicatedCounts.get(Tier.T1);
    Assert.assertNotNull(datasourceToUnderReplicatedCounts);
    Assert.assertEquals(1, datasourceToUnderReplicatedCounts.size());
    Assert.assertTrue(datasourceToUnderReplicatedCounts.containsKey(datasource));
    Assert.assertEquals(0L, datasourceToUnderReplicatedCounts.getLong(datasource));

    Map<String, Object2LongMap<String>> tierToUnderReplicatedUsingClusterView
        = coordinator.getTierToDatasourceToUnderReplicatedCount(true);
    Assert.assertNotNull(tierToUnderReplicatedCounts);
    Assert.assertEquals(1, tierToUnderReplicatedCounts.size());

    Object2LongMap<String> datasourceToUnderReplicatedUsingClusterView
        = tierToUnderReplicatedUsingClusterView.get(Tier.T1);
    Assert.assertNotNull(datasourceToUnderReplicatedUsingClusterView);
    Assert.assertEquals(1, datasourceToUnderReplicatedUsingClusterView.size());
    Assert.assertTrue(datasourceToUnderReplicatedUsingClusterView.containsKey(datasource));
    Assert.assertEquals(0L, datasourceToUnderReplicatedUsingClusterView.getLong(datasource));
  }

}
