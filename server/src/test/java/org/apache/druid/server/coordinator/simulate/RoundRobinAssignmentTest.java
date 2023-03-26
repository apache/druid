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

import java.util.ArrayList;
import java.util.List;

public class RoundRobinAssignmentTest extends CoordinatorSimulationBaseTest
{
  private static final long SIZE_1TB = 1_000_000;

  private List<DruidServer> historicals;

  @Override
  public void setUp()
  {
    historicals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }
  }

  @Test
  public void testSegmentsAreAssignedUniformly()
  {
    CoordinatorDynamicConfig config =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(0)
                                .withMaxSegmentsInNodeLoadingQueue(0)
                                .withReplicationThrottleLimit(20000)
                                .withUseRoundRobinSegmentAssignment(true)
                                .build();

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(config)
                             .withBalancer("random")
                             .withRules(DS.WIKI, Load.on(Tier.T1, 2).forever())
                             .withServers(historicals)
                             .withSegments(Segments.WIKI_10X100D)
                             .build();
    startSimulation(sim);

    // Run 1: all segments are assigned and loaded
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyValue(Metric.ASSIGNED_COUNT, 2000L);

    for (DruidServer historical : historicals) {
      Assert.assertEquals(200, historical.getTotalSegments());
    }
  }

  @Test
  public void testMultipleDatasourceSegmentsAreAssignedUniformly()
  {
    final List<DataSegment> segments = new ArrayList<>(Segments.WIKI_10X100D);
    segments.addAll(Segments.KOALA_100X100D);

    CoordinatorDynamicConfig config =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(0)
                                .withMaxSegmentsInNodeLoadingQueue(0)
                                .withReplicationThrottleLimit(20000)
                                .withUseRoundRobinSegmentAssignment(true)
                                .build();

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(config)
                             .withBalancer("random")
                             .withRules(DS.WIKI, Load.on(Tier.T1, 3).forever())
                             .withRules(DS.KOALA, Load.on(Tier.T1, 1).forever())
                             .withServers(historicals)
                             .withSegments(segments)
                             .build();
    startSimulation(sim);

    // Run 1: all segments are assigned and loaded
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyValue(Metric.ASSIGNED_COUNT, 13000L);

    for (DruidServer historical : historicals) {
      Assert.assertEquals(1300, historical.getTotalSegments());
    }
  }

}
