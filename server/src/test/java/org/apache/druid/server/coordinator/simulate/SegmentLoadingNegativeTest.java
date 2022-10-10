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

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
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

}
