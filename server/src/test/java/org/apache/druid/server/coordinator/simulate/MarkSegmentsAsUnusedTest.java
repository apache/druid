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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MarkSegmentsAsUnusedTest extends CoordinatorSimulationBaseTest
{
  @Override
  public void setUp()
  {

  }

  @Test
  public void testSegmentsOvershadowedByZeroReplicaSegmentsAreMarkedAsUnused()
  {
    final long size1TB = 1_000_000;
    final List<DruidServer> servers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      servers.add(createHistorical(i, Tier.T1, size1TB));
    }

    final List<DataSegment> segmentsV0 = Segments.WIKI_10X1D;
    final CoordinatorDynamicConfig dynamicConfig
        = CoordinatorDynamicConfig.builder().withMarkSegmentAsUnusedDelayMillis(0).build();
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withRules(DS.WIKI, Load.on(Tier.T1, 0).forever())
                             .withServers(servers)
                             .withSegments(segmentsV0)
                             .withDynamicConfig(dynamicConfig)
                             .build();
    startSimulation(sim);

    // Run 1: No segment is loaded
    runCoordinatorCycle();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);

    // Add v1 segments to overshadow v0 segments
    final List<DataSegment> segmentsV1 = segmentsV0.stream().map(
        segment -> segment.withVersion(segment.getVersion() + "__new")
    ).collect(Collectors.toList());
    addSegments(segmentsV1);

    // Run 2: nothing is loaded, v0 segments are overshadowed and marked as unused
    runCoordinatorCycle();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);
    verifyValue(Metric.OVERSHADOWED_COUNT, 10L);
  }
}
