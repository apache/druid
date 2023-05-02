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

package org.apache.druid.server.coordinator.duty;

import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CollectSegmentAndServerStatsTest
{
  @Mock
  private DruidCoordinator mockDruidCoordinator;

  @Test
  public void testCollectedSegmentStats()
  {
    DruidCoordinatorRuntimeParams runtimeParams =
        CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                           .withDruidCluster(DruidCluster.EMPTY)
                                           .withUsedSegmentsInTest()
                                           .withSegmentAssignerUsing(null)
                                           .build();

    Mockito.when(mockDruidCoordinator.computeNumsUnavailableUsedSegmentsPerDataSource())
           .thenReturn(Object2IntMaps.singleton("ds", 10));
    Mockito.when(mockDruidCoordinator.computeUnderReplicationCountsPerDataSourcePerTier())
           .thenReturn(Collections.singletonMap("ds", Object2LongMaps.singleton("tier1", 100)));

    CoordinatorDuty duty = new CollectSegmentAndServerStats(mockDruidCoordinator);
    DruidCoordinatorRuntimeParams params = duty.run(runtimeParams);

    CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.hasStat(Stats.Segments.UNAVAILABLE));
    Assert.assertTrue(stats.hasStat(Stats.Segments.UNDER_REPLICATED));
  }

}
