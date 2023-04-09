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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class EmitClusterStatsAndMetricsTest
{
  @Mock
  private ServiceEmitter mockServiceEmitter;
  @Mock
  private DruidCoordinator mockDruidCoordinator;
  @Mock
  DruidCluster mockDruidCluster;

  @Test
  public void testRunOnlyEmitStatsForHistoricalDuties()
  {
    final DruidCoordinatorRuntimeParams runtimeParams =
        CoordinatorRuntimeParamsTestHelpers.newBuilder()
                                           .withDruidCluster(mockDruidCluster)
                                           .withUsedSegmentsInTest()
                                           .withCoordinatorStats(new CoordinatorRunStats())
                                           .build();

    Mockito.when(mockDruidCoordinator.computeNumsUnavailableUsedSegmentsPerDataSource())
           .thenReturn(Object2IntMaps.emptyMap());
    Mockito.when(mockDruidCoordinator.computeUnderReplicationCountsPerDataSourcePerTier())
           .thenReturn(Collections.emptyMap());

    CoordinatorDuty duty = new EmitClusterStatsAndMetrics(
        mockDruidCoordinator,
        DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP,
        mockServiceEmitter
    );
    duty.run(runtimeParams);

    ArgumentCaptor<ServiceEventBuilder> argumentCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.verify(mockServiceEmitter, Mockito.atLeastOnce()).emit(argumentCaptor.capture());
  }

}
