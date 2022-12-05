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

import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class EmitClusterStatsAndMetricsTest
{
  @Mock
  private ServiceEmitter mockServiceEmitter;
  @Mock
  private DruidCoordinator mockDruidCoordinator;
  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;
  @Mock
  CoordinatorStats mockCoordinatorStats;
  @Mock
  DruidCluster mockDruidCluster;
  @Mock
  MetadataRuleManager mockMetadataRuleManager;

  @Test
  public void testRunOnlyEmitStatsForHistoricalDuties()
  {
    ArgumentCaptor<ServiceEventBuilder> argumentCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(mockCoordinatorStats);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getDruidCluster()).thenReturn(mockDruidCluster);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getDatabaseRuleManager()).thenReturn(mockMetadataRuleManager);
    Mockito.when(mockDruidCoordinator.computeNumsUnavailableUsedSegmentsPerDataSource()).thenReturn(Object2IntMaps.emptyMap());
    Mockito.when(mockDruidCoordinator.computeUnderReplicationCountsPerDataSourcePerTier()).thenReturn(ImmutableMap.of());
    CoordinatorDuty duty = new EmitClusterStatsAndMetrics(mockDruidCoordinator, DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP, false);
    duty.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockServiceEmitter, Mockito.atLeastOnce()).emit(argumentCaptor.capture());
    List<ServiceEventBuilder> emittedEvents = argumentCaptor.getAllValues();
    boolean foundCompactMetric = false;
    boolean foundHistoricalDutyMetric = false;
    for (ServiceEventBuilder eventBuilder : emittedEvents) {
      ServiceMetricEvent serviceMetricEvent = ((ServiceMetricEvent) eventBuilder.build("x", "x"));
      String metric = serviceMetricEvent.getMetric();
      if ("segment/overShadowed/count".equals(metric)) {
        foundHistoricalDutyMetric = true;
      } else if ("compact/task/count".equals(metric)) {
        foundCompactMetric = true;
      }
      String dutyGroup = (String) serviceMetricEvent.getUserDims().get("dutyGroup");
      Assert.assertNotNull(dutyGroup);
      Assert.assertEquals(DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP, dutyGroup);
    }
    Assert.assertTrue(foundHistoricalDutyMetric);
    Assert.assertFalse(foundCompactMetric);
  }

  @Test
  public void testRunEmitStatsForCompactionWhenHaveCompactSegmentDuty()
  {
    String groupName = "blah";
    ArgumentCaptor<ServiceEventBuilder> argumentCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(mockCoordinatorStats);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getDruidCluster()).thenReturn(mockDruidCluster);
    CoordinatorDuty duty = new EmitClusterStatsAndMetrics(mockDruidCoordinator, groupName, true);
    duty.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockServiceEmitter, Mockito.atLeastOnce()).emit(argumentCaptor.capture());
    List<ServiceEventBuilder> emittedEvents = argumentCaptor.getAllValues();
    boolean foundCompactMetric = false;
    boolean foundHistoricalDutyMetric = false;
    for (ServiceEventBuilder eventBuilder : emittedEvents) {
      ServiceMetricEvent serviceMetricEvent = ((ServiceMetricEvent) eventBuilder.build("x", "x"));
      String metric = serviceMetricEvent.getMetric();
      if ("segment/overShadowed/count".equals(metric)) {
        foundHistoricalDutyMetric = true;
      } else if ("compact/task/count".equals(metric)) {
        foundCompactMetric = true;
      }
      String dutyGroup = (String) serviceMetricEvent.getUserDims().get("dutyGroup");
      Assert.assertNotNull(dutyGroup);
      Assert.assertEquals(groupName, dutyGroup);
    }
    Assert.assertFalse(foundHistoricalDutyMetric);
    Assert.assertTrue(foundCompactMetric);
  }
}
