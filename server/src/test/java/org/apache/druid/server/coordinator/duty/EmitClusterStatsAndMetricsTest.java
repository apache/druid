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

  @Test
  public void testRunEmitStatsForCompactionCalculatePercentage()
  {
    String groupName = "blah";
    String dataSource = "foo";
    CoordinatorStats stats = new CoordinatorStats();
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING,
        dataSource,
        20
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_AWAITING,
        dataSource,
        2
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_AWAITING,
        dataSource,
        1
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_COMPACTED,
        dataSource,
        30
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_COMPACTED,
        dataSource,
        3
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED,
        dataSource,
        1
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_SKIPPED,
        dataSource,
        40
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_SKIPPED,
        dataSource,
        4
    );
    stats.addToDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED,
        dataSource,
        2
    );

    ArgumentCaptor<ServiceEventBuilder> argumentCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getEmitter()).thenReturn(mockServiceEmitter);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(stats);
    Mockito.when(mockDruidCoordinatorRuntimeParams.getDruidCluster()).thenReturn(mockDruidCluster);
    CoordinatorDuty duty = new EmitClusterStatsAndMetrics(mockDruidCoordinator, groupName, true);
    duty.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockServiceEmitter, Mockito.atLeastOnce()).emit(argumentCaptor.capture());
    List<ServiceEventBuilder> emittedEvents = argumentCaptor.getAllValues();
    boolean foundSegmentCountPercentage = false;
    boolean foundSegmentSizePercentage = false;
    boolean foundIntervalCountPercentage = false;

    for (ServiceEventBuilder eventBuilder : emittedEvents) {
      ServiceMetricEvent serviceMetricEvent = ((ServiceMetricEvent) eventBuilder.build("x", "x"));
      String metric = serviceMetricEvent.getMetric();
      if ("segment/compacted/count/percentage".equals(metric)) {
        foundSegmentCountPercentage = true;
        // Expected value is 3 / (3 + 2 + 4)
        Assert.assertEquals((double) 3 / 9, serviceMetricEvent.getValue());
      } else if ("segment/compacted/bytes/percentage".equals(metric)) {
        foundSegmentSizePercentage = true;
        // Expected value is 30 / (30 + 20 + 40)
        Assert.assertEquals((double) 30 / 90, serviceMetricEvent.getValue());
      } else if ("interval/compacted/count/percentage".equals(metric)) {
        foundIntervalCountPercentage = true;
        // Expected value is 1 / (1 + 1 + 2)
        Assert.assertEquals(0.25, serviceMetricEvent.getValue());
      }
    }
    Assert.assertTrue(foundSegmentCountPercentage);
    Assert.assertTrue(foundSegmentSizePercentage);
    Assert.assertTrue(foundIntervalCountPercentage);
  }
}
