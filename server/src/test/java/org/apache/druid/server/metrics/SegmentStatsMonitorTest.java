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


package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SegmentStatsMonitorTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final int PRIORITY = 111;
  private static final String TIER = "tier";

  private DruidServerConfig druidServerConfig;
  private SegmentLoadDropHandler segmentLoadDropMgr;
  private ServiceEmitter serviceEmitter;
  private SegmentStatsMonitor monitor;
  private final SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig();

  @Before
  public void setUp()
  {
    druidServerConfig = Mockito.mock(DruidServerConfig.class);
    segmentLoadDropMgr = Mockito.mock(SegmentLoadDropHandler.class);
    serviceEmitter = Mockito.mock(ServiceEmitter.class);
    monitor = new SegmentStatsMonitor(
        druidServerConfig,
        segmentLoadDropMgr,
        segmentLoaderConfig
    );
    Mockito.when(druidServerConfig.getTier()).thenReturn(TIER);
    Mockito.when(druidServerConfig.getPriority()).thenReturn(PRIORITY);
  }

  @Test(expected = IllegalStateException.class)
  public void testLazyLoadOnStartThrowsException()
  {
    SegmentLoaderConfig segmentLoaderConfig = Mockito.mock(SegmentLoaderConfig.class);
    Mockito.when(segmentLoaderConfig.isLazyLoadOnStart()).thenReturn(true);

    //should throw an exception here
    new SegmentStatsMonitor(druidServerConfig, segmentLoadDropMgr, segmentLoaderConfig);
  }

  @Test
  public void testSimple()
  {
    final SegmentRowCountDistribution segmentRowCountDistribution = new SegmentRowCountDistribution();
    segmentRowCountDistribution.addRowCountToDistribution(100_000L);

    Mockito.when(segmentLoadDropMgr.getAverageNumOfRowsPerSegmentForDatasource())
           .thenReturn(ImmutableMap.of(DATA_SOURCE, 100_000L));
    Mockito.when(segmentLoadDropMgr.getRowCountDistributionPerDatasource())
           .thenReturn(ImmutableMap.of(DATA_SOURCE, segmentRowCountDistribution));

    ArgumentCaptor<ServiceEventBuilder<ServiceMetricEvent>> eventArgumentCaptor = ArgumentCaptor.forClass(
        ServiceEventBuilder.class);
    monitor.doMonitor(serviceEmitter);
    Mockito.verify(serviceEmitter, Mockito.atLeastOnce()).emit(eventArgumentCaptor.capture());

    List<Map<String, Object>> eventsAsMaps = getEventMaps(eventArgumentCaptor.getAllValues());
    Map<String, Map<String, Object>> actual = metricKeyedMap(eventsAsMaps);

    List<ServiceEventBuilder<ServiceMetricEvent>> expectedEvents = new ArrayList<>();
    expectedEvents.add(averageRowCountEvent(100_000L));
    expectedEvents.add(rowCountRangeEvent("1-10k", 0));
    expectedEvents.add(rowCountRangeEvent("10k-2M", 1));
    expectedEvents.add(rowCountRangeEvent("2M-4M", 0));
    expectedEvents.add(rowCountRangeEvent("4M-6M", 0));
    expectedEvents.add(rowCountRangeEvent("6M-8M", 0));
    expectedEvents.add(rowCountRangeEvent("8M-10M", 0));
    expectedEvents.add(rowCountRangeEvent("10M+", 0));

    List<Map<String, Object>> expectedEventsAsMap = getEventMaps(expectedEvents);
    Map<String, Map<String, Object>> expected = metricKeyedMap(expectedEventsAsMap);

    Assert.assertEquals("different number of metrics were returned", expected.size(), actual.size());
    for (Map.Entry<String, Map<String, Object>> expectedKeyedEntry : expected.entrySet()) {
      Map<String, Object> actualValue = actual.get(expectedKeyedEntry.getKey());
      assertMetricMapsEqual(expectedKeyedEntry.getKey(), expectedKeyedEntry.getValue(), actualValue);
    }
  }

  @Test
  public void testZeroAndTombstoneDistribution()
  {
    final SegmentRowCountDistribution segmentRowCountDistribution = new SegmentRowCountDistribution();
    segmentRowCountDistribution.addRowCountToDistribution(100_000L);
    segmentRowCountDistribution.addRowCountToDistribution(0L);
    segmentRowCountDistribution.addTombstoneToDistribution();
    segmentRowCountDistribution.addTombstoneToDistribution();

    Mockito.when(segmentLoadDropMgr.getAverageNumOfRowsPerSegmentForDatasource())
           .thenReturn(ImmutableMap.of(DATA_SOURCE, 50_000L));
    Mockito.when(segmentLoadDropMgr.getRowCountDistributionPerDatasource())
           .thenReturn(ImmutableMap.of(DATA_SOURCE, segmentRowCountDistribution));

    ArgumentCaptor<ServiceEventBuilder<ServiceMetricEvent>> eventArgumentCaptor = ArgumentCaptor.forClass(
        ServiceEventBuilder.class);
    monitor.doMonitor(serviceEmitter);
    Mockito.verify(serviceEmitter, Mockito.atLeastOnce()).emit(eventArgumentCaptor.capture());

    List<Map<String, Object>> eventsAsMaps = getEventMaps(eventArgumentCaptor.getAllValues());
    Map<String, Map<String, Object>> actual = metricKeyedMap(eventsAsMaps);

    List<ServiceEventBuilder<ServiceMetricEvent>> expectedEvents = new ArrayList<>();
    expectedEvents.add(averageRowCountEvent(50_000L));
    expectedEvents.add(rowCountRangeEvent("0", 1));
    expectedEvents.add(rowCountRangeEvent("Tombstone", 2));
    expectedEvents.add(rowCountRangeEvent("1-10k", 0));
    expectedEvents.add(rowCountRangeEvent("10k-2M", 1));
    expectedEvents.add(rowCountRangeEvent("2M-4M", 0));
    expectedEvents.add(rowCountRangeEvent("4M-6M", 0));
    expectedEvents.add(rowCountRangeEvent("6M-8M", 0));
    expectedEvents.add(rowCountRangeEvent("8M-10M", 0));
    expectedEvents.add(rowCountRangeEvent("10M+", 0));

    List<Map<String, Object>> expectedEventsAsMap = getEventMaps(expectedEvents);
    Map<String, Map<String, Object>> expected = metricKeyedMap(expectedEventsAsMap);

    Assert.assertEquals("different number of metrics were returned", expected.size(), actual.size());
    for (Map.Entry<String, Map<String, Object>> expectedKeyedEntry : expected.entrySet()) {
      Map<String, Object> actualValue = actual.get(expectedKeyedEntry.getKey());
      assertMetricMapsEqual(expectedKeyedEntry.getKey(), expectedKeyedEntry.getValue(), actualValue);
    }
  }

  private void assertMetricMapsEqual(String messagePrefix, Map<String, Object> expected, Map<String, Object> actual)
  {
    Assert.assertEquals("different number of expected values for metrics", expected.size(), actual.size());
    for (Map.Entry<String, Object> expectedMetricEntry : expected.entrySet()) {
      Assert.assertEquals(
          messagePrefix + " " + expectedMetricEntry.getKey(),
          expectedMetricEntry.getValue(),
          actual.get(expectedMetricEntry.getKey())
      );
    }
  }

  @Nonnull
  private List<Map<String, Object>> getEventMaps(List<ServiceEventBuilder<ServiceMetricEvent>> eventBuilders)
  {
    return eventBuilders.stream()
                        .map(eventBuilder -> new HashMap<>(eventBuilder.build(ImmutableMap.of()).toMap()))
                        .peek(mappedValues -> mappedValues.remove("timestamp"))
                        .collect(Collectors.toList());
  }

  private Map<String, Map<String, Object>> metricKeyedMap(List<Map<String, Object>> eventsAsMaps)
  {
    return eventsAsMaps.stream()
                       .collect(
                           Collectors.toMap(eventasdf -> {
                             String metricName = eventasdf.get("metric").toString();
                             String range = eventasdf.getOrDefault("range", "").toString();
                             return metricName + range;
                           }, Function.identity())
                       );
  }

  private ServiceEventBuilder<ServiceMetricEvent> averageRowCountEvent(Number value)
  {
    return new ServiceMetricEvent.Builder().setDimension(DruidMetrics.DATASOURCE, DATA_SOURCE)
                                           .setDimension("tier", TIER)
                                           .setDimension("priority", String.valueOf(PRIORITY))
                                           .build("segment/rowCount/avg", value);
  }

  private ServiceEventBuilder<ServiceMetricEvent> rowCountRangeEvent(String range, Number value)
  {
    return new ServiceMetricEvent.Builder().setDimension(DruidMetrics.DATASOURCE, DATA_SOURCE)
                                           .setDimension("tier", TIER)
                                           .setDimension("priority", String.valueOf(PRIORITY))
                                           .setDimension("range", range)
                                           .build("segment/rowCount/range/count", value);
  }
}
