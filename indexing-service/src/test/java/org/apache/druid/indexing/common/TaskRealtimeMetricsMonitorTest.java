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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskRealtimeMetricsMonitorTest
{
  private static final Map<String, String[]> DIMENSIONS = ImmutableMap.of(
      "dim1",
      new String[]{"v1", "v2"},
      "dim2",
      new String[]{"vv"}
  );

  private static final Map<String, Object> TAGS = ImmutableMap.of("author", "Author Name", "version", 10);

  private SegmentGenerationMetrics segmentGenerationMetrics;
  private RowIngestionMeters rowIngestionMeters;
  private StubServiceEmitter emitter;
  private TaskRealtimeMetricsMonitor target;

  @BeforeEach
  public void setUp()
  {
    segmentGenerationMetrics = new SegmentGenerationMetrics();
    rowIngestionMeters = new SimpleRowIngestionMeters();
    emitter = new StubServiceEmitter();
    target = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        rowIngestionMeters,
        createMetricEventBuilder()
    );
  }

  @Test
  public void testdoMonitorShouldEmitUserProvidedTags()
  {
    target.doMonitor(emitter);

    List<ServiceMetricEvent> events = emitter.getMetricEvents("ingest/events/unparseable");
    Assertions.assertFalse(events.isEmpty());
    for (ServiceMetricEvent sme : events) {
      Assertions.assertEquals(TAGS, sme.getUserDims().get(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testdoMonitorWithoutTagsShouldNotEmitTags()
  {
    ServiceMetricEvent.Builder builderWithoutTags = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builderWithoutTags, DIMENSIONS);

    target = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        rowIngestionMeters,
        builderWithoutTags
    );
    target.doMonitor(emitter);

    List<ServiceMetricEvent> events = emitter.getMetricEvents("ingest/events/unparseable");
    Assertions.assertFalse(events.isEmpty());
    for (ServiceMetricEvent sme : events) {
      Assertions.assertFalse(sme.getUserDims().containsKey(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testMessageGapAggStats()
  {
    target.doMonitor(emitter);
    Assertions.assertTrue(emitter.getMetricEvents("ingest/events/minMessageGap").isEmpty());
    Assertions.assertTrue(emitter.getMetricEvents("ingest/events/maxMessageGap").isEmpty());
    Assertions.assertTrue(emitter.getMetricEvents("ingest/events/avgMessageGap").isEmpty());

    emitter.flush();
    segmentGenerationMetrics.reportMessageGap(1);
    target.doMonitor(emitter);

    Assertions.assertFalse(emitter.getMetricEvents("ingest/events/minMessageGap").isEmpty());
    Assertions.assertFalse(emitter.getMetricEvents("ingest/events/maxMessageGap").isEmpty());
    Assertions.assertFalse(emitter.getMetricEvents("ingest/events/avgMessageGap").isEmpty());
  }

  @Test
  public void testThrownAwayEmitsReasonDimension()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    monitor.doMonitor(emitter);

    Map<String, Long> thrownAwayByReason = new HashMap<>();
    for (ServiceMetricEvent event : emitter.getMetricEvents("ingest/events/thrownAway")) {
      Object reason = event.getUserDims().get("reason");
      thrownAwayByReason.put(reason.toString(), event.getValue().longValue());
    }

    Assertions.assertEquals(Long.valueOf(2), thrownAwayByReason.get("null"));
    Assertions.assertEquals(Long.valueOf(3), thrownAwayByReason.get("beforeMinimumMessageTime"));
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get("afterMaximumMessageTime"));
    Assertions.assertEquals(Long.valueOf(4), thrownAwayByReason.get("filtered"));
  }

  @Test
  public void testThrownAwayReasonDimensionOnlyEmittedWhenNonZero()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    monitor.doMonitor(emitter);

    Map<String, Long> thrownAwayByReason = new HashMap<>();
    for (ServiceMetricEvent event : emitter.getMetricEvents("ingest/events/thrownAway")) {
      Object reason = event.getUserDims().get("reason");
      thrownAwayByReason.put(reason.toString(), event.getValue().longValue());
    }

    // Only reasons with non-zero counts should be emitted
    Assertions.assertEquals(2, thrownAwayByReason.size());
    Assertions.assertTrue(thrownAwayByReason.containsKey("null"));
    Assertions.assertTrue(thrownAwayByReason.containsKey("filtered"));
    Assertions.assertFalse(thrownAwayByReason.containsKey("beforeMinimumMessageTime"));
    Assertions.assertFalse(thrownAwayByReason.containsKey("afterMaximumMessageTime"));
  }

  @Test
  public void testThrownAwayReasonDeltaAcrossMonitorCalls()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    monitor.doMonitor(emitter);

    long firstCallNullCount = 0;
    for (ServiceMetricEvent event : emitter.getMetricEvents("ingest/events/thrownAway")) {
      if ("null".equals(event.getUserDims().get("reason"))) {
        firstCallNullCount = event.getValue().longValue();
      }
    }
    Assertions.assertEquals(2, firstCallNullCount);

    emitter.flush();
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    realMeters.incrementThrownAway(InputRowFilterResult.CUSTOM_FILTER);
    monitor.doMonitor(emitter);

    // Find counts from second call - should be deltas only
    Map<String, Long> secondCallCounts = new HashMap<>();
    for (ServiceMetricEvent event : emitter.getMetricEvents("ingest/events/thrownAway")) {
      Object reason = event.getUserDims().get("reason");
      secondCallCounts.put(reason.toString(), event.getValue().longValue());
    }

    // Should emit only the delta (1 more NULL, 2 new FILTERED)
    Assertions.assertEquals(Long.valueOf(1), secondCallCounts.get("null"));
    Assertions.assertEquals(Long.valueOf(2), secondCallCounts.get("filtered"));
  }

  private ServiceMetricEvent.Builder createMetricEventBuilder()
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, DIMENSIONS);
    builder.setDimensionIfNotNull(DruidMetrics.TAGS, TAGS);
    return builder;
  }
}
