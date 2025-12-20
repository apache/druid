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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  @Before
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
    Assert.assertFalse(events.isEmpty());
    for (ServiceMetricEvent sme : events) {
      Assert.assertEquals(TAGS, sme.getUserDims().get(DruidMetrics.TAGS));
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
    Assert.assertFalse(events.isEmpty());
    for (ServiceMetricEvent sme : events) {
      Assert.assertFalse(sme.getUserDims().containsKey(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testMessageGapAggStats()
  {
    target.doMonitor(emitter);
    Assert.assertTrue(emitter.getMetricEvents("ingest/events/minMessageGap").isEmpty());
    Assert.assertTrue(emitter.getMetricEvents("ingest/events/maxMessageGap").isEmpty());
    Assert.assertTrue(emitter.getMetricEvents("ingest/events/avgMessageGap").isEmpty());

    emitter.flush();
    segmentGenerationMetrics.reportMessageGap(1);
    target.doMonitor(emitter);

    Assert.assertFalse(emitter.getMetricEvents("ingest/events/minMessageGap").isEmpty());
    Assert.assertFalse(emitter.getMetricEvents("ingest/events/maxMessageGap").isEmpty());
    Assert.assertFalse(emitter.getMetricEvents("ingest/events/avgMessageGap").isEmpty());
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
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);

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

    Assert.assertEquals(Long.valueOf(2), thrownAwayByReason.get("null"));
    Assert.assertEquals(Long.valueOf(3), thrownAwayByReason.get("beforeMinMessageTime"));
    Assert.assertEquals(Long.valueOf(1), thrownAwayByReason.get("afterMaxMessageTime"));
    Assert.assertEquals(Long.valueOf(4), thrownAwayByReason.get("filtered"));
  }

  @Test
  public void testThrownAwayReasonDimensionOnlyEmittedWhenNonZero()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);

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
    Assert.assertEquals(2, thrownAwayByReason.size());
    Assert.assertTrue(thrownAwayByReason.containsKey("null"));
    Assert.assertTrue(thrownAwayByReason.containsKey("filtered"));
    Assert.assertFalse(thrownAwayByReason.containsKey("beforeMinMessageTime"));
    Assert.assertFalse(thrownAwayByReason.containsKey("afterMaxMessageTime"));
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
    Assert.assertEquals(2, firstCallNullCount);

    emitter.flush();
    realMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);
    realMeters.incrementThrownAway(InputRowFilterResult.FILTERED);
    monitor.doMonitor(emitter);

    // Find counts from second call - should be deltas only
    Map<String, Long> secondCallCounts = new HashMap<>();
    for (ServiceMetricEvent event : emitter.getMetricEvents("ingest/events/thrownAway")) {
      Object reason = event.getUserDims().get("reason");
      secondCallCounts.put(reason.toString(), event.getValue().longValue());
    }

    // Should emit only the delta (1 more NULL, 2 new FILTERED)
    Assert.assertEquals(Long.valueOf(1), secondCallCounts.get("null"));
    Assert.assertEquals(Long.valueOf(2), secondCallCounts.get("filtered"));
  }

  private ServiceMetricEvent.Builder createMetricEventBuilder()
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    MonitorUtils.addDimensionsToBuilder(builder, DIMENSIONS);
    builder.setDimensionIfNotNull(DruidMetrics.TAGS, TAGS);
    return builder;
  }
}
