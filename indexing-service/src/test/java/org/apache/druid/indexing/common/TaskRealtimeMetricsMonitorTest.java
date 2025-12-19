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
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.InputRowThrownAwayReason;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
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

  @Mock(answer = Answers.RETURNS_MOCKS)
  private RowIngestionMeters rowIngestionMeters;
  @Mock
  private ServiceEmitter emitter;
  private Map<String, ServiceMetricEvent> emittedEvents;
  private TaskRealtimeMetricsMonitor target;

  @Before
  public void setUp()
  {
    emittedEvents = new HashMap<>();
    segmentGenerationMetrics = new SegmentGenerationMetrics();
    Mockito.doCallRealMethod().when(emitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          emittedEvents.put(e.getMetric(), e);
          return null;
        })
        .when(emitter).emit(ArgumentMatchers.any(Event.class));
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
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      Assert.assertEquals(TAGS, sme.getUserDims().get(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testdoMonitorWithoutTagsShouldNotEmitTags()
  {
    target = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        rowIngestionMeters,
        createMetricEventBuilder()
    );
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      Assert.assertFalse(sme.getUserDims().containsKey(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testMessageGapAggStats()
  {
    target = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        rowIngestionMeters,
        createMetricEventBuilder()
    );

    target.doMonitor(emitter);
    Assert.assertFalse(emittedEvents.containsKey("ingest/events/minMessageGap"));
    Assert.assertFalse(emittedEvents.containsKey("ingest/events/maxMessageGap"));
    Assert.assertFalse(emittedEvents.containsKey("ingest/events/avgMessageGap"));

    emittedEvents.clear();
    segmentGenerationMetrics.reportMessageGap(1);
    target.doMonitor(emitter);

    Assert.assertTrue(emittedEvents.containsKey("ingest/events/minMessageGap"));
    Assert.assertTrue(emittedEvents.containsKey("ingest/events/maxMessageGap"));
    Assert.assertTrue(emittedEvents.containsKey("ingest/events/avgMessageGap"));
  }

  @Test
  public void testThrownAwayEmitsReasonDimension()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();
    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.BEFORE_MIN_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.AFTER_MAX_MESSAGE_TIME);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);

    List<ServiceMetricEvent> allEmittedEvents = new ArrayList<>();
    ServiceEmitter captureEmitter = Mockito.mock(ServiceEmitter.class);
    Mockito.doCallRealMethod().when(captureEmitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          allEmittedEvents.add(e);
          return null;
        })
        .when(captureEmitter).emit(ArgumentMatchers.any(Event.class));

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    monitor.doMonitor(captureEmitter);

    Map<String, Long> thrownAwayByReason = new HashMap<>();
    for (ServiceMetricEvent event : allEmittedEvents) {
      if ("ingest/events/thrownAway".equals(event.getMetric())) {
        Object reason = event.getUserDims().get("reason");
        thrownAwayByReason.put(reason.toString(), event.getValue().longValue());
      }
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
    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);

    List<ServiceMetricEvent> allEmittedEvents = new ArrayList<>();
    ServiceEmitter captureEmitter = Mockito.mock(ServiceEmitter.class);
    Mockito.doCallRealMethod().when(captureEmitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          allEmittedEvents.add(e);
          return null;
        })
        .when(captureEmitter).emit(ArgumentMatchers.any(Event.class));

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    monitor.doMonitor(captureEmitter);

    List<String> emittedReasons = new ArrayList<>();
    for (ServiceMetricEvent event : allEmittedEvents) {
      if ("ingest/events/thrownAway".equals(event.getMetric())) {
        Object reason = event.getUserDims().get("reason");
        emittedReasons.add(reason.toString());
      }
    }

    // Only reasons with non-zero counts should be emitted
    Assert.assertEquals(2, emittedReasons.size());
    Assert.assertTrue(emittedReasons.contains("null"));
    Assert.assertTrue(emittedReasons.contains("filtered"));
    Assert.assertFalse(emittedReasons.contains("beforeMinMessageTime"));
    Assert.assertFalse(emittedReasons.contains("afterMaxMessageTime"));
  }

  @Test
  public void testThrownAwayReasonDeltaAcrossMonitorCalls()
  {
    SimpleRowIngestionMeters realMeters = new SimpleRowIngestionMeters();

    List<ServiceMetricEvent> allEmittedEvents = new ArrayList<>();
    ServiceEmitter captureEmitter = Mockito.mock(ServiceEmitter.class);
    Mockito.doCallRealMethod().when(captureEmitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          allEmittedEvents.add(e);
          return null;
        })
        .when(captureEmitter).emit(ArgumentMatchers.any(Event.class));

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(
        segmentGenerationMetrics,
        realMeters,
        createMetricEventBuilder()
    );

    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    monitor.doMonitor(captureEmitter);

    long firstCallNullCount = 0;
    for (ServiceMetricEvent event : allEmittedEvents) {
      if ("ingest/events/thrownAway".equals(event.getMetric()) 
          && "null".equals(event.getUserDims().get("reason"))) {
        firstCallNullCount = event.getValue().longValue();
      }
    }
    Assert.assertEquals(2, firstCallNullCount);

    allEmittedEvents.clear();
    realMeters.incrementThrownAway(InputRowThrownAwayReason.NULL_OR_EMPTY_RECORD);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    realMeters.incrementThrownAway(InputRowThrownAwayReason.FILTERED);
    monitor.doMonitor(captureEmitter);

    // Find counts from second call - should be deltas only
    Map<String, Long> secondCallCounts = new HashMap<>();
    for (ServiceMetricEvent event : allEmittedEvents) {
      if ("ingest/events/thrownAway".equals(event.getMetric())) {
        Object reason = event.getUserDims().get("reason");
        secondCallCounts.put(reason.toString(), event.getValue().longValue());
      }
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
