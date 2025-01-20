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

package org.apache.druid.java.util.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.core.ConcurrentTimeCounter;
import org.apache.druid.java.util.emitter.core.HttpPostEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpPostEmitterMonitorTest
{
  private HttpPostEmitter mockHttpPostEmitter;
  private ServiceEmitter mockServiceEmitter;
  private HttpPostEmitterMonitor monitor;

  @BeforeEach
  public void setUp()
  {
    mockHttpPostEmitter = mock(HttpPostEmitter.class);
    mockServiceEmitter = mock(ServiceEmitter.class);
    monitor = new HttpPostEmitterMonitor("testFeed", mockHttpPostEmitter,
            ImmutableMap.of("dimensionKey", "dimensionValue"));
  }

  @Test
  public void testDoMonitor()
  {
    when(mockHttpPostEmitter.getTotalEmittedEvents()).thenReturn(100L);
    when(mockHttpPostEmitter.getTotalDroppedBuffers()).thenReturn(10);
    when(mockHttpPostEmitter.getTotalAllocatedBuffers()).thenReturn(20);
    when(mockHttpPostEmitter.getTotalFailedBuffers()).thenReturn(5);
    when(mockHttpPostEmitter.getBatchFillingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getSuccessfulSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getFailedSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getEventsToEmit()).thenReturn(200L);
    when(mockHttpPostEmitter.getLargeEventsToEmit()).thenReturn(75L);
    when(mockHttpPostEmitter.getBuffersToEmit()).thenReturn(30);
    when(mockHttpPostEmitter.getBuffersToReuse()).thenReturn(15);

    final StubServiceEmitter stubServiceEmitter = new StubServiceEmitter("service", "host");

    assertTrue(monitor.doMonitor(stubServiceEmitter));

    Map<String, List<StubServiceEmitter.ServiceMetricEventSnapshot>> metricEvents = stubServiceEmitter.getMetricEvents();

    final String emitter_successful_sending_max_time_ms = "emitter/successfulSending/maxTimeMs";
    assertEquals(metricEvents.get(emitter_successful_sending_max_time_ms).get(0).getMetricEvent().getMetric(), emitter_successful_sending_max_time_ms);
    assertEquals(metricEvents.get(emitter_successful_sending_max_time_ms).get(0).getMetricEvent().getValue(), 0);

    final String emitter_events_emitted_delta = "emitter/events/emitted/delta";
    assertEquals(metricEvents.get(emitter_events_emitted_delta).get(0).getMetricEvent().getMetric(), emitter_events_emitted_delta);
    assertEquals(metricEvents.get(emitter_events_emitted_delta).get(0).getMetricEvent().getValue(), 100l);

    final String emitter_successful_sending_min_time_ms = "emitter/successfulSending/minTimeMs";
    assertEquals(metricEvents.get(emitter_successful_sending_min_time_ms).get(0).getMetricEvent().getMetric(), emitter_successful_sending_min_time_ms);
    assertEquals(metricEvents.get(emitter_successful_sending_min_time_ms).get(0).getMetricEvent().getValue(), 0);

    final String emitter_buffers_emit_queue = "emitter/buffers/emitQueue";
    assertEquals(metricEvents.get(emitter_buffers_emit_queue).get(0).getMetricEvent().getMetric(), emitter_buffers_emit_queue);
    assertEquals(metricEvents.get(emitter_buffers_emit_queue).get(0).getMetricEvent().getValue(), 30);

    final String emitter_failed_sending_min_time_ms = "emitter/failedSending/minTimeMs";
    assertEquals(metricEvents.get(emitter_failed_sending_min_time_ms).get(0).getMetricEvent().getMetric(), emitter_failed_sending_min_time_ms);
    assertEquals(metricEvents.get(emitter_failed_sending_min_time_ms).get(0).getMetricEvent().getValue(), 0);

    final String emitter_buffers_allocated_delta = "emitter/buffers/allocated/delta";
    assertEquals(metricEvents.get(emitter_buffers_allocated_delta).get(0).getMetricEvent().getMetric(), emitter_buffers_allocated_delta);
    assertEquals(metricEvents.get(emitter_buffers_allocated_delta).get(0).getMetricEvent().getValue(), 20);

    final String emitter_batch_filling_max_time_ms = "emitter/batchFilling/maxTimeMs";
    assertEquals(metricEvents.get(emitter_batch_filling_max_time_ms).get(0).getMetricEvent().getMetric(), emitter_batch_filling_max_time_ms);
    assertEquals(metricEvents.get(emitter_batch_filling_max_time_ms).get(0).getMetricEvent().getValue(), 0);

    final String emitter_buffers_dropped_delta = "emitter/buffers/dropped/delta";
    assertEquals(metricEvents.get(emitter_buffers_dropped_delta).get(0).getMetricEvent().getMetric(), emitter_buffers_dropped_delta);
    assertEquals(metricEvents.get(emitter_buffers_dropped_delta).get(0).getMetricEvent().getValue(), 10);

    final String emitter_batch_filling_min_time_ms = "emitter/batchFilling/minTimeMs";
    assertEquals(metricEvents.get(emitter_batch_filling_min_time_ms).get(0).getMetricEvent().getMetric(), emitter_batch_filling_min_time_ms);
    assertEquals(metricEvents.get(emitter_batch_filling_min_time_ms).get(0).getMetricEvent().getValue(), 0);

    final String emitter_events_emit_queue = "emitter/events/emitQueue";
    assertEquals(metricEvents.get(emitter_events_emit_queue).get(0).getMetricEvent().getMetric(), emitter_events_emit_queue);
    assertEquals(metricEvents.get(emitter_events_emit_queue).get(0).getMetricEvent().getValue(), 200l);

    final String emitter_events_large_emit_queue = "emitter/events/large/emitQueue";
    assertEquals(metricEvents.get(emitter_events_large_emit_queue).get(0).getMetricEvent().getMetric(), emitter_events_large_emit_queue);
    assertEquals(metricEvents.get(emitter_events_large_emit_queue).get(0).getMetricEvent().getValue(), 75l);

    final String emitter_buffers_reuse_queue = "emitter/buffers/reuseQueue";
    assertEquals(metricEvents.get(emitter_buffers_reuse_queue).get(0).getMetricEvent().getMetric(), emitter_buffers_reuse_queue);
    assertEquals(metricEvents.get(emitter_buffers_reuse_queue).get(0).getMetricEvent().getValue(), 15);

    final String emitter_buffers_failed_delta = "emitter/buffers/failed/delta";
    assertEquals(metricEvents.get(emitter_buffers_failed_delta).get(0).getMetricEvent().getMetric(), emitter_buffers_failed_delta);
    assertEquals(metricEvents.get(emitter_buffers_failed_delta).get(0).getMetricEvent().getValue(), 5);

    final String emitter_failed_sending_max_time_ms = "emitter/failedSending/maxTimeMs";
    assertEquals(metricEvents.get(emitter_failed_sending_max_time_ms).get(0).getMetricEvent().getMetric(), emitter_failed_sending_max_time_ms);
    assertEquals(metricEvents.get(emitter_failed_sending_max_time_ms).get(0).getMetricEvent().getValue(), 0);
  }
}