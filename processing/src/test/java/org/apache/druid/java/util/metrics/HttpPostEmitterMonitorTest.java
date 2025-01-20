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
  private HttpPostEmitterMonitor monitor;

  @BeforeEach
  public void setUp()
  {
    mockHttpPostEmitter = mock(HttpPostEmitter.class);
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
    when(mockHttpPostEmitter.getEventsToEmit()).thenReturn(200L);
    when(mockHttpPostEmitter.getLargeEventsToEmit()).thenReturn(75L);
    when(mockHttpPostEmitter.getBuffersToEmit()).thenReturn(30);
    when(mockHttpPostEmitter.getBuffersToReuse()).thenReturn(15);
    when(mockHttpPostEmitter.getBatchFillingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getSuccessfulSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getFailedSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));

    final StubServiceEmitter stubServiceEmitter = new StubServiceEmitter("service", "host");

    assertTrue(monitor.doMonitor(stubServiceEmitter));

    final Map<String, List<StubServiceEmitter.ServiceMetricEventSnapshot>> metricEvents = stubServiceEmitter.getMetricEvents();

    assertMetricValue(metricEvents, "emitter/successfulSending/maxTimeMs", 0);
    assertMetricValue(metricEvents, "emitter/events/emitted/delta", 100L);
    assertMetricValue(metricEvents, "emitter/successfulSending/minTimeMs", 0);
    assertMetricValue(metricEvents, "emitter/buffers/emitQueue", 30);
    assertMetricValue(metricEvents, "emitter/failedSending/minTimeMs", 0);
    assertMetricValue(metricEvents, "emitter/buffers/allocated/delta", 20);
    assertMetricValue(metricEvents, "emitter/batchFilling/maxTimeMs", 0);
    assertMetricValue(metricEvents, "emitter/buffers/dropped/delta", 10);
    assertMetricValue(metricEvents, "emitter/batchFilling/minTimeMs", 0);
    assertMetricValue(metricEvents, "emitter/events/emitQueue", 200L);
    assertMetricValue(metricEvents, "emitter/events/large/emitQueue", 75L);
    assertMetricValue(metricEvents, "emitter/buffers/reuseQueue", 15);
    assertMetricValue(metricEvents, "emitter/buffers/failed/delta", 5);
    assertMetricValue(metricEvents, "emitter/failedSending/maxTimeMs", 0L);
  }

  private void assertMetricValue(Map<String, List<StubServiceEmitter.ServiceMetricEventSnapshot>> metricEvents, String metricName, Number expectedValue)
  {
    assertEquals(metricEvents.get(metricName).get(0).getMetricEvent().getValue().doubleValue(), expectedValue.doubleValue());
  }
}
