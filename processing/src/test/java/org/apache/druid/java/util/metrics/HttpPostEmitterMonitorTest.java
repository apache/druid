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
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class HttpPostEmitterMonitorTest
{
  private HttpPostEmitter mockHttpPostEmitter;
  private ServiceEmitter mockServiceEmitter;
  private ImmutableMap<String, String> mockExtraDimensions;
  private HttpPostEmitterMonitor monitor;

  @BeforeEach
  public void setUp()
  {
    mockHttpPostEmitter = mock(HttpPostEmitter.class);
    mockServiceEmitter = mock(ServiceEmitter.class);
    mockExtraDimensions = ImmutableMap.of("dimensionKey", "dimensionValue");

    monitor = new HttpPostEmitterMonitor("testFeed", mockHttpPostEmitter, mockExtraDimensions);
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
    when(mockHttpPostEmitter.getLargeEventsToEmit()).thenReturn(50L);
    when(mockHttpPostEmitter.getBuffersToEmit()).thenReturn(30);
    when(mockHttpPostEmitter.getBuffersToReuse()).thenReturn(15);

    assertTrue(monitor.doMonitor(mockServiceEmitter));

    ArgumentCaptor<ServiceMetricEvent.Builder> captor = ArgumentCaptor.forClass(ServiceMetricEvent.Builder.class);
    verify(mockServiceEmitter, atLeastOnce()).emit(captor.capture());
  }

  @Test
  public void testEmitEmittedEvents()
  {
    when(mockHttpPostEmitter.getTotalEmittedEvents()).thenReturn(100L);
    when(mockHttpPostEmitter.getTotalDroppedBuffers()).thenReturn(10);
    when(mockHttpPostEmitter.getTotalAllocatedBuffers()).thenReturn(20);
    when(mockHttpPostEmitter.getTotalFailedBuffers()).thenReturn(5);
    when(mockHttpPostEmitter.getBatchFillingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getSuccessfulSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getFailedSendingTimeCounter()).thenReturn(mock(ConcurrentTimeCounter.class));
    when(mockHttpPostEmitter.getEventsToEmit()).thenReturn(200L);
    when(mockHttpPostEmitter.getLargeEventsToEmit()).thenReturn(50L);
    when(mockHttpPostEmitter.getBuffersToEmit()).thenReturn(30);
    when(mockHttpPostEmitter.getBuffersToReuse()).thenReturn(15);

    monitor.doMonitor(mockServiceEmitter);

    ArgumentCaptor<ServiceMetricEvent.Builder> captor = ArgumentCaptor.forClass(ServiceMetricEvent.Builder.class);
    verify(mockServiceEmitter, atLeastOnce()).emit(captor.capture());
  }


  @Test
  public void testEmitFailedBuffers() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    when(mockHttpPostEmitter.getTotalFailedBuffers()).thenReturn(5);

    // Use reflection to call the private emitFailedBuffers method
    Method emitFailedBuffersMethod = HttpPostEmitterMonitor.class.getDeclaredMethod("emitFailedBuffers", ServiceEmitter.class);
    emitFailedBuffersMethod.setAccessible(true);
    emitFailedBuffersMethod.invoke(monitor, mockServiceEmitter);

    // Capture the emitted metrics
    ArgumentCaptor<ServiceMetricEvent.Builder> captor = ArgumentCaptor.forClass(ServiceMetricEvent.Builder.class);
    verify(mockServiceEmitter, atLeastOnce()).emit(captor.capture());

    // Verify the failed buffers metric
    Assert.assertTrue(captor.getAllValues().stream().anyMatch(builder -> builder.build(ImmutableMap.of()).getMetric().equals("emitter/buffers/failed/delta") && builder.build(ImmutableMap.of()).getValue().equals(5)));
  }

  @Test
  public void testEmitTimeCounterMetrics() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    ConcurrentTimeCounter mockTimeCounter = mock(ConcurrentTimeCounter.class);

    when(mockTimeCounter.getTimeSumAndCountAndReset()).thenReturn(1311768465173141116L);
    when(mockTimeCounter.getAndResetMaxTime()).thenReturn(300);
    when(mockTimeCounter.getAndResetMinTime()).thenReturn(100);

    Method emitTimeCounterMetricsMethod = HttpPostEmitterMonitor.class.getDeclaredMethod("emitTimeCounterMetrics", ServiceEmitter.class, ConcurrentTimeCounter.class, String.class);
    emitTimeCounterMetricsMethod.setAccessible(true);
    emitTimeCounterMetricsMethod.invoke(monitor, mockServiceEmitter, mockTimeCounter, "emitter/test/");

    ArgumentCaptor<ServiceMetricEvent.Builder> captor = ArgumentCaptor.forClass(ServiceMetricEvent.Builder.class);
    verify(mockServiceEmitter, atMost(4)).emit(captor.capture());
  }

  @Test
  public void testEmitTimeCounterMetricsDoesNotEmit() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    ConcurrentTimeCounter mockTimeCounter = mock(ConcurrentTimeCounter.class);

    when(mockTimeCounter.getTimeSumAndCountAndReset()).thenReturn(0L);
    when(mockTimeCounter.getAndResetMaxTime()).thenReturn(null);
    when(mockTimeCounter.getAndResetMinTime()).thenReturn(null);

    Method emitTimeCounterMetricsMethod = HttpPostEmitterMonitor.class.getDeclaredMethod("emitTimeCounterMetrics", ServiceEmitter.class, ConcurrentTimeCounter.class, String.class);
    emitTimeCounterMetricsMethod.setAccessible(true);
    emitTimeCounterMetricsMethod.invoke(monitor, mockServiceEmitter, mockTimeCounter, "emitter/test/");

    ArgumentCaptor<ServiceMetricEvent.Builder> captor = ArgumentCaptor.forClass(ServiceMetricEvent.Builder.class);
    verify(mockServiceEmitter, never()).emit(captor.capture());
  }
}
