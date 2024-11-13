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
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventBufferTest
{
  private EventBuffer<ServiceMetricEvent> buffer;
  private final int capacity = 5;

  @BeforeEach
  public void setUp()
  {
    buffer = new EventBuffer<>(ServiceMetricEvent.class, capacity);
  }

  @Test
  public void testInsertAndSize()
  {
    fill(capacity);
    buffer.push(ServiceMetricEvent.builder().setMetric("my/other/test/metric", capacity).build(ImmutableMap.of()));
    Assertions.assertEquals(capacity, buffer.getSize(), "Size should not exceed capacity");
  }

  @Test
  public void testExtractAndSize()
  {
    int numPush = capacity + 1;
    fill(numPush);

    ServiceMetricEvent[] extractedEvents = buffer.extract();

    for (int i = numPush - 1; i >= numPush - capacity; --i) {
      ServiceMetricEvent event = extractedEvents[i % capacity];

      Assertions.assertEquals("my/test/metric", event.getMetric(), "Metric name incorrect");
      Assertions.assertEquals(i, event.getValue(), "Metric value should equal index");
    }
    Assertions.assertEquals(0, buffer.getSize(), "EventBuffer::extract() should clear size");
  }

  private void fill(int n)
  {
    for (int i = 0; i < n; ++i) {
      ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
      builder.setMetric("my/test/metric", i);
      buffer.push(builder.build(ImmutableMap.of()));
      Assertions.assertEquals(Math.min(i + 1, capacity), buffer.getSize(), "Size should not exceed capacity");
    }
  }
}
