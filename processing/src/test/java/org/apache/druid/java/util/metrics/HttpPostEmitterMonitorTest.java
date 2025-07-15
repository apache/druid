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

    stubServiceEmitter.verifyValue("emitter/successfulSending/maxTimeMs", 0);
    stubServiceEmitter.verifyValue("emitter/events/emitted/delta", 100L);
    stubServiceEmitter.verifyValue("emitter/successfulSending/minTimeMs", 0);
    stubServiceEmitter.verifyValue("emitter/buffers/emitQueue", 30);
    stubServiceEmitter.verifyValue("emitter/failedSending/minTimeMs", 0);
    stubServiceEmitter.verifyValue("emitter/buffers/allocated/delta", 20);
    stubServiceEmitter.verifyValue("emitter/batchFilling/maxTimeMs", 0);
    stubServiceEmitter.verifyValue("emitter/buffers/dropped/delta", 10);
    stubServiceEmitter.verifyValue("emitter/batchFilling/minTimeMs", 0);
    stubServiceEmitter.verifyValue("emitter/events/emitQueue", 200L);
    stubServiceEmitter.verifyValue("emitter/events/large/emitQueue", 75L);
    stubServiceEmitter.verifyValue("emitter/buffers/reuseQueue", 15);
    stubServiceEmitter.verifyValue("emitter/buffers/failed/delta", 5);
    stubServiceEmitter.verifyValue("emitter/failedSending/maxTimeMs", 0);
  }
}
