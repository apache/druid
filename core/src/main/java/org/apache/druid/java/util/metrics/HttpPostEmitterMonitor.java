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

public class HttpPostEmitterMonitor extends FeedDefiningMonitor
{
  private final HttpPostEmitter httpPostEmitter;
  private final ImmutableMap<String, String> extraDimensions;
  private final ServiceMetricEvent.Builder builder;
  private long lastTotalEmittedEvents = 0;
  private int lastTotalDroppedBuffers = 0;
  private int lastTotalAllocatedBuffers = 0;
  private int lastTotalFailedBuffers = 0;

  public HttpPostEmitterMonitor(
      String feed,
      HttpPostEmitter httpPostEmitter,
      ImmutableMap<String, String> extraDimensions
  )
  {
    super(feed);
    this.httpPostEmitter = httpPostEmitter;
    this.extraDimensions = extraDimensions;
    this.builder = builder();
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitEmittedEvents(emitter);
    emitDroppedBuffers(emitter);
    emitAllocatedBuffers(emitter);
    emitFailedBuffers(emitter);

    emitTimeCounterMetrics(emitter, httpPostEmitter.getBatchFillingTimeCounter(), "emitter/batchFilling/");
    emitTimeCounterMetrics(emitter, httpPostEmitter.getSuccessfulSendingTimeCounter(), "emitter/successfulSending/");
    emitTimeCounterMetrics(emitter, httpPostEmitter.getFailedSendingTimeCounter(), "emitter/failedSending/");

    emitter.emit(builder.build("emitter/events/emitQueue", httpPostEmitter.getEventsToEmit()));
    emitter.emit(builder.build("emitter/events/large/emitQueue", httpPostEmitter.getLargeEventsToEmit()));

    emitter.emit(builder.build("emitter/buffers/emitQueue", httpPostEmitter.getBuffersToEmit()));

    emitter.emit(builder.build("emitter/buffers/reuseQueue", httpPostEmitter.getBuffersToReuse()));

    return true;
  }

  private void emitEmittedEvents(ServiceEmitter emitter)
  {
    long newTotalEmittedEvents = httpPostEmitter.getTotalEmittedEvents();
    long emittedEventsDelta = newTotalEmittedEvents - lastTotalEmittedEvents;
    emitter.emit(builder.build("emitter/events/emitted/delta", emittedEventsDelta));
    lastTotalEmittedEvents = newTotalEmittedEvents;
  }

  private void emitDroppedBuffers(ServiceEmitter emitter)
  {
    int newTotalDroppedBuffers = httpPostEmitter.getTotalDroppedBuffers();
    int droppedBuffersDelta = newTotalDroppedBuffers - lastTotalDroppedBuffers;
    emitter.emit(builder.build("emitter/buffers/dropped/delta", droppedBuffersDelta));
    lastTotalDroppedBuffers = newTotalDroppedBuffers;
  }

  private void emitAllocatedBuffers(ServiceEmitter emitter)
  {
    int newTotalAllocatedBuffers = httpPostEmitter.getTotalAllocatedBuffers();
    int allocatedBuffersDelta = newTotalAllocatedBuffers - lastTotalAllocatedBuffers;
    emitter.emit(builder.build("emitter/buffers/allocated/delta", allocatedBuffersDelta));
    lastTotalAllocatedBuffers = newTotalAllocatedBuffers;
  }

  private void emitFailedBuffers(ServiceEmitter emitter)
  {
    int newTotalFailedBuffers = httpPostEmitter.getTotalFailedBuffers();
    int failedBuffersDelta = newTotalFailedBuffers - lastTotalFailedBuffers;
    emitter.emit(builder.build("emitter/buffers/failed/delta", failedBuffersDelta));
    lastTotalFailedBuffers = newTotalFailedBuffers;
  }

  private void emitTimeCounterMetrics(ServiceEmitter emitter, ConcurrentTimeCounter timeCounter, String metricNameBase)
  {
    long timeSumAndCount = timeCounter.getTimeSumAndCountAndReset();
    int timeSum = ConcurrentTimeCounter.timeSum(timeSumAndCount);
    int count = ConcurrentTimeCounter.count(timeSumAndCount);
    if (count != 0) {
      emitter.emit(builder.build(metricNameBase + "timeMsSum", timeSum));
      emitter.emit(builder.build(metricNameBase + "count", count));
    }
    Integer maxTime = timeCounter.getAndResetMaxTime();
    if (maxTime != null) {
      emitter.emit(builder.build(metricNameBase + "maxTimeMs", maxTime));
    }
    Integer minTime = timeCounter.getAndResetMinTime();
    if (minTime != null) {
      emitter.emit(builder.build(metricNameBase + "minTimeMs", minTime));
    }
  }

  @Override
  protected ServiceMetricEvent.Builder builder()
  {
    ServiceMetricEvent.Builder builder = super.builder();
    extraDimensions.forEach(builder::setDimension);
    return builder;
  }
}
