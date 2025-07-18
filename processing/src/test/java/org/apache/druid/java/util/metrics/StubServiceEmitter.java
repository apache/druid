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

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Test implementation of {@link ServiceEmitter} that collects emitted metrics
 * and alerts in lists.
 */
public class StubServiceEmitter extends ServiceEmitter implements MetricsVerifier
{
  private final Queue<Event> events = new ConcurrentLinkedDeque<>();
  private final Queue<AlertEvent> alertEvents = new ConcurrentLinkedDeque<>();
  private final ConcurrentHashMap<String, Queue<ServiceMetricEvent>> metricEvents = new ConcurrentHashMap<>();

  public StubServiceEmitter()
  {
    super("testing", "localhost", null);
  }

  public StubServiceEmitter(String service, String host)
  {
    super(service, host, null);
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      metricEvents.computeIfAbsent(metricEvent.getMetric(), name -> new ConcurrentLinkedDeque<>())
                  .add(metricEvent.copy());
    } else if (event instanceof AlertEvent) {
      alertEvents.add((AlertEvent) event);
    }
    events.add(event);
  }

  /**
   * Gets all the events emitted since the previous {@link #flush()}.
   */
  public List<Event> getEvents()
  {
    return new ArrayList<>(events);
  }

  public int getNumEmittedEvents()
  {
    return events.size();
  }

  /**
   * Gets all the metric events emitted for the given metric name since the previous {@link #flush()}.
   *
   * @return List of events emitted for the given metric.
   */
  public List<ServiceMetricEvent> getMetricEvents(String metricName)
  {
    final Queue<ServiceMetricEvent> metricEventQueue = metricEvents.get(metricName);
    return metricEventQueue == null ? List.of() : List.copyOf(metricEventQueue);
  }

  /**
   * Gets all the alerts emitted since the previous {@link #flush()}.
   */
  public List<AlertEvent> getAlerts()
  {
    return new ArrayList<>(alertEvents);
  }

  @Override
  public List<Number> getMetricValues(
      String metricName,
      Map<String, Object> dimensionFilters
  )
  {
    final List<Number> values = new ArrayList<>();
    final Queue<ServiceMetricEvent> events =
        metricEvents.getOrDefault(metricName, new ArrayDeque<>());
    final Map<String, Object> filters =
        dimensionFilters == null ? Collections.emptyMap() : dimensionFilters;
    for (ServiceMetricEvent event : events) {
      final Map<String, Object> userDims = event.getUserDims();
      boolean match = filters.keySet().stream()
                             .map(d -> filters.get(d).equals(userDims.get(d)))
                             .reduce((a, b) -> a && b)
                             .orElse(true);
      if (match) {
        values.add(event.getValue());
      }
    }

    return values;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void flush()
  {
    events.clear();
    alertEvents.clear();
    metricEvents.clear();
  }

  @Override
  public void close()
  {
  }
}
