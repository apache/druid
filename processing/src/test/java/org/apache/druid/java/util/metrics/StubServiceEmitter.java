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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test implementation of {@link ServiceEmitter} that collects emitted metrics
 * and alerts in lists.
 */
public class StubServiceEmitter extends ServiceEmitter implements MetricsVerifier
{
  private final List<Event> events = new ArrayList<>();
  private final List<AlertEvent> alertEvents = new ArrayList<>();
  private final Map<String, List<ServiceMetricEvent>> metricEvents = new HashMap<>();

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
      metricEvents.computeIfAbsent(metricEvent.getMetric(), name -> new ArrayList<>())
                  .add(metricEvent);
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
    return events;
  }

  /**
   * Gets all the metric events emitted since the previous {@link #flush()}.
   *
   * @return Map from metric name to list of events emitted for that metric.
   */
  public Map<String, List<ServiceMetricEvent>> getMetricEvents()
  {
    return metricEvents;
  }

  /**
   * Gets all the alerts emitted since the previous {@link #flush()}.
   */
  public List<AlertEvent> getAlerts()
  {
    return alertEvents;
  }

  @Override
  public List<Number> getMetricValues(
      String metricName,
      Map<String, Object> dimensionFilters
  )
  {
    final List<Number> values = new ArrayList<>();
    final List<ServiceMetricEvent> events =
        metricEvents.getOrDefault(metricName, Collections.emptyList());
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
