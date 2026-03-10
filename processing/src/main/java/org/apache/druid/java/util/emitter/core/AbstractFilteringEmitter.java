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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;
import java.util.Set;

/**
 * Base class for emitters that support metric-name-based filtering.
 *
 * <p>The base implementation provides a single filtering gate in {@link #emit(Event)} and then
 * delegates actual emission to {@link #emitFilteredEvent(Event)}.
 */
public abstract class AbstractFilteringEmitter implements Emitter, MetricFilteringEmitter
{
  private final boolean shouldFilterMetrics;
  private final Set<String> allowedMetricNames;

  protected AbstractFilteringEmitter(final boolean shouldFilterMetrics, final Set<String> allowedMetricNames)
  {
    this.shouldFilterMetrics = shouldFilterMetrics;
    this.allowedMetricNames = Set.copyOf(allowedMetricNames);
  }

  protected static Set<String> loadAllowedMetricNames(
      final boolean shouldFilterMetrics,
      final ObjectMapper objectMapper,
      final Optional<String> metricSpecPath,
      final String defaultMetricSpecPath,
      final MetricAllowlistParser parser
  )
  {
    if (!shouldFilterMetrics) {
      return Set.of();
    }

    return metricSpecPath
        .map(path -> MetricAllowlistLoader.loadAllowlistFromFile(objectMapper, path, parser))
        .orElseGet(() -> MetricAllowlistLoader.loadAllowlistFromClasspath(objectMapper, defaultMetricSpecPath, parser));
  }

  @Override
  public final void emit(final Event event)
  {
    preEmit(event);
    if (shouldFilterMetrics && shouldFilterEvent(event)) {
      return;
    }
    emitFilteredEvent(event);
  }

  @Override
  public boolean shouldFilterOutMetric(final String metricName)
  {
    return !allowedMetricNames.contains(metricName);
  }

  protected boolean isShouldFilterMetrics()
  {
    return shouldFilterMetrics;
  }

  protected Set<String> getAllowedMetricNames()
  {
    return allowedMetricNames;
  }

  /**
   * Returns whether this event should be dropped before emission.
   *
   * <p>Implementations should apply emitter-specific event handling semantics here.
   */
  protected abstract boolean shouldFilterEvent(Event event);

  /**
   * Hook for pre-emission checks that must run before metric filtering.
   */
  protected void preEmit(final Event event)
  {
    // default no-op
  }

  /**
   * Emits an event that already passed filtering (or is not a metric event).
   */
  protected abstract void emitFilteredEvent(Event event);
}
