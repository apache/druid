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

package org.apache.druid.testing.embedded;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.metrics.LatchableEmitter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class EmbeddedMetricEventPredicates
{
  private static final long WAIT_TIMEOUT_MILLIS = 60_000;

  private EmbeddedMetricEventPredicates()
  {
  }

  public static void waitForMetric(
      final LatchableEmitter emitter,
      final String metric,
      final Predicate<Long> valuePredicate
  )
  {
    waitForMetric(emitter, metric, Map.of(), valuePredicate);
  }

  public static void waitForMetric(
      final LatchableEmitter emitter,
      final String metric,
      final Map<String, Object> dimensions,
      final Predicate<Long> valuePredicate
  )
  {
    emitter.waitForEvent(
        event -> matches(event, metric, dimensions)
                 && valuePredicate.test(((ServiceMetricEvent) event).getValue().longValue()),
        WAIT_TIMEOUT_MILLIS
    );
  }

  public static void waitForMetricCount(
      final LatchableEmitter emitter,
      final String metric,
      final Map<String, Object> dimensions,
      final String predicateDimension,
      final Predicate<Object> dimensionPredicate,
      final long count
  )
  {
    final AtomicLong matchingCount = new AtomicLong();
    emitter.waitForEvent(
        event -> matches(event, metric, dimensions)
                 && dimensionPredicate.test(
                     ((ServiceMetricEvent) event).getUserDims().get(predicateDimension)
                 )
                 && matchingCount.incrementAndGet() >= count,
        WAIT_TIMEOUT_MILLIS
    );
  }

  private static boolean matches(final Event event, final String metric, final Map<String, Object> dimensions)
  {
    if (!(event instanceof ServiceMetricEvent)) {
      return false;
    }
    final ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
    return metric.equals(metricEvent.getMetric())
           && dimensions.entrySet().stream().allMatch(
               entry -> entry.getValue().equals(metricEvent.getUserDims().get(entry.getKey()))
           );
  }
}
