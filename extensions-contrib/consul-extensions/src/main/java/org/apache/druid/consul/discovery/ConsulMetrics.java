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

package org.apache.druid.consul.discovery;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

/**
 * Minimal helper to emit metrics if a ServiceEmitter is available.
 */
final class ConsulMetrics
{
  private ConsulMetrics()
  {
  }

  static void emitCount(ServiceEmitter emitter, String metric, String... dims)
  {
    if (emitter == null) {
      return;
    }
    ServiceMetricEvent.Builder b = ServiceMetricEvent.builder();
    if (dims != null && dims.length % 2 == 1) {
      // ignore last odd key with no value to avoid exceptions
    }
    if (dims != null) {
      for (int i = 0; i + 1 < dims.length; i += 2) {
        b.setDimension(dims[i], dims[i + 1]);
      }
    }
    emitter.emit(b.setMetric(metric, 1));
  }

  static void emitTimer(ServiceEmitter emitter, String metric, long millis, String... dims)
  {
    if (emitter == null) {
      return;
    }
    ServiceMetricEvent.Builder b = ServiceMetricEvent.builder();
    if (dims != null && dims.length % 2 == 1) {
      // ignore last odd key with no value to avoid exceptions
    }
    if (dims != null) {
      for (int i = 0; i + 1 < dims.length; i += 2) {
        b.setDimension(dims[i], dims[i + 1]);
      }
    }
    emitter.emit(b.setMetric(metric, millis));
  }

  @SuppressWarnings("unused")
  static void emitGauge(ServiceEmitter emitter, String metric, Number value, String... dims)
  {
    if (emitter == null) {
      return;
    }
    ServiceMetricEvent.Builder b = ServiceMetricEvent.builder();
    if (dims != null && dims.length % 2 == 1) {
      // ignore last odd key with no value to avoid exceptions
    }
    if (dims != null) {
      for (int i = 0; i + 1 < dims.length; i += 2) {
        b.setDimension(dims[i], dims[i + 1]);
      }
    }
    emitter.emit(b.setMetric(metric, value));
  }
}

