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

package org.apache.druid.emitter.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OpenTelemetryEmitter implements Emitter
{
  static final DruidContextTextMapGetter DRUID_CONTEXT_TEXT_MAP_GETTER = new DruidContextTextMapGetter();
  static final HashSet<String> TRACEPARENT_PROPAGATION_FIELDS = new HashSet<>(Arrays.asList(
      "traceparent",
      "tracestate"
  ));
  private static final Logger log = new Logger(OpenTelemetryEmitter.class);
  private final Tracer tracer;
  private final TextMapPropagator propagator;

  OpenTelemetryEmitter(OpenTelemetry openTelemetry)
  {
    tracer = openTelemetry.getTracer("druid-opentelemetry-extension");
    propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  @Override
  public void start()
  {
    log.debug("Starting OpenTelemetryEmitter");
  }

  @Override
  public void emit(Event e)
  {
    if (!(e instanceof ServiceMetricEvent)) {
      return;
    }
    ServiceMetricEvent event = (ServiceMetricEvent) e;

    // We only generate spans for the following types of events:
    // query/time
    if (!event.getMetric().equals("query/time")) {
      return;
    }

    emitQueryTimeEvent(event);
  }

  private void emitQueryTimeEvent(ServiceMetricEvent event)
  {
    Context opentelemetryContext = propagator.extract(Context.current(), event, DRUID_CONTEXT_TEXT_MAP_GETTER);

    try (Scope scope = opentelemetryContext.makeCurrent()) {
      DateTime endTime = event.getCreatedTime();
      DateTime startTime = endTime.minusMillis(event.getValue().intValue());

      Span span = tracer.spanBuilder(event.getService())
                        .setStartTimestamp(startTime.getMillis(), TimeUnit.MILLISECONDS)
                        .startSpan();

      getContext(event).entrySet()
                       .stream()
                       .filter(entry -> entry.getValue() != null)
                       .filter(entry -> !TRACEPARENT_PROPAGATION_FIELDS.contains(entry.getKey()))
                       .forEach(entry -> span.setAttribute(entry.getKey(), entry.getValue().toString()));

      Object status = event.getUserDims().get("success");
      if (status == null) {
        span.setStatus(StatusCode.UNSET);
      } else if (status.toString().equals("true")) {
        span.setStatus(StatusCode.OK);
      } else {
        span.setStatus(StatusCode.ERROR);
      }

      span.end(endTime.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private static Map<String, Object> getContext(ServiceMetricEvent event)
  {
    Object context = event.getUserDims().get("context");
    if (context instanceof Map) {
      return (Map<String, Object>) context;
    }
    return Collections.emptyMap();
  }

  @Override
  public void flush()
  {
  }

  @Override
  public void close()
  {
  }
}
