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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class OpenTelemetryEmitterTest
{
  private static class NoopExporter implements SpanExporter
  {
    public Collection<SpanData> spanDataCollection;

    @Override
    public CompletableResultCode export(Collection<SpanData> collection)
    {
      this.spanDataCollection = collection;
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush()
    {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown()
    {
      return CompletableResultCode.ofSuccess();
    }
  }

  private static final DateTime TIMESTAMP = DateTimes.of(2021, 11, 5, 1, 1);

  private OpenTelemetry openTelemetry;
  private NoopExporter noopExporter;
  private OpenTelemetryEmitter emitter;

  @Before
  public void setup()
  {
    noopExporter = new NoopExporter();
    openTelemetry = OpenTelemetrySdk.builder()
                                    .setTracerProvider(SdkTracerProvider.builder()
                                                                        .addSpanProcessor(SimpleSpanProcessor.create(
                                                                            noopExporter))
                                                                        .build())
                                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                                    .build();
    emitter = new OpenTelemetryEmitter(openTelemetry);
  }

  // Check that we don't call "emitQueryTimeEvent" method for event that is not instance of ServiceMetricEvent
  @Test
  public void testNoEmitNotServiceMetric()
  {
    final Event notServiceMetricEvent =
        new Event()
        {
          @Override
          public EventMap toMap()
          {
            return new EventMap();
          }

          @Override
          public String getFeed()
          {
            return null;
          }
        };

    emitter.emit(notServiceMetricEvent);
    Assert.assertNull(noopExporter.spanDataCollection);
  }

  // Check that we don't call "emitQueryTimeEvent" method for ServiceMetricEvent that is not "query/time" type
  @Test
  public void testNoEmitNotQueryTimeMetric()
  {
    final ServiceMetricEvent notQueryTimeMetric =
        new ServiceMetricEvent.Builder().build(
                                            TIMESTAMP,
                                            "query/cache/total/hitRate",
                                            0.54
                                        )
                                        .build(
                                            "broker",
                                            "brokerHost1"
                                        );

    emitter.emit(notQueryTimeMetric);
    Assert.assertNull(noopExporter.spanDataCollection);
  }

  @Test
  public void testTraceparentId()
  {
    final String traceId = "00-54ef39243e3feb12072e0f8a74c1d55a-ad6d5b581d7c29c1-01";
    final String expectedParentTraceId = "54ef39243e3feb12072e0f8a74c1d55a";
    final String expectedParentSpanId = "ad6d5b581d7c29c1";
    final Map<String, String> context = new HashMap<>();
    context.put("traceparent", traceId);

    final String serviceName = "druid/broker";
    final DateTime createdTime = TIMESTAMP;
    final long metricValue = 100;

    final ServiceMetricEvent queryTimeMetric =
        new ServiceMetricEvent.Builder().setDimension("context", context)
                                        .build(
                                            createdTime,
                                            "query/time",
                                            metricValue
                                        )
                                        .build(
                                            serviceName,
                                            "host"
                                        );

    emitter.emit(queryTimeMetric);

    Assert.assertEquals(1, noopExporter.spanDataCollection.size());

    SpanData actualSpanData = noopExporter.spanDataCollection.iterator().next();
    Assert.assertEquals(serviceName, actualSpanData.getName());
    Assert.assertEquals((createdTime.getMillis() - metricValue) * 1_000_000, actualSpanData.getStartEpochNanos());
    Assert.assertEquals(expectedParentTraceId, actualSpanData.getParentSpanContext().getTraceId());
    Assert.assertEquals(expectedParentSpanId, actualSpanData.getParentSpanContext().getSpanId());
  }

  @Test
  public void testAttributes()
  {
    final Map<String, String> context = new HashMap<>();
    final String expectedAttributeKey = "attribute";
    final String expectedAttributeValue = "value";
    context.put(expectedAttributeKey, expectedAttributeValue);

    final ServiceMetricEvent queryTimeMetricWithAttributes =
        new ServiceMetricEvent.Builder().setDimension("context", context)
                                        .build(
                                            TIMESTAMP,
                                            "query/time",
                                            100
                                        )
                                        .build(
                                            "druid/broker",
                                            "host"
                                        );

    emitter.emit(queryTimeMetricWithAttributes);

    SpanData actualSpanData = noopExporter.spanDataCollection.iterator().next();
    Assert.assertEquals(1, actualSpanData.getAttributes().size());
    Assert.assertEquals(
        expectedAttributeValue,
        actualSpanData.getAttributes().get(AttributeKey.stringKey(expectedAttributeKey))
    );
  }

  @Test
  public void testFilterNullValue()
  {
    final Map<String, String> context = new HashMap<>();
    context.put("attributeKey", null);

    final ServiceMetricEvent queryTimeMetric =
        new ServiceMetricEvent.Builder().setDimension("context", context)
                                        .build(
                                            TIMESTAMP,
                                            "query/time",
                                            100
                                        )
                                        .build(
                                            "druid/broker",
                                            "host"
                                        );

    emitter.emit(queryTimeMetric);

    SpanData actualSpanData = noopExporter.spanDataCollection.iterator().next();
    Assert.assertEquals(0, actualSpanData.getAttributes().size());
  }

  @Test
  public void testOkStatus()
  {
    final ServiceMetricEvent queryTimeMetric =
        new ServiceMetricEvent.Builder().setDimension("success", "true")
                                        .build(
                                            TIMESTAMP,
                                            "query/time",
                                            100
                                        )
                                        .build(
                                            "druid/broker",
                                            "host"
                                        );

    emitter.emit(queryTimeMetric);

    SpanData actualSpanData = noopExporter.spanDataCollection.iterator().next();
    Assert.assertEquals(StatusCode.OK, actualSpanData.getStatus().getStatusCode());
  }

  @Test
  public void testErrorStatus()
  {
    final ServiceMetricEvent queryTimeMetric =
        new ServiceMetricEvent.Builder().setDimension("success", "false")
                                        .build(
                                            TIMESTAMP,
                                            "query/time",
                                            100
                                        )
                                        .build(
                                            "druid/broker",
                                            "host"
                                        );

    emitter.emit(queryTimeMetric);

    SpanData actualSpanData = noopExporter.spanDataCollection.iterator().next();
    Assert.assertEquals(StatusCode.ERROR, actualSpanData.getStatus().getStatusCode());
  }
}
