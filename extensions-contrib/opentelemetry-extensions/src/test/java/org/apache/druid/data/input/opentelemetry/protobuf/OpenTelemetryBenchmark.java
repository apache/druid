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

package org.apache.druid.data.input.opentelemetry.protobuf;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Fork(1)
@State(Scope.Benchmark)
public class OpenTelemetryBenchmark
{

  private static ByteBuffer BUFFER;

  @Param(value = {"1", "2", "4", "8" })
  private int resourceMetricCount = 1;

  @Param(value = {"1"})
  private int instrumentationLibraryCount = 1;

  @Param(value = {"1", "2", "4", "8" })
  private int metricsCount = 1;

  @Param(value = {"1", "2", "4", "8" })
  private int dataPointCount;

  private static final long TIMESTAMP = TimeUnit.MILLISECONDS.toNanos(Instant.parse("2019-07-12T09:30:01.123Z").toEpochMilli());

  private static final InputRowSchema ROW_SCHEMA = new InputRowSchema(null,
      new DimensionsSpec(ImmutableList.of(
          new StringDimensionSchema("name"),
          new StringDimensionSchema("value"),
          new StringDimensionSchema("foo_key")),
          null, null),
      null);

  private static final OpenTelemetryMetricsProtobufInputFormat INPUT_FORMAT =
      new OpenTelemetryMetricsProtobufInputFormat("name",
          "value",
          "",
          "resource.");

  private ByteBuffer createMetricBuffer()
  {
    MetricsData.Builder metricsData = MetricsData.newBuilder();
    for (int i = 0; i < resourceMetricCount; i++) {
      ResourceMetrics.Builder resourceMetricsBuilder = metricsData.addResourceMetricsBuilder();
      Resource.Builder resourceBuilder = resourceMetricsBuilder.getResourceBuilder();

      for (int resourceAttributeI = 0; resourceAttributeI < 5; resourceAttributeI++) {
        KeyValue.Builder resourceAttributeBuilder = resourceBuilder.addAttributesBuilder();
        resourceAttributeBuilder.setKey("resource.label_key_" + resourceAttributeI);
        resourceAttributeBuilder.setValue(AnyValue.newBuilder().setStringValue("resource.label_value"));
      }

      for (int j = 0; j < instrumentationLibraryCount; j++) {
        InstrumentationLibraryMetrics.Builder instrumentationLibraryMetricsBuilder =
            resourceMetricsBuilder.addInstrumentationLibraryMetricsBuilder();

        for (int k = 0; k < metricsCount; k++) {
          Metric.Builder metricBuilder = instrumentationLibraryMetricsBuilder.addMetricsBuilder();
          metricBuilder.setName("io.confluent.domain/such/good/metric/wow");

          for (int l = 0; l < dataPointCount; l++) {
            NumberDataPoint.Builder dataPointBuilder = metricBuilder.getSumBuilder().addDataPointsBuilder();
            dataPointBuilder.setAsDouble(42.0).setTimeUnixNano(TIMESTAMP);

            for (int metricAttributeI = 0; metricAttributeI < 10; metricAttributeI++) {
              KeyValue.Builder attributeBuilder = dataPointBuilder.addAttributesBuilder();
              attributeBuilder.setKey("foo_key_" + metricAttributeI);
              attributeBuilder.setValue(AnyValue.newBuilder().setStringValue("foo-value"));
            }
          }
        }
      }
    }
    return ByteBuffer.wrap(metricsData.build().toByteArray());
  }

  @Setup
  public void init()
  {
    BUFFER = createMetricBuffer();
  }

  @Benchmark()
  public void measureSerde(Blackhole blackhole) throws IOException
  {
    for (CloseableIterator<InputRow> it = INPUT_FORMAT.createReader(ROW_SCHEMA, new ByteEntity(BUFFER), null).read(); it.hasNext(); ) {
      InputRow row = it.next();
      blackhole.consume(row);
    }
  }
}
