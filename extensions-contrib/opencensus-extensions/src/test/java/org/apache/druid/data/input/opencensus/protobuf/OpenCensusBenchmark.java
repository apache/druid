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

package org.apache.druid.data.input.opencensus.protobuf;

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.LabelValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import io.opencensus.proto.resource.v1.Resource;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Fork(1)
public class OpenCensusBenchmark
{
  private static final Instant INSTANT = Instant.parse("2019-07-12T09:30:01.123Z");
  private static final Timestamp TIMESTAMP = Timestamp.newBuilder()
                                                      .setSeconds(INSTANT.getEpochSecond())
                                                      .setNanos(INSTANT.getNano()).build();

  private static final JSONParseSpec PARSE_SPEC = new JSONParseSpec(
      new TimestampSpec("timestamp", "millis", null),
      new DimensionsSpec(null, null, null),
      new JSONPathSpec(
          true,
          Lists.newArrayList(
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "name", ""),
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "value", ""),
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "foo_key", "")
          )
      ), null, null
  );

  private static final OpenCensusProtobufInputRowParser PARSER = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

  private static final ByteBuffer BUFFER = ByteBuffer.wrap(createMetric().toByteArray());

  static Metric createMetric()
  {
    final MetricDescriptor.Builder descriptorBuilder = MetricDescriptor.newBuilder()
                                                        .setName("io.confluent.domain/such/good/metric/wow")
                                                        .setUnit("ms")
                                                        .setType(MetricDescriptor.Type.CUMULATIVE_DOUBLE);


    final TimeSeries.Builder tsBuilder = TimeSeries.newBuilder()
                                                   .setStartTimestamp(TIMESTAMP)
                                                   .addPoints(Point.newBuilder().setDoubleValue(42.0).build());
    for (int i = 0; i < 10; i++) {
      descriptorBuilder.addLabelKeys(LabelKey.newBuilder()
                                             .setKey("foo_key_" + i)
                                             .build());
      tsBuilder.addLabelValues(LabelValue.newBuilder()
                                         .setHasValue(true)
                                         .setValue("foo_value")
                                         .build());
    }

    final Map<String, String> resourceLabels = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      resourceLabels.put("resoure.label_" + i, "val_" + i);
    }

    return Metric.newBuilder()
                 .setMetricDescriptor(descriptorBuilder.build())
                 .setResource(
                     Resource.newBuilder()
                             .setType("env")
                             .putAllLabels(resourceLabels)
                             .build())
                 .addTimeseries(tsBuilder.build())
                 .build();
  }

  @Benchmark()
  public void measureSerde(Blackhole blackhole)
  {
    // buffer must be reset / duplicated each time to ensure each iteration reads the entire buffer from the beginning
    for (InputRow row : PARSER.parseBatch(BUFFER.duplicate())) {
      blackhole.consume(row);
    }
  }
}
