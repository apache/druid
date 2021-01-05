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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.DistributionValue;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.LabelValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import io.opencensus.proto.metrics.v1.TimeSeries;
import io.opencensus.proto.resource.v1.Resource;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class OpenCensusProtobufInputRowParserTest
{
  private static final Instant INSTANT = Instant.parse("2019-07-12T09:30:01.123Z");
  private static final Timestamp TIMESTAMP = Timestamp.newBuilder()
                                                      .setSeconds(INSTANT.getEpochSecond())
                                                      .setNanos(INSTANT.getNano()).build();

  static final JSONParseSpec PARSE_SPEC = new JSONParseSpec(
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

  static final JSONParseSpec PARSE_SPEC_WITH_DIMENSIONS = new JSONParseSpec(
      new TimestampSpec("timestamp", "millis", null),
      new DimensionsSpec(ImmutableList.of(
          new StringDimensionSchema("foo_key"),
          new StringDimensionSchema("env_key")
      ), null, null),
      new JSONPathSpec(
          true,
          Lists.newArrayList(
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "name", ""),
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "value", ""),
              new JSONPathFieldSpec(JSONPathFieldType.ROOT, "foo_key", "")
          )
      ), null, null
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws Exception
  {
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(
        OpenCensusProtobufInputRowParserTest.PARSE_SPEC,
        "metric.name",
        "descriptor.",
        "custom."
    );

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new OpenCensusProtobufExtensionsModule().getJacksonModules());

    final OpenCensusProtobufInputRowParser actual = (OpenCensusProtobufInputRowParser) jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        InputRowParser.class
    );
    Assert.assertEquals(parser, actual);
    Assert.assertEquals("metric.name", actual.getMetricDimension());
    Assert.assertEquals("descriptor.", actual.getMetricLabelPrefix());
    Assert.assertEquals("custom.", actual.getResourceLabelPrefix());
  }


  @Test
  public void testDefaults()
  {
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(
        OpenCensusProtobufInputRowParserTest.PARSE_SPEC,
        null, null, null
    );

    Assert.assertEquals("name", parser.getMetricDimension());
    Assert.assertEquals("", parser.getMetricLabelPrefix());
    Assert.assertEquals("", parser.getResourceLabelPrefix());
  }

  @Test
  public void testDoubleGaugeParse()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

    Metric metric = doubleGaugeMetric(TIMESTAMP);

    InputRow row = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray())).get(0);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "name", "metric_gauge_double");
    assertDimensionEquals(row, "foo_key", "foo_value");


    Assert.assertEquals(2000, row.getMetric("value").doubleValue(), 0.0);
  }

  @Test
  public void testIntGaugeParse()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

    Metric metric = intGaugeMetric(TIMESTAMP);

    InputRow row = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray())).get(0);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "name", "metric_gauge_int64");
    assertDimensionEquals(row, "foo_key", "foo_value");

    Assert.assertEquals(1000, row.getMetric("value").intValue());
  }

  @Test
  public void testSummaryParse()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

    Metric metric = summaryMetric(TIMESTAMP);

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());
    assertDimensionEquals(row, "name", "metric_summary-count");
    assertDimensionEquals(row, "foo_key", "foo_value");
    Assert.assertEquals(40, row.getMetric("value").doubleValue(), 0.0);

    row = rows.get(1);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());
    assertDimensionEquals(row, "name", "metric_summary-sum");
    assertDimensionEquals(row, "foo_key", "foo_value");
    Assert.assertEquals(10, row.getMetric("value").doubleValue(), 0.0);
  }

  @Test
  public void testDistributionParse()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

    Metric metric = distributionMetric(TIMESTAMP);

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());
    assertDimensionEquals(row, "name", "metric_distribution-count");
    assertDimensionEquals(row, "foo_key", "foo_value");
    Assert.assertEquals(100, row.getMetric("value").intValue());

    row = rows.get(1);
    Assert.assertEquals(INSTANT.toEpochMilli(), row.getTimestampFromEpoch());
    assertDimensionEquals(row, "name", "metric_distribution-sum");
    assertDimensionEquals(row, "foo_key", "foo_value");
    Assert.assertEquals(500, row.getMetric("value").doubleValue(), 0.0);
  }

  @Test
  public void testDimensionsParseWithParseSpecDimensions()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC_WITH_DIMENSIONS, null, null, "");

    Metric metric = summaryMetric(TIMESTAMP);

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, "env_key", "env_val");
    assertDimensionEquals(row, "foo_key", "foo_value");

    row = rows.get(1);
    Assert.assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, "env_key", "env_val");
    assertDimensionEquals(row, "foo_key", "foo_value");

  }

  @Test
  public void testDimensionsParseWithoutPARSE_SPECDimensions()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, "");

    Metric metric = summaryMetric(TIMESTAMP);

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-count");
    assertDimensionEquals(row, "env_key", "env_val");
    assertDimensionEquals(row, "foo_key", "foo_value");

    row = rows.get(1);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-sum");
    assertDimensionEquals(row, "env_key", "env_val");
    assertDimensionEquals(row, "foo_key", "foo_value");

  }

  @Test
  public void testMetricNameOverride()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, "dimension_name", null, "");

    Metric metric = summaryMetric(Timestamp.getDefaultInstance());

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "dimension_name", "metric_summary-count");
    assertDimensionEquals(row, "foo_key", "foo_value");
    assertDimensionEquals(row, "env_key", "env_val");

    row = rows.get(1);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "dimension_name", "metric_summary-sum");
    assertDimensionEquals(row, "foo_key", "foo_value");
    assertDimensionEquals(row, "env_key", "env_val");
  }

  @Test
  public void testDefaultPrefix()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, null, null);

    Metric metric = summaryMetric(Timestamp.getDefaultInstance());

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-count");
    assertDimensionEquals(row, "foo_key", "foo_value");
    assertDimensionEquals(row, "env_key", "env_val");

    row = rows.get(1);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-sum");
    assertDimensionEquals(row, "foo_key", "foo_value");
    assertDimensionEquals(row, "env_key", "env_val");
  }

  @Test
  public void testCustomPrefix()
  {
    //configure parser with desc file
    OpenCensusProtobufInputRowParser parser = new OpenCensusProtobufInputRowParser(PARSE_SPEC, null, "descriptor.", "custom.");

    Metric metric = summaryMetric(Timestamp.getDefaultInstance());

    List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(metric.toByteArray()));

    Assert.assertEquals(2, rows.size());

    InputRow row = rows.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-count");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    assertDimensionEquals(row, "custom.env_key", "env_val");

    row = rows.get(1);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "name", "metric_summary-sum");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    assertDimensionEquals(row, "custom.env_key", "env_val");
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);

    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

  static Metric doubleGaugeMetric(Timestamp timestamp)
  {
    return getMetric(
        "metric_gauge_double",
        "metric_gauge_double_description",
        Type.GAUGE_DOUBLE,
        Point.newBuilder()
            .setTimestamp(timestamp)
            .setDoubleValue(2000)
            .build(),
        timestamp);
  }

  static Metric intGaugeMetric(Timestamp timestamp)
  {
    return getMetric(
        "metric_gauge_int64",
        "metric_gauge_int64_description",
        MetricDescriptor.Type.GAUGE_INT64,
        Point.newBuilder()
            .setTimestamp(timestamp)
            .setInt64Value(1000)
            .build(),
        timestamp);
  }

  static Metric summaryMetric(Timestamp timestamp)
  {

    SummaryValue.Snapshot snapshot = SummaryValue.Snapshot.newBuilder()
        .setSum(DoubleValue.newBuilder().setValue(10).build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(50.0)
            .setValue(10)
            .build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(75.0)
            .setValue(20)
            .build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(95.0)
            .setValue(30)
            .build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(98.0)
            .setValue(40)
            .build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(99.0)
            .setValue(50)
            .build())
        .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
            .setPercentile(99.9)
            .setValue(60)
            .build())
        .build();


    SummaryValue summaryValue = SummaryValue.newBuilder()
        .setCount(Int64Value.newBuilder().setValue(40).build())
        .setSnapshot(snapshot)
        .build();

    return getMetric(
        "metric_summary",
        "metric_summary_description",
        MetricDescriptor.Type.SUMMARY,
        Point.newBuilder()
            .setTimestamp(timestamp)
            .setSummaryValue(summaryValue)
            .build(),
        timestamp);
  }

  static Metric distributionMetric(Timestamp timestamp)
  {
    DistributionValue distributionValue = DistributionValue.newBuilder()
        .setCount(100)
        .setSum(500)
        .build();

    return getMetric(
        "metric_distribution",
        "metric_distribution_description",
        MetricDescriptor.Type.GAUGE_DISTRIBUTION,
        Point.newBuilder()
            .setTimestamp(timestamp)
            .setDistributionValue(distributionValue)
            .build(),
        timestamp);
  }

  static Metric getMetric(String name, String description, MetricDescriptor.Type type, Point point, Timestamp timestamp)
  {
    Metric dist = Metric.newBuilder()
        .setMetricDescriptor(
            MetricDescriptor.newBuilder()
                .setName(name)
                .setDescription(description)
                .setUnit("ms")
                .setType(type)
                .addLabelKeys(
                    LabelKey.newBuilder()
                        .setKey("foo_key")
                        .build())
                .build())
        .setResource(
            Resource.newBuilder()
                .setType("env")
                .putAllLabels(Collections.singletonMap("env_key", "env_val"))
                .build())
        .addTimeseries(
            TimeSeries.newBuilder()
                .setStartTimestamp(timestamp)
                .addLabelValues(
                    LabelValue.newBuilder()
                        .setHasValue(true)
                        .setValue("foo_value")
                        .build())
                .addPoints(point)
                .build())
        .build();

    return dist;
  }

}
