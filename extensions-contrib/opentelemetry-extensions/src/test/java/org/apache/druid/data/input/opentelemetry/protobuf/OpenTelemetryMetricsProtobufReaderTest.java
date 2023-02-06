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
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OpenTelemetryMetricsProtobufReaderTest
{
  private static final long TIMESTAMP = TimeUnit.MILLISECONDS.toNanos(Instant.parse("2019-07-12T09:30:01.123Z").toEpochMilli());
  public static final String RESOURCE_ATTRIBUTE_COUNTRY = "country";
  public static final String RESOURCE_ATTRIBUTE_VALUE_USA = "usa";

  public static final String RESOURCE_ATTRIBUTE_ENV = "env";
  public static final String RESOURCE_ATTRIBUTE_VALUE_DEVEL = "devel";

  public static final String INSTRUMENTATION_SCOPE_NAME = "mock-instr-lib";
  public static final String INSTRUMENTATION_SCOPE_VERSION = "1.0";

  public static final String METRIC_ATTRIBUTE_COLOR = "color";
  public static final String METRIC_ATTRIBUTE_VALUE_RED = "red";

  public static final String METRIC_ATTRIBUTE_FOO_KEY = "foo_key";
  public static final String METRIC_ATTRIBUTE_FOO_VAL = "foo_value";

  private final MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();

  private final Metric.Builder metricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
      .addScopeMetricsBuilder()
      .addMetricsBuilder();

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_COLOR),
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_FOO_KEY),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_ENV),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_COUNTRY)
  ));

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    metricsDataBuilder
        .getResourceMetricsBuilder(0)
        .getResourceBuilder()
        .addAttributes(KeyValue.newBuilder()
            .setKey(RESOURCE_ATTRIBUTE_COUNTRY)
            .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_USA)));

    metricsDataBuilder
        .getResourceMetricsBuilder(0)
        .getScopeMetricsBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

  }

  @Test
  public void testSumWithAttributes()
  {
    metricBuilder
        .setName("example_sum")
        .getSumBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAttributesBuilder() // test sum with attributes
        .setKey(METRIC_ATTRIBUTE_COLOR)
        .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "raw.value", "6");
  }

  @Test
  public void testGaugeWithAttributes()
  {
    metricBuilder.setName("example_gauge")
        .getGaugeBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAttributesBuilder() // test sum with attributes
        .setKey(METRIC_ATTRIBUTE_COLOR)
        .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "raw.value", "6");
  }

  @Test
  public void testBatchedMetricParse()
  {
    metricBuilder.setName("example_sum")
        .getSumBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAttributesBuilder() // test sum with attributes
        .setKey(METRIC_ATTRIBUTE_COLOR)
        .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    // Create Second Metric
    Metric.Builder gaugeMetricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
        .addScopeMetricsBuilder()
        .addMetricsBuilder();

    metricsDataBuilder.getResourceMetricsBuilder(1)
        .getResourceBuilder()
        .addAttributes(KeyValue.newBuilder()
            .setKey(RESOURCE_ATTRIBUTE_ENV)
            .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL))
            .build());

    metricsDataBuilder.getResourceMetricsBuilder(1)
        .getScopeMetricsBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    gaugeMetricBuilder.setName("example_gauge")
        .getGaugeBuilder()
        .addDataPointsBuilder()
        .setAsInt(8)
        .setTimeUnixNano(TIMESTAMP)
        .addAttributesBuilder() // test sum with attributes
        .setKey(METRIC_ATTRIBUTE_FOO_KEY)
        .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build());

    MetricsData metricsData = metricsDataBuilder.build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "raw.value", "6");

    Assert.assertTrue(rows.hasNext());
    row = rows.next();
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.env", "devel");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    assertDimensionEquals(row, "raw.value", "8");

  }

  @Test
  public void testDimensionSpecExclusions()
  {
    metricsDataBuilder.getResourceMetricsBuilder(0)
        .getResourceBuilder()
        .addAttributesBuilder()
        .setKey(RESOURCE_ATTRIBUTE_ENV)
        .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL).build());

    metricBuilder.setName("example_gauge")
        .getGaugeBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAllAttributes(ImmutableList.of(
            KeyValue.newBuilder()
                .setKey(METRIC_ATTRIBUTE_COLOR)
                .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build()).build(),
            KeyValue.newBuilder()
                .setKey(METRIC_ATTRIBUTE_FOO_KEY)
                .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build()).build()));

    MetricsData metricsData = metricsDataBuilder.build();

    DimensionsSpec dimensionsSpecWithExclusions = DimensionsSpec.builder().setDimensionExclusions(ImmutableList.of(
            "descriptor." + METRIC_ATTRIBUTE_COLOR,
            "custom." + RESOURCE_ATTRIBUTE_COUNTRY
    )).build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpecWithExclusions,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "raw.value", "6");
    assertDimensionEquals(row, "custom.env", "devel");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    Assert.assertFalse(row.getDimensions().contains("custom.country"));
    Assert.assertFalse(row.getDimensions().contains("descriptor.color"));
  }

  @Test
  public void testUnsupportedValueTypes()
  {
    KeyValueList kvList = KeyValueList.newBuilder()
        .addValues(
            KeyValue.newBuilder()
                .setKey("foo")
                .setValue(AnyValue.newBuilder().setStringValue("bar").build()))
        .build();

    metricsDataBuilder.getResourceMetricsBuilder(0)
        .getResourceBuilder()
        .addAttributesBuilder()
        .setKey(RESOURCE_ATTRIBUTE_ENV)
        .setValue(AnyValue.newBuilder().setKvlistValue(kvList).build());

    metricBuilder
        .setName("example_sum")
        .getSumBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAllAttributes(ImmutableList.of(
            KeyValue.newBuilder()
                .setKey(METRIC_ATTRIBUTE_COLOR)
                .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build()).build(),
            KeyValue.newBuilder()
                .setKey(METRIC_ATTRIBUTE_FOO_KEY)
                .setValue(AnyValue.newBuilder().setKvlistValue(kvList).build()).build()));

    MetricsData metricsData = metricsDataBuilder.build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");

    // Unsupported resource attribute type is omitted
    Assert.assertEquals(0, row.getDimension("custom.env").size());

    // Unsupported metric attribute type is omitted
    Assert.assertEquals(0, row.getDimension("descriptor.foo_key").size());

    assertDimensionEquals(row, "raw.value", "6");
  }

  @Test
  public void testInvalidProtobuf()
  {
    byte[] invalidProtobuf = new byte[] {0x00, 0x01};
    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(invalidProtobuf));
    try (CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read()) {
      Assert.assertThrows(ParseException.class, () -> rows.hasNext());
      Assert.assertThrows(ParseException.class, () -> rows.next());
    }
    catch (IOException e) {
      // Comes from the implicit call to close. Ignore
    }
  }

  @Test
  public void testInvalidMetricType()
  {
    metricBuilder
        .setName("unsupported_histogram_metric")
        .getExponentialHistogramBuilder()
        .addDataPointsBuilder()
        .setTimeUnixNano(TIMESTAMP);

    MetricsData metricsData = metricsDataBuilder.build();

    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(metricsData.toByteArray()));
    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        "metric.name",
        "raw.value",
        "descriptor.",
        "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(0, rowList.size());
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

}
