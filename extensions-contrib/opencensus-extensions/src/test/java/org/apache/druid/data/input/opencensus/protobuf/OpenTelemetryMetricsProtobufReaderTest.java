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

import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

  public static final String INSTRUMENTATION_LIBRARY_NAME = "mock-instr-lib";
  public static final String INSTRUMENTATION_LIBRARY_VERSION = "1.0";

  public static final String METRIC_ATTRIBUTE_COLOR = "color";
  public static final String METRIC_ATTRIBUTE_VALUE_RED = "red";

  public static final String METRIC_ATTRIBUTE_FOO_KEY = "foo_key";
  public static final String METRIC_ATTRIBUTE_FOO_VAL = "foo_value";

  private final MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();

  private final Metric.Builder metricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
      .addInstrumentationLibraryMetricsBuilder()
      .addMetricsBuilder();

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_COLOR),
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_FOO_KEY),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_ENV),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_COUNTRY)
  ), null, null);

  public static final String TOPIC = "telemetry.metrics.otel";
  public static final int PARTITION = 2;
  public static final long OFFSET = 13095752723L;
  public static final long TS = 1643974867555L;
  public static final TimestampType TSTYPE = TimestampType.CREATE_TIME;
  public static final byte[] V0_HEADER_BYTES = ByteBuffer.allocate(Integer.BYTES)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putInt(1)
    .array();
  private static final Header HEADERV1 = new RecordHeader("v", V0_HEADER_BYTES);
  private static final Headers HEADERS = new RecordHeaders(new Header[]{HEADERV1});



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
      .getInstrumentationLibraryMetricsBuilder(0)
      .getInstrumentationLibraryBuilder()
      .setName(INSTRUMENTATION_LIBRARY_NAME)
      .setVersion(INSTRUMENTATION_LIBRARY_VERSION);

  }

  @Test
  public void testSumWithAttributes() throws IOException
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
    ConsumerRecord consumerRecord = new ConsumerRecord(TOPIC, PARTITION, OFFSET, TS, TSTYPE,
         -1L, -1, -1, null, metricsData.toByteArray(), HEADERS);
    KafkaRecordEntity kafkaRecordEntity = new KafkaRecordEntity(consumerRecord);
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");

    CloseableIterator<InputRow> rows = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
      ), kafkaRecordEntity, null).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "value", "6");
  }

  @Test
  public void testGaugeWithAttributes() throws IOException
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
    ConsumerRecord consumerRecord = new ConsumerRecord(TOPIC, PARTITION, OFFSET, TS, TSTYPE,
        -1L, -1, -1, null, metricsData.toByteArray(), HEADERS);
    KafkaRecordEntity kafkaRecordEntity = new KafkaRecordEntity(consumerRecord);
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");
    CloseableIterator<InputRow> rows = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), kafkaRecordEntity, null).read();

    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "value", "6");
  }

  @Test
  public void testBatchedMetricParse() throws IOException
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
        .addInstrumentationLibraryMetricsBuilder()
        .addMetricsBuilder();

    metricsDataBuilder.getResourceMetricsBuilder(1)
        .getResourceBuilder()
        .addAttributes(KeyValue.newBuilder()
          .setKey(RESOURCE_ATTRIBUTE_ENV)
          .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL))
          .build());

    metricsDataBuilder.getResourceMetricsBuilder(1)
      .getInstrumentationLibraryMetricsBuilder(0)
      .getInstrumentationLibraryBuilder()
      .setName(INSTRUMENTATION_LIBRARY_NAME)
      .setVersion(INSTRUMENTATION_LIBRARY_VERSION);

    gaugeMetricBuilder.setName("example_gauge")
      .getGaugeBuilder()
      .addDataPointsBuilder()
      .setAsInt(8)
      .setTimeUnixNano(TIMESTAMP)
      .addAttributesBuilder() // test sum with attributes
      .setKey(METRIC_ATTRIBUTE_FOO_KEY)
      .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build());

    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord consumerRecord = new ConsumerRecord(TOPIC, PARTITION, OFFSET, TS, TSTYPE,
        -1L, -1, -1, null, metricsData.toByteArray(), HEADERS);
    KafkaRecordEntity kafkaRecordEntity = new KafkaRecordEntity(consumerRecord);
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
         "descriptor.",
        "custom.");

    CloseableIterator<InputRow> rows = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), kafkaRecordEntity, null).read();


    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
    assertDimensionEquals(row, "value", "6");

    Assert.assertTrue(rows.hasNext());
    row = rows.next();
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.env", "devel");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    assertDimensionEquals(row, "value", "8");

  }

  @Test
  public void testDimensionSpecExclusions() throws IOException
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

    DimensionsSpec dimensionsSpecWithExclusions = new DimensionsSpec(null,
        ImmutableList.of(
          "descriptor." + METRIC_ATTRIBUTE_COLOR,
          "custom." + RESOURCE_ATTRIBUTE_COUNTRY
        ), null);

    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord consumerRecord = new ConsumerRecord(TOPIC, PARTITION, OFFSET, TS, TSTYPE,
        -1L, -1, -1, null, metricsData.toByteArray(), HEADERS);
    KafkaRecordEntity kafkaRecordEntity = new KafkaRecordEntity(consumerRecord);
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");

    CloseableIterator<InputRow> rows = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpecWithExclusions,
        ColumnsFilter.all()
    ), kafkaRecordEntity, null).read();


    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();

    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "value", "6");
    assertDimensionEquals(row, "custom.env", "devel");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
    Assert.assertFalse(row.getDimensions().contains("custom.country"));
    Assert.assertFalse(row.getDimensions().contains("descriptor.color"));
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

}
