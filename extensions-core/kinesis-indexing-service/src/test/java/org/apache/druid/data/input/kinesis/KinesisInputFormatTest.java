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

package org.apache.druid.data.input.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class KinesisInputFormatTest
{
  static {
    NullHandling.initializeForTests();
  }


  private static final String KINESIS_APPROXIMATE_TIME_DATE = "2024-07-29";
  private static final long KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS = DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE).getMillis();
  private static final String DATA_TIMSTAMP_DATE = "2024-07-30";
  private static final String PARTITION_KEY = "partition_key_1";

  private static final byte[] SIMPLE_JSON_VALUE_BYTES = StringUtils.toUtf8(
      TestUtils.singleQuoteToStandardJson(
          "{"
          + "    'timestamp': '" + DATA_TIMSTAMP_DATE + "',"
          + "    'bar': null,"
          + "    'foo': 'x',"
          + "    'baz': 4,"
          + "    'o': {'mg': 1}"
          + "}"
      )
  );

  private KinesisInputFormat format;

  @Before
  public void setUp()
  {
    format = new KinesisInputFormat(
        // Value Format
        new JsonInputFormat(
            new JSONPathSpec(
                true,
                ImmutableList.of(
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
                )
            ),
            null,
            null,
            false,
            false
        ),
        "kinesis.newts.partitionKey",
        "kinesis.newts.timestamp"
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = new ObjectMapper();
    KinesisInputFormat kif = new KinesisInputFormat(
        // Value Format
        new JsonInputFormat(
            new JSONPathSpec(
                true,
                ImmutableList.of(
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
                )
            ),
            null,
            null,
            false,
            false
        ),
        "kinesis.newts.partitionKey",
        "kinesis.newts.timestamp"
    );
    Assert.assertEquals(format, kif);

    final byte[] formatBytes = mapper.writeValueAsBytes(format);
    final byte[] kifBytes = mapper.writeValueAsBytes(kif);
    Assert.assertArrayEquals(formatBytes, kifBytes);
  }

  @Test
  public void testTimestampFromHeader() throws IOException
  {
    KinesisRecordEntity inputEntity = makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("kinesis.newts.timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal

        Assert.assertEquals(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testRawSample() throws IOException
  {
    KinesisRecordEntity inputEntity = makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec(KinesisInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING, "auto", DateTimes.EPOCH),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRowListPlusRawValues rawValues = iterator.next();
        Assert.assertEquals(1, rawValues.getInputRows().size());
        InputRow row = rawValues.getInputRows().get(0);
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal

        Assert.assertEquals(
            String.valueOf(KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testProcessesSampleTimestampFromHeader() throws IOException
  {
    KinesisRecordEntity inputEntity = makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("kinesis.newts.timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRowListPlusRawValues rawValues = iterator.next();
        Assert.assertEquals(1, rawValues.getInputRows().size());
        InputRow row = rawValues.getInputRows().get(0);
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal

        Assert.assertEquals(DateTimes.of(String.valueOf(KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS)), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithMultipleMixedRecordsTimestampFromHeader() throws IOException
  {
    final byte[][] values = new byte[5][];
    for (int i = 0; i < values.length; i++) {
      values[i] = StringUtils.toUtf8(
          "{\n"
          + "    \"timestamp\": \"2024-07-2" + i + "\",\n"
          + "    \"bar\": null,\n"
          + "    \"foo\": \"x\",\n"
          + "    \"baz\": 4,\n"
          + "    \"index\": " + i + ",\n"
          + "    \"o\": {\n"
          + "        \"mg\": 1\n"
          + "    }\n"
          + "}"
      );
    }

    SettableByteEntity<KinesisRecordEntity> settableByteEntity = new SettableByteEntity<>();

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("kinesis.newts.timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kinesis.newts.timestamp"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        settableByteEntity,
        null
    );

    for (int i = 0; i < values.length; i++) {
      KinesisRecordEntity inputEntity = makeInputEntity(values[i], DateTimes.of("2024-07-1" + i).getMillis());
      settableByteEntity.setEntity(inputEntity);

      final int numExpectedIterations = 1;
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {

          final InputRow row = iterator.next();

          // Payload verification
          // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
          // but test reading them anyway since it isn't technically illegal
          Assert.assertEquals(DateTimes.of("2024-07-1" + i), row.getTimestamp());
          Assert.assertEquals(
              String.valueOf(DateTimes.of("2024-07-1" + i).getMillis()),
              Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
          );
          Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
          Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
          Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
          Assert.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("index")));

          Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
          Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
          Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

          numActualIterations++;
        }

        Assert.assertEquals(numExpectedIterations, numActualIterations);
      }
    }
  }

  @Test
  public void testTimestampFromData() throws IOException
  {
    KinesisRecordEntity inputEntity = makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal

        Assert.assertEquals(DateTimes.of(DATA_TIMSTAMP_DATE), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithMultipleMixedRecordsTimestampFromData() throws IOException
  {
    final byte[][] values = new byte[5][];
    for (int i = 0; i < values.length; i++) {
      values[i] = StringUtils.toUtf8(
          "{\n"
          + "    \"timestamp\": \"2024-07-2" + i + "\",\n"
          + "    \"bar\": null,\n"
          + "    \"foo\": \"x\",\n"
          + "    \"baz\": 4,\n"
          + "    \"index\": " + i + ",\n"
          + "    \"o\": {\n"
          + "        \"mg\": 1\n"
          + "    }\n"
          + "}"
      );
    }

    SettableByteEntity<KinesisRecordEntity> settableByteEntity = new SettableByteEntity<>();

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kinesis.newts.timestamp"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        settableByteEntity,
        null
    );

    for (int i = 0; i < values.length; i++) {
      KinesisRecordEntity inputEntity = makeInputEntity(values[i], KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);
      settableByteEntity.setEntity(inputEntity);

      final int numExpectedIterations = 1;
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {

          final InputRow row = iterator.next();

          // Payload verification
          // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
          // but test reading them anyway since it isn't technically illegal
          Assert.assertEquals(DateTimes.of("2024-07-2" + i), row.getTimestamp());
          Assert.assertEquals(
              String.valueOf(DateTimes.of("2024-07-29").getMillis()),
              Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
          );
          Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
          Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
          Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
          Assert.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("index")));

          Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
          Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
          Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

          numActualIterations++;
        }

        Assert.assertEquals(numExpectedIterations, numActualIterations);
      }
    }
  }

  @Test
  public void testMissingTimestampThrowsException() throws IOException
  {
    KinesisRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("time", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kinesis.newts.timestamp"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        Throwable t = Assert.assertThrows(ParseException.class, iterator::next);
        Assert.assertTrue(
            t.getMessage().startsWith("Timestamp[null] is unparseable! Event: {")
        );
      }
    }
  }

  @Test
  public void testWithSchemaDiscoveryKinesisTimestampExcluded() throws IOException
  {
    KinesisRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            DimensionsSpec.builder()
                .useSchemaDiscovery(true)
                .setDimensionExclusions(ImmutableList.of("kinesis.newts.timestamp"))
                .build(),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();
        List<String> expectedDimensions = Arrays.asList(
            "foo",
            "root_baz",
            "o",
            "bar",
            "path_omg",
            "jq_omg",
            "jq_omg2",
            "baz",
            "root_baz2",
            "path_omg2",
            "kinesis.newts.partitionKey"
        );
        Collections.sort(expectedDimensions);
        Collections.sort(row.getDimensions());
        Assert.assertEquals(
            expectedDimensions,
            row.getDimensions()
        );

        // Payload verifications
        Assert.assertEquals(DateTimes.of(DATA_TIMSTAMP_DATE), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE).getMillis()),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithSchemaDiscoveryTimestampFromHeader() throws IOException
  {
    KinesisRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("kinesis.newts.timestamp", "iso", null),
            DimensionsSpec.builder()
                .useSchemaDiscovery(true)
                .build(),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();
        List<String> expectedDimensions = Arrays.asList(
            "foo",
            "timestamp",
            "root_baz",
            "o",
            "bar",
            "path_omg",
            "jq_omg",
            "jq_omg2",
            "baz",
            "root_baz2",
            "path_omg2",
            "kinesis.newts.partitionKey"
        );
        Collections.sort(expectedDimensions);
        Collections.sort(row.getDimensions());
        Assert.assertEquals(
            expectedDimensions,
            row.getDimensions()
        );

        // Payload verifications
        Assert.assertEquals(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE).getMillis()),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testValueInCsvFormat() throws IOException
  {
    format = new KinesisInputFormat(
        // Value Format
        new CsvInputFormat(
            Arrays.asList("foo", "bar", "timestamp", "baz"),
            null,
            false,
            false,
            0,
            null
        ),
        "kinesis.newts.partitionKey",
        "kinesis.newts.timestamp"
    );

    KinesisRecordEntity inputEntity =
        makeInputEntity(StringUtils.toUtf8("x,,2024-07-30,4"), KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kinesis.newts.timestamp",
                        "kinesis.newts.partitionKey"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();
        Assert.assertEquals(
            Arrays.asList(
                "bar",
                "foo",
                "kinesis.newts.timestamp",
                "kinesis.newts.partitionKey"
            ),
            row.getDimensions()
        );
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal

        Assert.assertEquals(DateTimes.of("2024-07-30"), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE).getMillis()),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertTrue(row.getDimension("bar").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithPartialDeclarationSchemaDiscovery() throws IOException
  {
    KinesisRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_VALUE_BYTES, KINESIS_APPROXOIMATE_TIMESTAMP_MILLIS);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            DimensionsSpec.builder().setDimensions(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar"))
            ).useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        List<String> expectedDimensions = Arrays.asList(
            "bar",
            "foo",
            "kinesis.newts.timestamp",
            "kinesis.newts.partitionKey",
            "root_baz",
            "o",
            "path_omg",
            "jq_omg",
            "jq_omg2",
            "baz",
            "root_baz2",
            "path_omg2"
        );
        Collections.sort(expectedDimensions);
        Collections.sort(row.getDimensions());
        Assert.assertEquals(
            expectedDimensions,
            row.getDimensions()
        );

        // Payload verifications
        Assert.assertEquals(DateTimes.of(DATA_TIMSTAMP_DATE), row.getTimestamp());
        Assert.assertEquals(
            String.valueOf(DateTimes.of(KINESIS_APPROXIMATE_TIME_DATE).getMillis()),
            Iterables.getOnlyElement(row.getDimension("kinesis.newts.timestamp"))
        );
        Assert.assertEquals(PARTITION_KEY, Iterables.getOnlyElement(row.getDimension("kinesis.newts.partitionKey")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void testValidInputFormatConstruction()
  {
    InputFormat valueFormat = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
            )
        ),
        null,
        null,
        false,
        false
    );
    // null partitionKeyColumnName and null timestampColumnName is valid
    new KinesisInputFormat(valueFormat, null, null);

    // non-null partitionKeyColumnName and null timestampColumnName is valid
    new KinesisInputFormat(valueFormat, "kinesis.partitionKey", null);

    // null partitionKeyColumnName and non-null timestampColumnName is valid
    new KinesisInputFormat(valueFormat, null, "kinesis.timestamp");

    // non-null partitionKeyColumnName and non-null timestampColumnName is valid
    new KinesisInputFormat(valueFormat, "kinesis.partitionKey", "kinesis.timestamp");

  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void testInvalidInputFormatConstruction()
  {
    // null value format is invalid
    Assert.assertThrows(
        "valueFormat must not be null",
        NullPointerException.class,
        () -> new KinesisInputFormat(null, null, null)
    );

    InputFormat valueFormat = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
            )
        ),
        null,
        null,
        false,
        false
    );

    // partitionKeyColumnName == timestampColumnName is invalid
    Assert.assertThrows(
        "timestampColumnName and partitionKeyColumnName must be different",
        IllegalStateException.class,
        () -> new KinesisInputFormat(valueFormat, "kinesis.timestamp", "kinesis.timestamp")
    );
  }

  private KinesisRecordEntity makeInputEntity(
      byte[] payload,
      long kinesisTimestampMillis)
  {
    return new KinesisRecordEntity(
        new Record().withData(ByteBuffer.wrap(payload))
            .withApproximateArrivalTimestamp(new Date(kinesisTimestampMillis))
            .withPartitionKey(PARTITION_KEY)
    );
  }

  private SettableByteEntity<KinesisRecordEntity> newSettableByteEntity(KinesisRecordEntity kinesisRecordEntity)
  {
    SettableByteEntity<KinesisRecordEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(kinesisRecordEntity);
    return settableByteEntity;
  }
}
