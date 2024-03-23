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

package org.apache.druid.data.input.kafkainput;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class KafkaInputFormatTest
{
  private KafkaRecordEntity inputEntity;
  private final long timestamp = DateTimes.of("2021-06-24").getMillis();
  private static final String TOPIC = "sample";
  private static final Iterable<Header> SAMPLE_HEADERS = ImmutableList.of(
      new Header()
      {
        @Override
        public String key()
        {
          return "encoding";
        }

        @Override
        public byte[] value()
        {
          return "application/json".getBytes(StandardCharsets.UTF_8);
        }
      },
      new Header()
      {
        @Override
        public String key()
        {
          return "kafkapkc";
        }

        @Override
        public byte[] value()
        {
          return "pkc-bar".getBytes(StandardCharsets.UTF_8);
        }
      }
  );
  private KafkaInputFormat format;

  @Before
  public void setUp()
  {
    format = new KafkaInputFormat(
        new KafkaStringHeaderFormat(null),
        // Key Format
        new JsonInputFormat(
            new JSONPathSpec(true, ImmutableList.of()),
            null,
            null,
            false,
            false
        ),
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
        "kafka.newheader.",
        "kafka.newkey.key",
        "kafka.newts.timestamp",
        "kafka.newtopic.topic"
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = new ObjectMapper();
    KafkaInputFormat kif = new KafkaInputFormat(
        new KafkaStringHeaderFormat(null),
        // Key Format
        new JsonInputFormat(
            new JSONPathSpec(true, ImmutableList.of()),
            null,
            null,
            false,
            false
        ),
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
        "kafka.newheader.",
        "kafka.newkey.key",
        "kafka.newts.timestamp",
        "kafka.newtopic.topic"
    );
    Assert.assertEquals(format, kif);

    final byte[] formatBytes = mapper.writeValueAsBytes(format);
    final byte[] kifBytes = mapper.writeValueAsBytes(kif);
    Assert.assertArrayEquals(formatBytes, kifBytes);
  }

  @Test
  public void testWithHeaderKeyAndValue() throws IOException
  {
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}"
    );

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-25\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(key, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newheader.encoding",
                        "kafka.newheader.kafkapkc",
                        "kafka.newts.timestamp",
                        "kafka.newtopic.topic"
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
                "kafka.newheader.encoding",
                "kafka.newheader.kafkapkc",
                "kafka.newts.timestamp",
                "kafka.newtopic.topic"
            ),
            row.getDimensions()
        );
        // Payload verifications
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal
        
        Assert.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        // Header verification
        Assert.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
        Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
        Assert.assertEquals(
            String.valueOf(DateTimes.of("2021-06-24").getMillis()),
            Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
        );
        Assert.assertEquals(
            TOPIC,
            Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
        );
        Assert.assertEquals(
            "2021-06-25",
            Iterables.getOnlyElement(row.getDimension("timestamp"))
        );

        // Key verification
        Assert.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  //Headers cannot be null, so testing only no key use case!
  public void testWithOutKey() throws IOException
  {
    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-24\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(null, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newheader.encoding",
                        "kafka.newheader.kafkapkc",
                        "kafka.newts.timestamp",
                        "kafka.newtopic.topic"
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

        // Key verification
        Assert.assertTrue(row.getDimension("kafka.newkey.key").isEmpty());
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }

  }

  @Test
  public void testTimestampFromHeader() throws IOException
  {
    Iterable<Header> sample_header_with_ts = Iterables.unmodifiableIterable(
        Iterables.concat(
            SAMPLE_HEADERS,
            ImmutableList.of(
                new Header()
                {
                  @Override
                  public String key()
                  {
                    return "headerTs";
                  }

                  @Override
                  public byte[] value()
                  {
                    return "2021-06-24".getBytes(StandardCharsets.UTF_8);
                  }
                }
            )
        )
    );
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}"
    );

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-24\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(sample_header_with_ts);
    inputEntity = makeInputEntity(key, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("kafka.newheader.headerTs", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newheader.encoding",
                        "kafka.newheader.kafkapkc"
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

        Assert.assertEquals(DateTimes.of("2021-06-24"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        // Header verification
        Assert.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
        Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
        Assert.assertEquals(
            String.valueOf(DateTimes.of("2021-06-24").getMillis()),
            Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
        );
        Assert.assertEquals(
            "2021-06-24",
            Iterables.getOnlyElement(row.getDimension("kafka.newheader.headerTs"))
        );
        Assert.assertEquals(
            "2021-06-24",
            Iterables.getOnlyElement(row.getDimension("timestamp"))
        );

        // Key verification
        Assert.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

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
  public void testWithOutKeyAndHeaderSpecs() throws IOException
  {
    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-24\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(null, payload, headers);

    KafkaInputFormat localFormat = new KafkaInputFormat(
        null,
        null,
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
        "kafka.newheader.", "kafka.newkey.", "kafka.newts.", "kafka.newtopic."
    );

    final InputEntityReader reader = localFormat.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newts.timestamp",
                        "kafka.newtopic.topic"
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

        // Key verification
        // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
        // but test reading them anyway since it isn't technically illegal
        Assert.assertTrue(row.getDimension("kafka.newkey.key").isEmpty());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }

  }

  @Test
  public void testWithMultipleMixedRecords() throws IOException
  {
    final byte[][] keys = new byte[5][];
    final byte[][] values = new byte[5][];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = StringUtils.toUtf8(
          "{\n"
          + "    \"key\": \"sampleKey-" + i + "\"\n"
          + "}"
      );
    }
    keys[2] = null;

    for (int i = 0; i < values.length; i++) {
      values[i] = StringUtils.toUtf8(
          "{\n"
          + "    \"timestamp\": \"2021-06-2" + i + "\",\n"
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

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    SettableByteEntity<KafkaRecordEntity> settableByteEntity = new SettableByteEntity<>();

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newheader.encoding",
                        "kafka.newheader.kafkapkc",
                        "kafka.newts.timestamp",
                        "kafka.newtopic.topic"
                    )
                )
            ),
            ColumnsFilter.all()
        ),
        settableByteEntity,
        null
    );

    for (int i = 0; i < keys.length; i++) {
      headers = headers.add(new RecordHeader("indexH", String.valueOf(i).getBytes(StandardCharsets.UTF_8)));

      inputEntity = makeInputEntity(keys[i], values[i], headers);
      settableByteEntity.setEntity(inputEntity);

      final int numExpectedIterations = 1;
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {

          final InputRow row = iterator.next();

          // Payload verification
          // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
          // but test reading them anyway since it isn't technically illegal
          Assert.assertEquals(DateTimes.of("2021-06-2" + i), row.getTimestamp());
          Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
          Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
          Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
          Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
          Assert.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("index")));


          // Header verification
          Assert.assertEquals(
              "application/json",
              Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding"))
          );
          Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
          Assert.assertEquals(
              String.valueOf(DateTimes.of("2021-06-24").getMillis()),
              Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
          );
          Assert.assertEquals(
              TOPIC,
              Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
          );
          Assert.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("kafka.newheader.indexH")));


          // Key verification
          if (i == 2) {
            Assert.assertEquals(Collections.emptyList(), row.getDimension("kafka.newkey.key"));
          } else {
            Assert.assertEquals("sampleKey-" + i, Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));
          }

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
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}"
    );

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-25\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(key, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("time", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newheader.encoding",
                        "kafka.newheader.kafkapkc",
                        "kafka.newts.timestamp",
                        "kafka.newtopic.topic"
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
        Throwable t = Assert.assertThrows(ParseException.class, () -> iterator.next());
        Assert.assertEquals(
            "Timestamp[null] is unparseable! Event: {kafka.newtopic.topic=sample, foo=x, kafka.newts"
            + ".timestamp=1624492800000, kafka.newkey.key=sampleKey...",
            t.getMessage()
        );
      }
    }
  }

  @Test
  public void testWithSchemaDiscovery() throws IOException
  {
    // testWithHeaderKeyAndValue + schemaless
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}"
    );

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-25\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(key, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
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
                "kafka.newtopic.topic",
                "foo",
                "kafka.newts.timestamp",
                "kafka.newkey.key",
                "root_baz",
                "o",
                "bar",
                "kafka.newheader.kafkapkc",
                "path_omg",
                "jq_omg",
                "jq_omg2",
                "baz",
                "root_baz2",
                "kafka.newheader.encoding",
                "path_omg2"
            ),
            row.getDimensions()
        );

        // Payload verifications
        Assert.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        // Header verification
        Assert.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
        Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
        Assert.assertEquals(
            String.valueOf(DateTimes.of("2021-06-24").getMillis()),
            Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
        );
        Assert.assertEquals(
            TOPIC,
            Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
        );
        Assert.assertEquals(
            "2021-06-25",
            Iterables.getOnlyElement(row.getDimension("timestamp"))
        );

        // Key verification
        Assert.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithPartialDeclarationSchemaDiscovery() throws IOException
  {
    // testWithHeaderKeyAndValue + partial-schema + schema discovery
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}"
    );

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-25\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}"
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = makeInputEntity(key, payload, headers);

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            DimensionsSpec.builder().setDimensions(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "kafka.newheader.kafkapkc"))
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

        Assert.assertEquals(
            Arrays.asList(
                "bar",
                "kafka.newheader.kafkapkc",
                "kafka.newtopic.topic",
                "foo",
                "kafka.newts.timestamp",
                "kafka.newkey.key",
                "root_baz",
                "o",
                "path_omg",
                "jq_omg",
                "jq_omg2",
                "baz",
                "root_baz2",
                "kafka.newheader.encoding",
                "path_omg2"
            ),
            row.getDimensions()
        );

        // Payload verifications
        Assert.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assert.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        // Header verification
        Assert.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
        Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
        Assert.assertEquals(
            String.valueOf(DateTimes.of("2021-06-24").getMillis()),
            Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
        );
        Assert.assertEquals(
            TOPIC,
            Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
        );
        Assert.assertEquals(
            "2021-06-25",
            Iterables.getOnlyElement(row.getDimension("timestamp"))
        );

        // Key verification
        Assert.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  private KafkaRecordEntity makeInputEntity(byte[] key, byte[] payload, Headers headers)
  {
    return new KafkaRecordEntity(
        new ConsumerRecord<>(
            TOPIC,
            0,
            0,
            timestamp,
            null,
            0,
            0,
            key,
            payload,
            headers,
            Optional.empty()
        )
    );
  }


  private SettableByteEntity<KafkaRecordEntity> newSettableByteEntity(KafkaRecordEntity kafkaRecordEntity)
  {
    SettableByteEntity<KafkaRecordEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(kafkaRecordEntity);
    return settableByteEntity;
  }
}
