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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.common.TestUtils;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KafkaInputFormatTest
{
  private static final long TIMESTAMP_MILLIS = DateTimes.of("2021-06-24").getMillis();
  private static final String TOPIC = "sample";
  private static final byte[] SIMPLE_JSON_KEY_BYTES = StringUtils.toUtf8(
      TestUtils.singleQuoteToStandardJson(
          "{'key': 'sampleKey'}"
      )
  );
  private static final byte[] SIMPLE_JSON_VALUE_BYTES = StringUtils.toUtf8(
      TestUtils.singleQuoteToStandardJson(
          "{"
          + "    'timestamp': '2021-06-25',"
          + "    'bar': null,"
          + "    'foo': 'x',"
          + "    'baz': 4,"
          + "    'o': {'mg': 1}"
          + "}"
      )
  );

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

  @BeforeEach
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
        "kafka.newtopic.topic",
        "kafka.new.partition",
        "kafka.new.offset",
        null
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
        "kafka.newtopic.topic",
        "kafka.new.partition",
        "kafka.new.offset",
        null
    );
    Assertions.assertEquals(format, kif);

    final byte[] formatBytes = mapper.writeValueAsBytes(format);
    final byte[] kifBytes = mapper.writeValueAsBytes(kif);
    Assertions.assertArrayEquals(formatBytes, kifBytes);
  }

  @Test
  public void testWithHeaderKeyAndValue() throws IOException
  {
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, SIMPLE_JSON_VALUE_BYTES, headers);

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
        Assertions.assertEquals(
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
        
        Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  //Headers cannot be null, so testing only no key use case!
  public void testWithOutKey() throws IOException
  {
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity = makeInputEntity(null, SIMPLE_JSON_VALUE_BYTES, headers);

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
        Assertions.assertTrue(row.getDimension("kafka.newkey.key").isEmpty());
        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }

  }

  @Test
  public void testTimestampFromHeader() throws IOException
  {
    final Iterable<Header> sampleHeaderWithTs = Iterables.unmodifiableIterable(
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
    Headers headers = new RecordHeaders(sampleHeaderWithTs);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, SIMPLE_JSON_VALUE_BYTES, headers);

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

        Assertions.assertEquals(DateTimes.of("2021-06-24"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());
        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithOutKeyAndHeaderSpecs() throws IOException
  {
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(null, SIMPLE_JSON_VALUE_BYTES, headers);

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
        "kafka.newheader.", "kafka.newkey.", "kafka.newts.", "kafka.newtopic.",
        "kafka.new.partition", "kafka.new.offset",
        null
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
        Assertions.assertTrue(row.getDimension("kafka.newkey.key").isEmpty());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
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

      KafkaRecordEntity inputEntity = makeInputEntity(keys[i], values[i], headers);
      settableByteEntity.setEntity(inputEntity);

      final int numExpectedIterations = 1;
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {

          final InputRow row = iterator.next();

          // Payload verification
          // this isn't super realistic, since most of these columns are not actually defined in the dimensionSpec
          // but test reading them anyway since it isn't technically illegal
          Assertions.assertEquals(DateTimes.of("2021-06-2" + i), row.getTimestamp());
          Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
          Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
          Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
          Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
          Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
          Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));
          Assertions.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("index")));

          // Header verification
          Assertions.assertEquals(
              "application/json",
              Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding"))
          );
          Assertions.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
          Assertions.assertEquals(
              String.valueOf(DateTimes.of("2021-06-24").getMillis()),
              Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
          );
          Assertions.assertEquals(
              TOPIC,
              Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
          );
          Assertions.assertEquals(String.valueOf(i), Iterables.getOnlyElement(row.getDimension("kafka.newheader.indexH")));


          // Key verification
          if (i == 2) {
            Assertions.assertEquals(Collections.emptyList(), row.getDimension("kafka.newkey.key"));
          } else {
            Assertions.assertEquals("sampleKey-" + i, Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));
          }

          Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
          Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
          Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());

          numActualIterations++;
        }

        Assertions.assertEquals(numExpectedIterations, numActualIterations);
      }
    }
  }

  @Test
  public void testMissingTimestampThrowsException() throws IOException
  {
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, SIMPLE_JSON_VALUE_BYTES, headers);

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
        Throwable t = Assertions.assertThrows(ParseException.class, iterator::next);
        Assertions.assertTrue(
            t.getMessage().startsWith("Timestamp[null] is unparseable! Event: {")
        );
      }
    }
  }

  @Test
  public void testWithSchemaDiscovery() throws IOException
  {
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, SIMPLE_JSON_VALUE_BYTES, headers);

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
        Assertions.assertEquals(
            Arrays.asList(
                "kafka.newtopic.topic",
                "foo",
                "kafka.newts.timestamp",
                "kafka.newkey.key",
                "kafka.new.partition",
                "root_baz",
                "o",
                "bar",
                "kafka.newheader.kafkapkc",
                "path_omg",
                "jq_omg",
                "jq_omg2",
                "baz",
                "kafka.new.offset",
                "root_baz2",
                "kafka.newheader.encoding",
                "path_omg2"
            ),
            row.getDimensions()
        );

        // Payload verifications
        Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testKeyInCsvFormat() throws IOException
  {
    format = new KafkaInputFormat(
        new KafkaStringHeaderFormat(null),
        // Key Format
        new CsvInputFormat(
            // name of the field doesn't matter, it just has to be something
            Collections.singletonList("foo"),
            null,
            false,
            false,
            0,
            null
        ),
        // Value Format
        new JsonInputFormat(
            new JSONPathSpec(true, ImmutableList.of()),
            null,
            null,
            false,
            false
        ),
        "kafka.newheader.",
        "kafka.newkey.key",
        "kafka.newts.timestamp",
        "kafka.newtopic.topic",
        "kafka.new.partition",
        "kafka.new.offset",
        null
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(
            // x,y,z are ignored; key will be "sampleKey"
            StringUtils.toUtf8("sampleKey,x,y,z"),
            SIMPLE_JSON_VALUE_BYTES,
            headers
        );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "bar",
                        "foo",
                        "kafka.newkey.key",
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
        Assertions.assertEquals(
            Arrays.asList(
                "bar",
                "foo",
                "kafka.newkey.key",
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

        Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertTrue(row.getDimension("bar").isEmpty());

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testValueInCsvFormat() throws IOException
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
        new CsvInputFormat(
            Arrays.asList("foo", "bar", "timestamp", "baz"),
            null,
            false,
            false,
            0,
            null
        ),
        "kafka.newheader.",
        "kafka.newkey.key",
        "kafka.newts.timestamp",
        "kafka.newtopic.topic",
        "kafka.new.partition",
        "kafka.new.offset",
        null
    );

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, StringUtils.toUtf8("x,,2021-06-25,4"), headers);

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
        Assertions.assertEquals(
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

        Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertTrue(row.getDimension("bar").isEmpty());

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithPartialDeclarationSchemaDiscovery() throws IOException
  {
    // testWithHeaderKeyAndValue + partial-schema + schema discovery
    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    KafkaRecordEntity inputEntity =
        makeInputEntity(SIMPLE_JSON_KEY_BYTES, SIMPLE_JSON_VALUE_BYTES, headers);

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

        Assertions.assertEquals(
            Arrays.asList(
                "bar",
                "kafka.newheader.kafkapkc",
                "kafka.newtopic.topic",
                "foo",
                "kafka.newts.timestamp",
                "kafka.newkey.key",
                "kafka.new.partition",
                "root_baz",
                "o",
                "path_omg",
                "jq_omg",
                "jq_omg2",
                "baz",
                "kafka.new.offset",
                "root_baz2",
                "kafka.newheader.encoding",
                "path_omg2"
            ),
            row.getDimensions()
        );

        // Payload verifications
        Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(ImmutableMap.of("mg", 1L), row.getRaw("o"));

        verifyHeader(row);

        // Key verification
        Assertions.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  private static final byte[] MULTI_FIELD_JSON_KEY_BYTES = StringUtils.toUtf8(
      TestUtils.singleQuoteToStandardJson(
          "{'tenant': 'acme', 'userId': 42, 'region': 'us-east', 'ts': '2021-06-26', 'loc': {'zone': 'b'}}"
      )
  );

  private static KafkaInputFormat makeKeyPrefixFormat(InputFormat keyFormat, @Nullable String keyColumnPrefix)
  {
    return new KafkaInputFormat(
        new KafkaStringHeaderFormat(null),
        keyFormat,
        new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), null, null, false, false),
        "kafka.newheader.",
        "kafka.newkey.key",
        "kafka.newts.timestamp",
        "kafka.newtopic.topic",
        "kafka.new.partition",
        "kafka.new.offset",
        keyColumnPrefix
    );
  }

  private static JsonInputFormat plainJsonFormat()
  {
    return new JsonInputFormat(new JSONPathSpec(true, ImmutableList.of()), null, null, false, false);
  }

  private static InputRow readOnlyRow(InputEntityReader reader) throws IOException
  {
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assertions.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assertions.assertFalse(iterator.hasNext());
      return row;
    }
  }

  private InputEntityReader discoveryReader(
      KafkaInputFormat kafkaInputFormat,
      KafkaRecordEntity entity,
      String tsColumn
  )
  {
    return kafkaInputFormat.createReader(
        new InputRowSchema(
            new TimestampSpec(tsColumn, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(entity),
        null
    );
  }

  @Test
  public void testSerdeWithKeyColumnPrefix() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final KafkaInputFormat withPrefix = makeKeyPrefixFormat(plainJsonFormat(), "kafka.key.");
    final KafkaInputFormat withoutPrefix = makeKeyPrefixFormat(plainJsonFormat(), null);

    Assertions.assertEquals("kafka.key.", withPrefix.getKeyColumnPrefix());
    Assertions.assertNull(withoutPrefix.getKeyColumnPrefix());
    Assertions.assertNotEquals(withPrefix, withoutPrefix);
    Assertions.assertNotEquals(withPrefix.hashCode(), withoutPrefix.hashCode());

    final Map<?, ?> withPrefixJson = mapper.readValue(mapper.writeValueAsBytes(withPrefix), Map.class);
    Assertions.assertEquals("kafka.key.", withPrefixJson.get("keyColumnPrefix"));

    // Specs that don't set the new property serialize exactly as before.
    final Map<?, ?> withoutPrefixJson = mapper.readValue(mapper.writeValueAsBytes(withoutPrefix), Map.class);
    Assertions.assertFalse(withoutPrefixJson.containsKey("keyColumnPrefix"));
  }

  @Test
  public void testKeyColumnPrefixIngestsAllJsonKeyFields() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(plainJsonFormat(), "kafka.key.");
    final KafkaRecordEntity inputEntity = makeInputEntity(
        MULTI_FIELD_JSON_KEY_BYTES,
        SIMPLE_JSON_VALUE_BYTES,
        new RecordHeaders(SAMPLE_HEADERS)
    );

    final InputRow row = readOnlyRow(discoveryReader(kafkaInputFormat, inputEntity, "timestamp"));

    Assertions.assertEquals(DateTimes.of("2021-06-25"), row.getTimestamp());
    Assertions.assertEquals("acme", Iterables.getOnlyElement(row.getDimension("kafka.key.tenant")));
    Assertions.assertEquals("42", Iterables.getOnlyElement(row.getDimension("kafka.key.userId")));
    Assertions.assertEquals(42L, row.getRaw("kafka.key.userId"));
    Assertions.assertEquals("us-east", Iterables.getOnlyElement(row.getDimension("kafka.key.region")));
    Assertions.assertEquals(ImmutableMap.of("zone", "b"), row.getRaw("kafka.key.loc"));
    Assertions.assertTrue(row.getDimensions().containsAll(
        ImmutableList.of("kafka.key.tenant", "kafka.key.userId", "kafka.key.region", "kafka.key.ts", "kafka.key.loc")
    ));

    // keyColumnName is not populated when keyColumnPrefix is set, and the internal timestamp never leaks.
    Assertions.assertFalse(row.getDimensions().contains("kafka.newkey.key"));
    Assertions.assertNull(row.getRaw("kafka.newkey.key"));
    Assertions.assertFalse(row.getDimensions().contains("kafka.key." + KafkaInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING));

    // Value and header columns are unaffected.
    Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
    verifyHeader(row);
  }

  @Test
  public void testKeyColumnPrefixWithKeyFlattenSpec() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(
        new JsonInputFormat(
            new JSONPathSpec(
                true,
                ImmutableList.of(new JSONPathFieldSpec(JSONPathFieldType.PATH, "zone", "$.loc.zone"))
            ),
            null,
            null,
            false,
            false
        ),
        "k_"
    );
    final KafkaRecordEntity inputEntity = makeInputEntity(
        MULTI_FIELD_JSON_KEY_BYTES,
        SIMPLE_JSON_VALUE_BYTES,
        new RecordHeaders(SAMPLE_HEADERS)
    );

    final InputRow row = readOnlyRow(discoveryReader(kafkaInputFormat, inputEntity, "timestamp"));
    Assertions.assertEquals("b", Iterables.getOnlyElement(row.getDimension("k_zone")));
    Assertions.assertEquals("acme", Iterables.getOnlyElement(row.getDimension("k_tenant")));
  }

  @Test
  public void testKeyColumnPrefixWithCsvKey() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(
        new CsvInputFormat(ImmutableList.of("tenant", "userId", "region"), null, false, false, 0, null),
        "kafka.key."
    );
    final KafkaRecordEntity inputEntity = makeInputEntity(
        StringUtils.toUtf8("acme,42,us-east"),
        SIMPLE_JSON_VALUE_BYTES,
        new RecordHeaders(SAMPLE_HEADERS)
    );

    final InputEntityReader reader = kafkaInputFormat.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of("foo", "kafka.key.tenant", "kafka.key.userId", "kafka.key.region")
                )
            ),
            ColumnsFilter.all()
        ),
        newSettableByteEntity(inputEntity),
        null
    );

    final InputRow row = readOnlyRow(reader);
    Assertions.assertEquals(
        ImmutableList.of("foo", "kafka.key.tenant", "kafka.key.userId", "kafka.key.region"),
        row.getDimensions()
    );
    Assertions.assertEquals("acme", Iterables.getOnlyElement(row.getDimension("kafka.key.tenant")));
    Assertions.assertEquals("42", Iterables.getOnlyElement(row.getDimension("kafka.key.userId")));
    Assertions.assertEquals("us-east", Iterables.getOnlyElement(row.getDimension("kafka.key.region")));
  }

  @Test
  public void testTimestampFromPrefixedKeyField() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(plainJsonFormat(), "kafka.key.");
    final KafkaRecordEntity inputEntity = makeInputEntity(
        MULTI_FIELD_JSON_KEY_BYTES,
        StringUtils.toUtf8("{\"foo\": \"x\"}"),
        new RecordHeaders(SAMPLE_HEADERS)
    );

    final InputRow row = readOnlyRow(discoveryReader(kafkaInputFormat, inputEntity, "kafka.key.ts"));
    Assertions.assertEquals(DateTimes.of("2021-06-26"), row.getTimestamp());
    Assertions.assertEquals("acme", Iterables.getOnlyElement(row.getDimension("kafka.key.tenant")));
  }

  @Test
  public void testKeyColumnPrefixPrecedence() throws IOException
  {
    // With an empty prefix, key fields become top-level columns. A value field with the same name wins, and a header
    // with the same name wins over the key, matching the existing keyColumnName behavior.
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(plainJsonFormat(), "");
    final Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    headers.add(new RecordHeader("region", StringUtils.toUtf8("from-header")));
    final KafkaRecordEntity inputEntity = makeInputEntity(
        StringUtils.toUtf8("{\"foo\": \"from-key\", \"tenant\": \"acme\", \"kafka.newheader.region\": \"from-key\"}"),
        SIMPLE_JSON_VALUE_BYTES,
        headers
    );

    final InputRow row = readOnlyRow(discoveryReader(kafkaInputFormat, inputEntity, "timestamp"));
    Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
    Assertions.assertEquals("acme", Iterables.getOnlyElement(row.getDimension("tenant")));
    Assertions.assertEquals("from-header", Iterables.getOnlyElement(row.getDimension("kafka.newheader.region")));
  }

  @Test
  public void testKeyColumnPrefixWithNullKey() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(plainJsonFormat(), "kafka.key.");
    final KafkaRecordEntity inputEntity =
        makeInputEntity(null, SIMPLE_JSON_VALUE_BYTES, new RecordHeaders(SAMPLE_HEADERS));

    final InputRow row = readOnlyRow(discoveryReader(kafkaInputFormat, inputEntity, "timestamp"));
    Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
    Assertions.assertTrue(row.getDimensions().stream().noneMatch(d -> d.startsWith("kafka.key.")));
  }

  @Test
  public void testKeyColumnPrefixSample() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(plainJsonFormat(), "kafka.key.");
    final KafkaRecordEntity inputEntity = makeInputEntity(
        MULTI_FIELD_JSON_KEY_BYTES,
        SIMPLE_JSON_VALUE_BYTES,
        new RecordHeaders(SAMPLE_HEADERS)
    );

    try (CloseableIterator<InputRowListPlusRawValues> iterator =
             discoveryReader(kafkaInputFormat, inputEntity, "timestamp").sample()) {
      Assertions.assertTrue(iterator.hasNext());
      final InputRowListPlusRawValues sampled = iterator.next();
      Assertions.assertFalse(iterator.hasNext());
      Assertions.assertNull(sampled.getParseException());

      final Map<String, Object> raw = Iterables.getOnlyElement(sampled.getRawValuesList());
      Assertions.assertEquals("acme", raw.get("kafka.key.tenant"));
      Assertions.assertEquals(42, ((Number) raw.get("kafka.key.userId")).intValue());
      Assertions.assertEquals("x", raw.get("foo"));
      Assertions.assertFalse(raw.containsKey("kafka.newkey.key"));

      final List<InputRow> rows = sampled.getInputRows();
      Assertions.assertEquals(1, rows.size());
      Assertions.assertEquals("us-east", Iterables.getOnlyElement(rows.get(0).getDimension("kafka.key.region")));
    }
  }

  @Test
  public void testKeyColumnPrefixSampleWithKeyFlattenSpec() throws IOException
  {
    final KafkaInputFormat kafkaInputFormat = makeKeyPrefixFormat(
        new JsonInputFormat(
            new JSONPathSpec(
                true,
                ImmutableList.of(new JSONPathFieldSpec(JSONPathFieldType.PATH, "zone", "$.loc.zone"))
            ),
            null,
            null,
            false,
            false
        ),
        "k_"
    );
    final KafkaRecordEntity inputEntity = makeInputEntity(
        MULTI_FIELD_JSON_KEY_BYTES,
        SIMPLE_JSON_VALUE_BYTES,
        new RecordHeaders(SAMPLE_HEADERS)
    );

    // The sampler should show the same key columns as ingestion, including flattened fields.
    try (CloseableIterator<InputRowListPlusRawValues> iterator =
             discoveryReader(kafkaInputFormat, inputEntity, "timestamp").sample()) {
      final InputRowListPlusRawValues sampled = iterator.next();
      final Map<String, Object> raw = Iterables.getOnlyElement(sampled.getRawValuesList());
      Assertions.assertEquals("b", raw.get("k_zone"));
      Assertions.assertEquals("acme", raw.get("k_tenant"));
      Assertions.assertEquals("b", Iterables.getOnlyElement(sampled.getInputRows().get(0).getDimension("k_zone")));
    }
  }

  private KafkaRecordEntity makeInputEntity(byte[] key, byte[] payload, Headers headers)
  {
    return new KafkaRecordEntity(
        new ConsumerRecord<>(
            TOPIC,
            0,
            0,
            TIMESTAMP_MILLIS,
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

  private void verifyHeader(InputRow row)
  {
    Assertions.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
    Assertions.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
    Assertions.assertEquals(
        String.valueOf(DateTimes.of("2021-06-24").getMillis()),
        Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp"))
    );
    Assertions.assertEquals(
        TOPIC,
        Iterables.getOnlyElement(row.getDimension("kafka.newtopic.topic"))
    );
    Assertions.assertEquals(
        "2021-06-25",
        Iterables.getOnlyElement(row.getDimension("timestamp"))
    );
  }

  private SettableByteEntity<KafkaRecordEntity> newSettableByteEntity(KafkaRecordEntity kafkaRecordEntity)
  {
    SettableByteEntity<KafkaRecordEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(kafkaRecordEntity);
    return settableByteEntity;
  }
}
