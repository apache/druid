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

package org.apache.druid.data.input.influx;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InfluxInputFormatTest
{
  private InputRowSchema schema;

  @BeforeEach
  public void setUp()
  {
    schema = new InputRowSchema(
        new TimestampSpec(InfluxLineProtocolReader.TIMESTAMP_KEY, "millis", null),
        DimensionsSpec.EMPTY,
        ColumnsFilter.all()
    );
  }

  @Test
  public void testParseRealSample() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "cpu,host=foo.bar.baz,region=us-east-1,application=echo pct_idle=99.3,pct_user=88.8,m1_load=2i 1465839830100400200"
    );
    Assertions.assertEquals("cpu", row.getDimension("measurement").get(0));
    Assertions.assertEquals(1465839830100L, row.getTimestampFromEpoch());
    Assertions.assertEquals("foo.bar.baz", row.getDimension("host").get(0));
    Assertions.assertEquals("us-east-1", row.getDimension("region").get(0));
    Assertions.assertEquals("echo", row.getDimension("application").get(0));
    Assertions.assertEquals(99.3, row.getMetric("pct_idle").doubleValue(), 0.0);
    Assertions.assertEquals(88.8, row.getMetric("pct_user").doubleValue(), 0.0);
    Assertions.assertEquals(2L, row.getMetric("m1_load").longValue());
  }

  @Test
  public void testParseNegativeTimestamp() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=500i -123456789"
    );
    Assertions.assertEquals("foo", row.getDimension("measurement").get(0));
    Assertions.assertEquals(-123L, row.getTimestampFromEpoch());
    Assertions.assertEquals("us-east-1", row.getDimension("region").get(0));
    Assertions.assertEquals("127.0.0.1", row.getDimension("host").get(0));
    Assertions.assertEquals(1.0, row.getMetric("m").doubleValue(), 0.0);
    Assertions.assertEquals(3.0, row.getMetric("n").doubleValue(), 0.0);
    Assertions.assertEquals(500L, row.getMetric("o").longValue());
  }

  @Test
  public void testParseTruncatedTimestamp() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=500i 123"
    );
    Assertions.assertEquals("foo", row.getDimension("measurement").get(0));
    Assertions.assertEquals(0L, row.getTimestampFromEpoch());
  }

  @Test
  public void testParseNoTags() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "foo m=1.0,n=3.0 123456789"
    );
    Assertions.assertEquals("foo", row.getDimension("measurement").get(0));
    Assertions.assertEquals(123L, row.getTimestampFromEpoch());
    Assertions.assertEquals(1.0, row.getMetric("m").doubleValue(), 0.0);
    Assertions.assertEquals(3.0, row.getMetric("n").doubleValue(), 0.0);
  }

  @Test
  public void testParseQuotedStringFieldValue() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "foo,region=us-east-1,host=127.0.0.1 m=1.0,n=3.0,o=\"something \\\"cool\\\" \" 123456789"
    );
    Assertions.assertEquals("foo", row.getDimension("measurement").get(0));
    Assertions.assertEquals(123L, row.getTimestampFromEpoch());
    Assertions.assertEquals("something \"cool\" ", row.getDimension("o").get(0));
  }

  @Test
  public void testParseUnicodeCharacters() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "\uD83D\uDE00,\uD83D\uDE05=\uD83D\uDE06 \uD83D\uDE0B=100i,b=\"\uD83D\uDE42\" 123456789"
    );
    Assertions.assertEquals("\uD83D\uDE00", row.getDimension("measurement").get(0));
    Assertions.assertEquals(123L, row.getTimestampFromEpoch());
    Assertions.assertEquals("\uD83D\uDE06", row.getDimension("\uD83D\uDE05").get(0));
    Assertions.assertEquals(100L, row.getMetric("\uD83D\uDE0B").longValue());
    Assertions.assertEquals("\uD83D\uDE42", row.getDimension("b").get(0));
  }

  @Test
  public void testParseEscapedCharactersInIdentifiers() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(null),
        "f\\,oo\\ \\=,bar=baz m=1.0,n=3.0 123456789"
    );
    Assertions.assertEquals("f,oo =", row.getDimension("measurement").get(0));
    Assertions.assertEquals(123L, row.getTimestampFromEpoch());
    Assertions.assertEquals("baz", row.getDimension("bar").get(0));
    Assertions.assertEquals(1.0, row.getMetric("m").doubleValue(), 0.0);
    Assertions.assertEquals(3.0, row.getMetric("n").doubleValue(), 0.0);
  }

  @Test
  public void testWhitelistPass() throws IOException
  {
    InputRow row = readSingleRow(
        new InfluxInputFormat(ImmutableList.of("cpu")),
        "cpu,host=foo.bar.baz,region=us-east pct_idle=99.3,pct_user=88.8,m1_load=2 1465839830100400200"
    );
    Assertions.assertEquals("cpu", row.getDimension("measurement").get(0));
  }

  @Test
  public void testWhitelistFail()
  {
    Assertions.assertThrows(
        ParseException.class,
        () -> readSingleRow(
            new InfluxInputFormat(ImmutableList.of("mem")),
            "cpu,host=foo.bar.baz,region=us-east pct_idle=99.3,pct_user=88.8,m1_load=2 1465839830100400200"
        )
    );
  }

  @Test
  public void testParseEmptyLine() throws IOException
  {
    // empty line is skipped (this was a parse exception in old InfluxParser)
    List<InputRow> rows = readAllRows(new InfluxInputFormat(null), "");
    Assertions.assertTrue(rows.isEmpty());
  }

  @Test
  public void testParseInvalidMeasurement()
  {
    Assertions.assertThrows(
        ParseException.class,
        () -> readSingleRow(new InfluxInputFormat(null), "invalid measurement")
    );
  }

  @Test
  public void testParseInvalidTimestamp()
  {
    Assertions.assertThrows(
        ParseException.class,
        () -> readSingleRow(new InfluxInputFormat(null), "foo i=123 123x")
    );
  }

  @Test
  public void testMultipleLines() throws IOException
  {
    String input = "cpu,host=a pct_idle=99.3 1465839830100400200\n"
                   + "mem,host=b used=1024i 1465839831100400200";
    InfluxInputFormat format = new InfluxInputFormat(null);
    List<InputRow> rows = readAllRows(format, input);
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals("cpu", rows.get(0).getDimension("measurement").get(0));
    Assertions.assertEquals("a", rows.get(0).getDimension("host").get(0));
    Assertions.assertEquals("mem", rows.get(1).getDimension("measurement").get(0));
    Assertions.assertEquals("b", rows.get(1).getDimension("host").get(0));
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new InfluxExtensionsModule().getJacksonModules()) {
      mapper.registerModule(module);
    }

    InfluxInputFormat format = new InfluxInputFormat(ImmutableList.of("cpu", "mem"));
    String json = mapper.writeValueAsString(format);
    InfluxInputFormat deserialized = (InfluxInputFormat) mapper.readValue(json, InputFormat.class);

    Assertions.assertEquals(format, deserialized);
    Assertions.assertEquals(format.getWhitelistMeasurements(), deserialized.getWhitelistMeasurements());
  }

  @Test
  public void testSerdeNoWhitelist() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new InfluxExtensionsModule().getJacksonModules()) {
      mapper.registerModule(module);
    }

    InfluxInputFormat format = new InfluxInputFormat(null);
    String json = mapper.writeValueAsString(format);
    InfluxInputFormat deserialized = (InfluxInputFormat) mapper.readValue(json, InputFormat.class);

    Assertions.assertEquals(format, deserialized);
    Assertions.assertNull(deserialized.getWhitelistMeasurements());
  }

  @Test
  public void testIsSplittable()
  {
    Assertions.assertFalse(new InfluxInputFormat(null).isSplittable());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(InfluxInputFormat.class).usingGetClass().verify();
  }

  private InputRow readSingleRow(InfluxInputFormat format, String line) throws IOException
  {
    InputEntityReader reader = format.createReader(schema, new ByteEntity(StringUtils.toUtf8(line)), null);
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assertions.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();
      Assertions.assertFalse(iterator.hasNext());
      return row;
    }
  }

  private List<InputRow> readAllRows(InfluxInputFormat format, String data) throws IOException
  {
    InputEntityReader reader = format.createReader(schema, new ByteEntity(StringUtils.toUtf8(data)), null);
    List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }
    return rows;
  }
}
