/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.TestObjectMapper;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class InputRowParserSerdeTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testStringInputRowParserSerde() throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo", "bar")), null, null),
            null,
            null
        ),
        null
    );
    final ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        ByteBufferInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ByteBuffer.wrap(
            "{\"foo\":\"x\",\"bar\":\"y\",\"qux\":\"z\",\"timestamp\":\"2000\"}".getBytes(Charsets.UTF_8)
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "bar"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(new DateTime("2000").getMillis(), parsed.getTimestampFromEpoch());
  }

  @Test
  public void testStringInputRowParserSerdeMultiCharset() throws Exception
  {
    Charset[] testCharsets = {
        Charsets.US_ASCII, Charsets.ISO_8859_1, Charsets.UTF_8,
        Charsets.UTF_16BE, Charsets.UTF_16LE, Charsets.UTF_16
    };

    for (Charset testCharset : testCharsets) {
      InputRow parsed = testCharsetParseHelper(testCharset);
      Assert.assertEquals(ImmutableList.of("foo", "bar"), parsed.getDimensions());
      Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
      Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
      Assert.assertEquals(new DateTime("3000").getMillis(), parsed.getTimestampFromEpoch());
    }
  }

  @Test
  public void testMapInputRowParserSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timeposix", "posix", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo", "bar")), ImmutableList.of("baz"), null),
            null,
            null
        )
    );
    final MapInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        MapInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ImmutableMap.<String, Object>of(
            "foo", "x",
            "bar", "y",
            "qux", "z",
            "timeposix", "1"
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "bar"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("x"), parsed.getDimension("foo"));
    Assert.assertEquals(ImmutableList.of("y"), parsed.getDimension("bar"));
    Assert.assertEquals(1000, parsed.getTimestampFromEpoch());
  }

  @Test
  public void testMapInputRowParserNumbersSerde() throws Exception
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timemillis", "millis", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo", "values")), ImmutableList.of("toobig", "value"), null),
            null,
            null
        )
    );
    final MapInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        MapInputRowParser.class
    );
    final InputRow parsed = parser2.parse(
        ImmutableMap.<String, Object>of(
            "timemillis", 1412705931123L,
            "toobig", 123E64,
            "value", 123.456,
            "long", 123456789000L,
            "values", Lists.newArrayList(1412705931123L, 123.456, 123E45, "hello")
        )
    );
    Assert.assertEquals(ImmutableList.of("foo", "values"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of(), parsed.getDimension("foo"));
    Assert.assertEquals(
        ImmutableList.of("1412705931123", "123.456", "1.23E47", "hello"),
        parsed.getDimension("values")
    );
    Assert.assertEquals(Float.POSITIVE_INFINITY, parsed.getFloatMetric("toobig"), 0.0);
    Assert.assertEquals(123E64, parsed.getRaw("toobig"));
    Assert.assertEquals(123.456f, parsed.getFloatMetric("value"), 0.0f);
    Assert.assertEquals(123456789000L, parsed.getRaw("long"));
    Assert.assertEquals(1.23456791E11f, parsed.getFloatMetric("long"), 0.0f);
    Assert.assertEquals(1412705931123L, parsed.getTimestampFromEpoch());
  }

  private InputRow testCharsetParseHelper(Charset charset) throws Exception
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo", "bar")), null, null),
            null,
            null
        ),
        charset.name()
    );

    final ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        ByteBufferInputRowParser.class
    );

    final InputRow parsed = parser2.parse(
        ByteBuffer.wrap(
            "{\"foo\":\"x\",\"bar\":\"y\",\"qux\":\"z\",\"timestamp\":\"3000\"}".getBytes(charset)
        )
    );

    return parsed;
  }

  @Test
  public void testFlattenParse() throws Exception
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(JSONPathFieldSpec.createNestedField("foobar1", "$.foo.bar1"));
    fields.add(JSONPathFieldSpec.createNestedField("foobar2", "$.foo.bar2"));
    fields.add(JSONPathFieldSpec.createNestedField("baz0", "$.baz[0]"));
    fields.add(JSONPathFieldSpec.createNestedField("baz1", "$.baz[1]"));
    fields.add(JSONPathFieldSpec.createNestedField("baz2", "$.baz[2]"));
    fields.add(JSONPathFieldSpec.createNestedField("hey0barx", "$.hey[0].barx"));
    fields.add(JSONPathFieldSpec.createNestedField("metA", "$.met.a"));
    fields.add(JSONPathFieldSpec.createRootField("timestamp"));
    fields.add(JSONPathFieldSpec.createRootField("foo.bar1"));

    JSONPathSpec flattenSpec = new JSONPathSpec(true, fields);
    final StringInputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(null, null, null),
            flattenSpec,
            null
        ),
        null
    );

    final StringInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        StringInputRowParser.class
    );

    final InputRow parsed = parser2.parse(
        "{\"blah\":[4,5,6], \"newmet\":5, \"foo\":{\"bar1\":\"aaa\", \"bar2\":\"bbb\"}, \"baz\":[1,2,3], \"timestamp\":\"2999\", \"foo.bar1\":\"Hello world!\", \"hey\":[{\"barx\":\"asdf\"}], \"met\":{\"a\":456}}"
    );
    Assert.assertEquals(ImmutableList.of("foobar1", "foobar2", "baz0", "baz1", "baz2", "hey0barx", "metA", "timestamp", "foo.bar1", "blah", "newmet", "baz"), parsed.getDimensions());
    Assert.assertEquals(ImmutableList.of("aaa"), parsed.getDimension("foobar1"));
    Assert.assertEquals(ImmutableList.of("bbb"), parsed.getDimension("foobar2"));
    Assert.assertEquals(ImmutableList.of("1"), parsed.getDimension("baz0"));
    Assert.assertEquals(ImmutableList.of("2"), parsed.getDimension("baz1"));
    Assert.assertEquals(ImmutableList.of("3"), parsed.getDimension("baz2"));
    Assert.assertEquals(ImmutableList.of("Hello world!"), parsed.getDimension("foo.bar1"));
    Assert.assertEquals(ImmutableList.of("asdf"), parsed.getDimension("hey0barx"));
    Assert.assertEquals(ImmutableList.of("456"), parsed.getDimension("metA"));
    Assert.assertEquals(ImmutableList.of("5"), parsed.getDimension("newmet"));
    Assert.assertEquals(new DateTime("2999").getMillis(), parsed.getTimestampFromEpoch());

    String testSpec = "{\"enabled\": true,\"useFieldDiscovery\": true, \"fields\": [\"parseThisRootField\"]}";
    final JSONPathSpec parsedSpec = jsonMapper.readValue(testSpec, JSONPathSpec.class);
    List<JSONPathFieldSpec> fieldSpecs = parsedSpec.getFields();
    Assert.assertEquals(JSONPathFieldType.ROOT, fieldSpecs.get(0).getType());
    Assert.assertEquals("parseThisRootField", fieldSpecs.get(0).getName());
    Assert.assertEquals(null, fieldSpecs.get(0).getExpr());
  }

}
