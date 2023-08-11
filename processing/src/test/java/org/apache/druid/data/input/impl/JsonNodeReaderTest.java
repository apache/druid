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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class JsonNodeReaderTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseMultipleRows() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used,
        false,
        true
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    final int numExpectedIterations = 3;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        final String msgId = String.valueOf(++numActualIterations);
        Assert.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("jq_omg")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testParsePrettyFormatJSON() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\n"
                           + "    \"timestamp\": \"2019-01-01\",\n"
                           + "    \"bar\": null,\n"
                           + "    \"foo\": \"x\",\n"
                           + "    \"baz\": 4,\n"
                           + "    \"o\": {\n"
                           + "        \"mg\": 1\n"
                           + "    }\n"
                           + "}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        Assert.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testInvalidJSONText() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4xxx,\"o\":{\"mg\":2}}"
                           //baz property is illegal
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    //expect a ParseException on the following `next` call on iterator
    expectedException.expect(ParseException.class);

    // the 2nd line is ill-formed, so the parse of this text chunk aborts
    final int numExpectedIterations = 0;

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        iterator.next();
        ++numActualIterations;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testSampleMultipleRows() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    int acturalRowCount = 0;
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      while (iterator.hasNext()) {

        final InputRowListPlusRawValues rawValues = iterator.next();

        // 1 row returned 3 times
        Assert.assertEquals(1, rawValues.getInputRows().size());
        InputRow row = rawValues.getInputRows().get(0);

        final String msgId = String.valueOf(++acturalRowCount);
        Assert.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("jq_omg")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

      }
    }

    Assert.assertEquals(3, acturalRowCount);
  }

  @Test
  public void testSamplInvalidJSONText() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    //2nd row is has an invalid timestamp which causes a parse exception, but is valid JSON
    //3rd row is malformed json and terminates the row iteration
    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"invalidtimestamp\",\"bar\":null,\"foo\":\"x\",\"baz\":5,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4xxx,\"o\":{\"mg\":2}}\n"
                           //value of baz is invalid
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    // the invalid timestamp in line 2 causes a parse exception, but because it is valid JSON, parsing can continue
    // the invalid character in line 3 stops parsing of the 4-line text as a whole
    // so the total num of iteration is 3
    final int numExpectedIterations = 3;

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        numActualIterations++;

        final InputRowListPlusRawValues rawValues = iterator.next();

        if (numActualIterations == 2 || numActualIterations == 3) {
          Assert.assertNotNull(rawValues.getParseException());
        }
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testEmptyJSONText() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    //input is empty
    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8(
            "" // empty row
        )
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    //expect a ParseException on the following `next` call on iterator
    expectedException.expect(ParseException.class);

    // the 2nd line is ill-formed, so the parse of this text chunk aborts
    final int numExpectedIterations = 0;

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        iterator.next();
        ++numActualIterations;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }



  @Test
  public void testSampleEmptyText() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
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
        false, //make sure JsonReader is used
        false,
        true
    );

    //input is empty
    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    // the total num of iteration is 1
    final int numExpectedIterations = 1;

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        numActualIterations++;

        final InputRowListPlusRawValues rawValues = iterator.next();

        Assert.assertNotNull(rawValues.getParseException());
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }
}
