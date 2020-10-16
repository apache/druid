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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class JsonReaderTest
{
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
        null
    );

    //make sure JsonReader is used
    format.setLineSplittable(false);

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            Collections.emptyList()
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
        null
    );

    //make sure JsonReader is used
    format.setLineSplittable(false);

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
            Collections.emptyList()
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
        null
    );

    //make sure JsonReader is used
    format.setLineSplittable(false);

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4xxx,\"o\":{\"mg\":2}}" //baz property is illegal
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            Collections.emptyList()
        ),
        source,
        null
    );

    // the 2nd line is ill-formed, it stops to iterate to the 3rd line.
    // So in total, only 1 lines has been parsed
    final int numExpectedIterations = 1;

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        try {
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
        catch (Exception e) {
          //ignore the exception when parsing the 2nd
        }
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
        null
    );

    //make sure JsonReader is used
    format.setLineSplittable(false);

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            Collections.emptyList()
        ),
        source,
        null
    );

    final int numExpectedIterations = 3;
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRowListPlusRawValues rawValues = iterator.next();

        Assert.assertEquals(1, rawValues.getInputRows().size());
        InputRow row = rawValues.getInputRows().get(0);

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
}
