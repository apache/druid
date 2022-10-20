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
import java.util.Arrays;
import java.util.Collections;

public class JsonLineReaderTest
{
  @Test
  public void testParseRow() throws IOException
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
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}")
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
  public void testParseRowWithConditional() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foo", "$.[?(@.maybe_object)].maybe_object.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "baz", "$.maybe_object_2.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar", "$.[?(@.something_else)].something_else.foo")
            )
        ),
        null,
        null,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"something_else\": {\"foo\": \"test\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))),
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
        Assert.assertEquals("test", Iterables.getOnlyElement(row.getDimension("bar")));
        Assert.assertEquals(Collections.emptyList(), row.getDimension("foo"));
        Assert.assertTrue(row.getDimension("baz").isEmpty());
        numActualIterations++;
      }
      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testParseRowKeepNullColumns() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg")
            )
        ),
        null,
        true,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"o\":{\"mg\":null}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
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
        Assert.assertEquals(Arrays.asList("path_omg", "timestamp", "bar", "foo"), row.getDimensions());
        Assert.assertTrue(row.getDimension("bar").isEmpty());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertTrue(row.getDimension("path_omg").isEmpty());
        numActualIterations++;
      }
      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testKeepNullColumnsWithNoNullValues() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg")
            )
        ),
        null,
        true,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":1,\"foo\":\"x\",\"o\":{\"mg\":\"a\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
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
        Assert.assertEquals(Arrays.asList("path_omg", "timestamp", "bar", "foo"), row.getDimensions());
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("bar")));
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("a", Iterables.getOnlyElement(row.getDimension("path_omg")));
        numActualIterations++;
      }
      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testFalseKeepNullColumns() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg")
            )
        ),
        null,
        false,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"o\":{\"mg\":\"a\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
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
        Assert.assertEquals(Arrays.asList("path_omg", "timestamp", "foo"), row.getDimensions());
        Assert.assertTrue(row.getDimension("bar").isEmpty());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("a", Iterables.getOnlyElement(row.getDimension("path_omg")));
        numActualIterations++;
      }
      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }
}
