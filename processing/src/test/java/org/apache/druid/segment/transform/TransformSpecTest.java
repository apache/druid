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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransformSpecTest extends InitializedNullHandlingTest
{
  private static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec("t", "auto", DateTimes.of("2000-01-01")),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("f", "x", "y"))),
      ColumnsFilter.all()
  );

  private static final Map<String, Object> ROW1 = ImmutableMap.<String, Object>builder()
                                                              .put("x", "foo")
                                                              .put("y", "bar")
                                                              .put("a", 2.0)
                                                              .put("b", 3L)
                                                              .put("bool", true)
                                                              .build();

  private static final Map<String, Object> ROW2 = ImmutableMap.<String, Object>builder()
                                                              .put("x", "foo")
                                                              .put("y", "baz")
                                                              .put("a", 2.0)
                                                              .put("b", 4L)
                                                              .put("bool", false)
                                                              .build();

  @SafeVarargs
  private static InputSourceReader makeReader(Map<String, Object>... rows)
  {
    final List<InputRow> inputRows = Arrays.stream(rows)
                                           .map(row -> MapInputRowParser.parse(SCHEMA, row))
                                           .collect(Collectors.toList());
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read(InputStats inputStats)
      {
        return CloseableIterators.withEmptyBaggage(inputRows.iterator());
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Test
  public void testTransforms() throws IOException
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("h", "concat(f,g)", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("x", "y", "a", "b", "f", "g"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1)).read()) {
      final InputRow row = iterator.next();

      Assert.assertNotNull(row);
      Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), row.getTimestampFromEpoch());
      Assert.assertEquals(DateTimes.of("2000-01-01"), row.getTimestamp());
      Assert.assertEquals(ImmutableList.of("f", "x", "y"), row.getDimensions());
      Assert.assertEquals(ImmutableList.of("foo"), row.getDimension("x"));
      Assert.assertEquals(3.0, row.getMetric("b").doubleValue(), 0);
      Assert.assertEquals("foobar", row.getRaw("f"));
      Assert.assertEquals(ImmutableList.of("foobar"), row.getDimension("f"));
      Assert.assertEquals(ImmutableList.of("5.0"), row.getDimension("g"));
      Assert.assertEquals(ImmutableList.of(), row.getDimension("h"));
      Assert.assertEquals(5L, row.getMetric("g").longValue());
    }
  }

  @Test
  public void testTransformOverwriteField() throws IOException
  {
    // Transforms are allowed to overwrite fields, and to refer to the fields they overwrite; double-check this.

    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("x", "concat(x,y)", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("x", "y"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1)).read()) {
      final InputRow row = iterator.next();

      Assert.assertNotNull(row);
      Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), row.getTimestampFromEpoch());
      Assert.assertEquals(DateTimes.of("2000-01-01"), row.getTimestamp());
      Assert.assertEquals(ImmutableList.of("f", "x", "y"), row.getDimensions());
      Assert.assertEquals(ImmutableList.of("foobar"), row.getDimension("x"));
      Assert.assertEquals(3.0, row.getMetric("b").doubleValue(), 0);
      Assert.assertNull(row.getRaw("f"));
    }
  }

  @Test
  public void testFilterOnTransforms() throws IOException
  {
    // Filters are allowed to refer to transformed fields; double-check this.

    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("x", "foo", null),
                new SelectorDimFilter("f", "foobar", null),
                new SelectorDimFilter("g", "5.0", null)
            )
        ),
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("x", "f", "g", "y", "a", "b"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1)).read()) {
      Assert.assertNotNull(iterator.next());
    }
    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW2)).read()) {
      Assert.assertNull(iterator.next());
    }
  }

  @Test
  public void testTransformTimeFromOtherFields() throws IOException
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("__time", "(a + b) * 3600000", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("a", "b"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1)).read()) {
      final InputRow row = iterator.next();

      Assert.assertNotNull(row);
      Assert.assertEquals(DateTimes.of("1970-01-01T05:00:00Z"), row.getTimestamp());
      Assert.assertEquals(DateTimes.of("1970-01-01T05:00:00Z").getMillis(), row.getTimestampFromEpoch());
    }
  }

  @Test
  public void testTransformTimeFromTime() throws IOException
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("__time", "__time + 3600000", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("__time"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1)).read()) {
      final InputRow row = iterator.next();

      Assert.assertNotNull(row);
      Assert.assertEquals(DateTimes.of("2000-01-01T01:00:00Z"), row.getTimestamp());
      Assert.assertEquals(DateTimes.of("2000-01-01T01:00:00Z").getMillis(), row.getTimestampFromEpoch());
    }
  }

  @Test
  public void testBoolTransforms() throws IOException
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("truthy1", "bool", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("truthy2", "if(bool,1,0)", TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(
        ImmutableSet.of("bool"),
        transformSpec.getRequiredColumns()
    );

    try (CloseableIterator<InputRow> iterator = transformSpec.decorate(makeReader(ROW1, ROW2)).read()) {
      final InputRow row = iterator.next();

      Assert.assertNotNull(row);
      Assert.assertEquals(1L, row.getRaw("truthy1"));
      Assert.assertEquals(1L, row.getRaw("truthy2"));

      final InputRow row2 = iterator.next();

      Assert.assertNotNull(row2);
      Assert.assertEquals(0L, row2.getRaw("truthy1"));
      Assert.assertEquals(0L, row2.getRaw("truthy2"));
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("x", "foo", null),
                new SelectorDimFilter("f", "foobar", null),
                new SelectorDimFilter("g", "5.0", null)
            )
        ),
        ImmutableList.of(
            new ExpressionTransform("f", "concat(x,y)", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("g", "a + b", TestExprMacroTable.INSTANCE)
        )
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Assert.assertEquals(
        transformSpec,
        jsonMapper.readValue(jsonMapper.writeValueAsString(transformSpec), TransformSpec.class)
    );
  }
}