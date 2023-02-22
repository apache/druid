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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TransformerTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTransformNullRowReturnNull()
  {
    final Transformer transformer = new Transformer(new TransformSpec(null, null));
    Assert.assertNull(transformer.transform((InputRow) null));
    Assert.assertNull(transformer.transform((InputRowListPlusRawValues) null));
  }

  @Test
  public void testTransformTimeColumn()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("__time", "timestamp_shift(__time, 'P1D', -2)", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final DateTime now = DateTimes.nowUtc();
    final InputRow row = new MapBasedInputRow(
        now,
        ImmutableList.of("dim"),
        ImmutableMap.of("__time", now, "dim", false)
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(now.minusDays(2), actual.getTimestamp());
  }

  @Test
  public void testTransformTimeColumnWithInvalidTimeValue()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("__time", "timestamp_parse(ts, null, 'UTC')", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final DateTime now = DateTimes.nowUtc();
    final InputRow row = new MapBasedInputRow(
        now,
        ImmutableList.of("ts", "dim"),
        ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
    );
    if (NullHandling.replaceWithDefault()) {
      final InputRow actual = transformer.transform(row);
      Assert.assertNotNull(actual);
      Assert.assertEquals(DateTimes.of("1970-01-01T00:00:00.000Z"), actual.getTimestamp());
    } else {
      expectedException.expectMessage("Could not transform value for __time.");
      expectedException.expect(ParseException.class);
      transformer.transform(row);
    }
  }

  @Test
  public void testTransformTimeColumnWithInvalidTimeValueInputRowListPlusRawValues()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("__time", "timestamp_parse(ts, null, 'UTC')", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final DateTime now = DateTimes.nowUtc();
    final InputRow row = new MapBasedInputRow(
        now,
        ImmutableList.of("ts", "dim"),
        ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
    );
    final InputRowListPlusRawValues actual = transformer.transform(
        InputRowListPlusRawValues.of(
            row,
            ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
        )
    );
    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.getRawValuesList().size());
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(1, actual.getInputRows().size());
      Assert.assertEquals(DateTimes.of("1970-01-01T00:00:00.000Z"), actual.getInputRows().get(0).getTimestamp());
    } else {
      Assert.assertNull(actual.getInputRows());
      Assert.assertEquals("Could not transform value for __time.", actual.getParseException().getMessage());
    }
  }

  @Test
  public void testTransformWithStringTransformOnBooleanColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", false)
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assert.assertEquals(5L, actual.getRaw("dim"));
    Assert.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithStringTransformOnLongColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", 10L)
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assert.assertEquals(2L, actual.getRaw("dim"));
    Assert.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithStringTransformOnDoubleColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", 200.5d)
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assert.assertEquals(5L, actual.getRaw("dim"));
    Assert.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Ignore("Disabled until https://github.com/apache/druid/issues/9824 is fixed")
  @Test
  public void testTransformWithStringTransformOnListColumnThrowingException()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", ImmutableList.of(10, 20, 100))
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    // Unlike for querying, Druid doesn't explode multi-valued columns automatically for ingestion.
    expectedException.expect(AssertionError.class);
    actual.getRaw("dim");
  }

  @Test
  public void testTransformWithSelectorFilterWithStringBooleanValueOnBooleanColumnFilterAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(new SelectorDimFilter("dim", "false", null), null)
    );
    final InputRow row1 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", false)
    );
    Assert.assertEquals(row1, transformer.transform(row1));
    final InputRow row2 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", true)
    );
    Assert.assertNull(transformer.transform(row2));
  }

  @Test
  public void testTransformWithSelectorFilterWithStringBooleanValueOnStringColumn()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(new SelectorDimFilter("dim", "false", null), null)
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "false")
    );
    Assert.assertEquals(row, transformer.transform(row));
    final InputRow row2 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "true")
    );
    Assert.assertNull(transformer.transform(row2));
  }

  @Test
  public void testTransformWithTransformAndFilterTransformFirst()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            new SelectorDimFilter("dim", "0", null),
            // A boolean expression returns a long.
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim) == 10", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "short")
    );
    final InputRow actual = transformer.transform(row);
    Assert.assertNotNull(actual);
    Assert.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assert.assertEquals(0L, actual.getRaw("dim"));
    Assert.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testInputRowListPlusRawValuesTransformWithFilter()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            new SelectorDimFilter("dim", "val1", null),
            null
            )
    );
    List<InputRow> rows = Arrays.asList(
        new MapBasedInputRow(
            DateTimes.nowUtc(),
            ImmutableList.of("dim"),
            ImmutableMap.of("dim", "val1")
        ),

        //this row will be filtered
        new MapBasedInputRow(
            DateTimes.nowUtc(),
            ImmutableList.of("dim"),
            ImmutableMap.of("dim", "val2")
        )
    );
    List<Map<String, Object>> valList = Arrays.asList(
        ImmutableMap.of("dim", "val1"),
        ImmutableMap.of("dim", "val2")
    );

    final InputRowListPlusRawValues actual = transformer.transform(InputRowListPlusRawValues.ofList(valList, rows));
    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.getInputRows().size());
    Assert.assertEquals(1, actual.getRawValuesList().size());
    Assert.assertEquals("val1", actual.getInputRows().get(0).getRaw("dim"));
    Assert.assertEquals("val1", actual.getRawValuesList().get(0).get("dim"));
  }
}
