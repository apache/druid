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

package org.apache.druid.segment.virtual;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.BucketExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionVirtualColumnTest
{
  private static final InputRow ROW0 = new MapBasedInputRow(
      DateTimes.of("2000-01-01T00:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of()
  );

  private static final InputRow ROW1 = new MapBasedInputRow(
      DateTimes.of("2000-01-01T00:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of("x", 4)
  );

  private static final InputRow ROW2 = new MapBasedInputRow(
      DateTimes.of("2000-01-01T02:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of("x", 2.1, "y", 3L, "z", "foobar")
  );
  private static final InputRow ROW3 = new MapBasedInputRow(
      DateTimes.of("2000-01-02T01:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of("x", 2L, "y", 3L, "z", "foobar")
  );

  private static final ExpressionVirtualColumn X_PLUS_Y = new ExpressionVirtualColumn(
      "expr",
      "x + y",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn CONSTANT_LIKE = new ExpressionVirtualColumn(
      "expr",
      "like('foo', 'f%')",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn CONSTANT_NULL_ARITHMETIC = new ExpressionVirtualColumn(
      "expr",
      "2.1 + null",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_LIKE = new ExpressionVirtualColumn(
      "expr",
      "like(z, 'f%')",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_CONCAT_X = new ExpressionVirtualColumn(
      "expr",
      "z + cast(x, 'string')",
      ValueType.STRING,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_CONCAT_NONEXISTENT = new ExpressionVirtualColumn(
      "expr",
      "concat(z, nonexistent)",
      ValueType.STRING,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn TIME_FLOOR = new ExpressionVirtualColumn(
      "expr",
      "timestamp_floor(__time, 'P1D')",
      ValueType.LONG,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn SCALE_LONG = new ExpressionVirtualColumn(
      "expr",
      "x * 2",
      ValueType.LONG,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn SCALE_FLOAT = new ExpressionVirtualColumn(
      "expr",
      "x * 2",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );

  private static final ThreadLocal<Row> CURRENT_ROW = new ThreadLocal<>();
  private static final ColumnSelectorFactory COLUMN_SELECTOR_FACTORY = RowBasedColumnSelectorFactory.create(
      CURRENT_ROW,
      null
  );

  @Test
  public void testObjectSelector()
  {
    final BaseObjectColumnValueSelector selector = X_PLUS_Y.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(null, selector.getObject());

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(4.0d, selector.getObject());
    } else {
      // y is null for row1
      Assert.assertEquals(null, selector.getObject());
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5.1d, selector.getObject());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5L, selector.getObject());
  }

  @Test
  public void testLongSelector()
  {
    final BaseLongColumnValueSelector selector = X_PLUS_Y.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0L, selector.getLong());
    } else {
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(4L, selector.getLong());
    } else {
      // y is null for row1
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5L, selector.getLong());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5L, selector.getLong());
  }

  @Test
  public void testLongSelectorUsingStringFunction()
  {
    final BaseLongColumnValueSelector selector = Z_CONCAT_X.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0L, selector.getLong());
    } else {
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(4L, selector.getLong());
    } else {
      // y is null for row1
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW2);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0L, selector.getLong());
    } else {
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW3);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0L, selector.getLong());
    } else {
      Assert.assertTrue(selector.isNull());
    }
  }

  @Test
  public void testFloatSelector()
  {
    final BaseFloatColumnValueSelector selector = X_PLUS_Y.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0.0f, selector.getFloat(), 0.0f);
    } else {
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(4.0f, selector.getFloat(), 0.0f);
    } else {
      // y is null for row1
      Assert.assertTrue(selector.isNull());
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5.1f, selector.getFloat(), 0.0f);

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5.0f, selector.getFloat(), 0.0f);
  }

  @Test
  public void testDimensionSelector()
  {
    final DimensionSelector selector = X_PLUS_Y.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.notNull());

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(false, nullMatcher.matches());
      Assert.assertEquals(false, fiveMatcher.matches());
      Assert.assertEquals(true, nonNullMatcher.matches());
      Assert.assertEquals("4.0", selector.lookupName(selector.getRow().get(0)));
    } else {
      // y is null in row1
      Assert.assertEquals(true, nullMatcher.matches());
      Assert.assertEquals(false, fiveMatcher.matches());
      Assert.assertEquals(false, nonNullMatcher.matches());
      Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5.1", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(true, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testDimensionSelectorUsingStringFunction()
  {
    final DimensionSelector selector = Z_CONCAT_X.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    Assert.assertNotNull(selector);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals(
        NullHandling.replaceWithDefault() ? "4" : null,
        selector.lookupName(selector.getRow().get(0))
    );

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals("foobar2.1", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals("foobar2", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testDimensionSelectorUsingNonexistentColumn()
  {
    final DimensionSelector selector = Z_CONCAT_NONEXISTENT.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    Assert.assertNotNull(selector);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertNull(selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertNull(selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals(
        NullHandling.replaceWithDefault() ? "foobar" : null,
        selector.lookupName(selector.getRow().get(0))
    );

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals(
        NullHandling.replaceWithDefault() ? "foobar" : null,
        selector.lookupName(selector.getRow().get(0))
    );
  }

  @Test
  public void testDimensionSelectorWithExtraction()
  {
    final DimensionSelector selector = X_PLUS_Y.makeDimensionSelector(
        new ExtractionDimensionSpec("expr", "x", new BucketExtractionFn(1.0, 0.0)),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.notNull());

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(false, nullMatcher.matches());
      Assert.assertEquals(false, fiveMatcher.matches());
      Assert.assertEquals(true, nonNullMatcher.matches());
      Assert.assertEquals("4", selector.lookupName(selector.getRow().get(0)));
    } else {
      // y is null in row1
      Assert.assertEquals(true, nullMatcher.matches());
      Assert.assertEquals(false, fiveMatcher.matches());
      Assert.assertEquals(false, nonNullMatcher.matches());
      Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(true, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(true, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("5", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testLongSelectorWithConstantLikeExprMacro()
  {
    final BaseLongColumnValueSelector selector =
        CONSTANT_LIKE.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(1L, selector.getLong());
  }

  @Test
  public void testLongSelectorWithConstantNullArithmetic()
  {
    final BaseLongColumnValueSelector selector =
        CONSTANT_NULL_ARITHMETIC.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(2L, selector.getLong());
      Assert.assertFalse(selector.isNull());
    } else {
      Assert.assertTrue(selector.isNull());
    }
  }

  @Test
  public void testFloatSelectorWithConstantNullArithmetic()
  {
    final BaseFloatColumnValueSelector selector =
        CONSTANT_NULL_ARITHMETIC.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(2.1f, selector.getFloat(), 0.0f);
      Assert.assertFalse(selector.isNull());
    } else {
      Assert.assertTrue(selector.isNull());
    }
  }

  @Test
  public void testExprEvalSelectorWithConstantNullArithmetic()
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(
        COLUMN_SELECTOR_FACTORY,
        Parser.parse(CONSTANT_NULL_ARITHMETIC.getExpression(), TestExprMacroTable.INSTANCE)
    );

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(2.1f, selector.getFloat(), 0.0f);
      Assert.assertFalse(selector.isNull());
      Assert.assertEquals(2.1d, selector.getObject().asDouble(), 0.0d);
    } else {
      Assert.assertTrue(selector.isNull());
      Assert.assertTrue(selector.getObject().isNumericNull());
    }
  }

  @Test
  public void testLongSelectorWithZLikeExprMacro()
  {
    final ColumnValueSelector selector = Z_LIKE.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(0L, selector.getLong());

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(0L, selector.getLong());

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(1L, selector.getLong());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(1L, selector.getLong());
  }

  @Test
  public void testLongSelectorOfTimeColumn()
  {
    final ColumnValueSelector selector = TIME_FLOOR.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getLong());
    Assert.assertEquals((float) DateTimes.of("2000-01-01").getMillis(), selector.getFloat(), 0.0f);
    Assert.assertEquals((double) DateTimes.of("2000-01-01").getMillis(), selector.getDouble(), 0.0d);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getObject());

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getLong());

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getLong());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(DateTimes.of("2000-01-02").getMillis(), selector.getLong());
    Assert.assertEquals(DateTimes.of("2000-01-02").getMillis(), selector.getDouble(), 0.0);
  }

  @Test
  public void testRequiredColumns()
  {
    Assert.assertEquals(ImmutableList.of("x", "y"), X_PLUS_Y.requiredColumns());
    Assert.assertEquals(ImmutableList.of(), CONSTANT_LIKE.requiredColumns());
    Assert.assertEquals(ImmutableList.of("z"), Z_LIKE.requiredColumns());
    Assert.assertEquals(ImmutableList.of("z", "x"), Z_CONCAT_X.requiredColumns());
  }

  @Test
  public void testExprEvalSelectorWithLongsAndNulls()
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(
        RowBasedColumnSelectorFactory.create(
            CURRENT_ROW,
            ImmutableMap.of("x", ValueType.LONG)
        ),
        Parser.parse(SCALE_LONG.getExpression(), TestExprMacroTable.INSTANCE)
    );

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, selector.getLong(), 0.0f);
      Assert.assertFalse(selector.isNull());
    } else {
      Assert.assertTrue(selector.isNull());
      Assert.assertTrue(selector.getObject().isNumericNull());
    }
  }

  @Test
  public void testExprEvalSelectorWithDoublesAndNulls()
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(
        RowBasedColumnSelectorFactory.create(
            CURRENT_ROW,
            ImmutableMap.of("x", ValueType.DOUBLE)
        ),
        Parser.parse(SCALE_FLOAT.getExpression(), TestExprMacroTable.INSTANCE)
    );

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, selector.getDouble(), 0.0f);
      Assert.assertFalse(selector.isNull());
    } else {
      Assert.assertTrue(selector.isNull());
      Assert.assertTrue(selector.getObject().isNumericNull());
    }
  }

  @Test
  public void testExprEvalSelectorWithFloatAndNulls()
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(
        RowBasedColumnSelectorFactory.create(
            CURRENT_ROW,
            ImmutableMap.of("x", ValueType.FLOAT)
        ),
        Parser.parse(SCALE_FLOAT.getExpression(), TestExprMacroTable.INSTANCE)
    );

    CURRENT_ROW.set(ROW0);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, selector.getFloat(), 0.0f);
      Assert.assertFalse(selector.isNull());
    } else {
      Assert.assertTrue(selector.isNull());
      Assert.assertTrue(selector.getObject().isNumericNull());
    }
  }
}
