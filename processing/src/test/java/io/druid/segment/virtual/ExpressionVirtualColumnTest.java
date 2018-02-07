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

package io.druid.segment.virtual;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.DateTimes;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.extraction.BucketExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.BaseFloatColumnValueSelector;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.BaseObjectColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ValueType;
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

  private static final ExpressionVirtualColumn XPLUSY = new ExpressionVirtualColumn(
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
  private static final ExpressionVirtualColumn ZLIKE = new ExpressionVirtualColumn(
      "expr",
      "like(z, 'f%')",
      ValueType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn ZCONCATX = new ExpressionVirtualColumn(
      "expr",
      "z + cast(x, 'string')",
      ValueType.STRING,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn TIMEFLOOR = new ExpressionVirtualColumn(
      "expr",
      "timestamp_floor(__time, 'P1D')",
      ValueType.LONG,
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
    final BaseObjectColumnValueSelector selector = XPLUSY.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(null, selector.getObject());

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(4.0d, selector.getObject());

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5.1d, selector.getObject());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5L, selector.getObject());
  }

  @Test
  public void testLongSelector()
  {
    final BaseLongColumnValueSelector selector = XPLUSY.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(0L, selector.getLong());

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(4L, selector.getLong());

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5L, selector.getLong());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5L, selector.getLong());
  }

  @Test
  public void testLongSelectorUsingStringFunction()
  {
    final BaseLongColumnValueSelector selector = ZCONCATX.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(0L, selector.getLong());

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(4L, selector.getLong());

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(0L, selector.getLong());

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(0L, selector.getLong());
  }

  @Test
  public void testFloatSelector()
  {
    final BaseFloatColumnValueSelector selector = XPLUSY.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(0.0f, selector.getFloat(), 0.0f);

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(4.0f, selector.getFloat(), 0.0f);

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(5.1f, selector.getFloat(), 0.0f);

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(5.0f, selector.getFloat(), 0.0f);
  }

  @Test
  public void testDimensionSelector()
  {
    final DimensionSelector selector = XPLUSY.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.<String>notNull());

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("4.0", selector.lookupName(selector.getRow().get(0)));

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
    final DimensionSelector selector = ZCONCATX.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    Assert.assertNotNull(selector);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals("4", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals("foobar2.1", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(1, selector.getRow().size());
    Assert.assertEquals("foobar2", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testDimensionSelectorWithExtraction()
  {
    final DimensionSelector selector = XPLUSY.makeDimensionSelector(
        new ExtractionDimensionSpec("expr", "x", new BucketExtractionFn(1.0, 0.0)),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nullMatcher = selector.makeValueMatcher((String) null);
    final ValueMatcher fiveMatcher = selector.makeValueMatcher("5");
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(Predicates.<String>notNull());

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(false, nonNullMatcher.matches());
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    Assert.assertEquals(false, nullMatcher.matches());
    Assert.assertEquals(false, fiveMatcher.matches());
    Assert.assertEquals(true, nonNullMatcher.matches());
    Assert.assertEquals("4", selector.lookupName(selector.getRow().get(0)));

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
  public void testLongSelectorWithZLikeExprMacro()
  {
    final ColumnValueSelector selector = ZLIKE.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

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
    final ColumnValueSelector selector = TIMEFLOOR.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

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
    Assert.assertEquals(ImmutableList.of("x", "y"), XPLUSY.requiredColumns());
    Assert.assertEquals(ImmutableList.of(), CONSTANT_LIKE.requiredColumns());
    Assert.assertEquals(ImmutableList.of("z"), ZLIKE.requiredColumns());
    Assert.assertEquals(ImmutableList.of("z", "x"), ZCONCATX.requiredColumns());
  }
}
