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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.BucketExtractionFn;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.ConstantMultiValueDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;

public class ExpressionVirtualColumnTest extends InitializedNullHandlingTest
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

  private static final InputRow ROWMULTI = new MapBasedInputRow(
      DateTimes.of("2000-01-02T01:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of(
          "x", 2L,
          "y", 3L,
          "a", ImmutableList.of("a", "b", "c"),
          "b", ImmutableList.of("1", "2", "3"),
          "c", ImmutableList.of("4", "5", "6")
      )
  );
  private static final InputRow ROWMULTI2 = new MapBasedInputRow(
      DateTimes.of("2000-01-02T01:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of(
          "x", 3L,
          "y", 4L,
          "a", ImmutableList.of("d", "e", "f"),
          "b", ImmutableList.of("3", "4", "5"),
          "c", ImmutableList.of("7", "8", "9")
      )
  );
  private static final InputRow ROWMULTI3 = new MapBasedInputRow(
      DateTimes.of("2000-01-02T01:00:00").getMillis(),
      ImmutableList.of(),
      ImmutableMap.of(
          "x", 3L,
          "y", 4L,
          "b", Arrays.asList(new String[]{"3", null, "5"})
      )
  );

  private static final ExpressionVirtualColumn X_PLUS_Y = new ExpressionVirtualColumn(
      "expr",
      "x + y",
      ColumnType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn CONSTANT_LIKE = new ExpressionVirtualColumn(
      "expr",
      "like('foo', 'f%')",
      ColumnType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn CONSTANT_NULL_ARITHMETIC = new ExpressionVirtualColumn(
      "expr",
      "2.1 + null",
      ColumnType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_LIKE = new ExpressionVirtualColumn(
      "expr",
      "like(z, 'f%')",
      ColumnType.FLOAT,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_CONCAT_X = new ExpressionVirtualColumn(
      "expr",
      "z + cast(x, 'string')",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn Z_CONCAT_NONEXISTENT = new ExpressionVirtualColumn(
      "expr",
      "concat(z, nonexistent)",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn TIME_FLOOR = new ExpressionVirtualColumn(
      "expr",
      "timestamp_floor(__time, 'P1D')",
      ColumnType.LONG,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn SCALE_LONG = new ExpressionVirtualColumn(
      "expr",
      "x * 2",
      ColumnType.LONG,
      TestExprMacroTable.INSTANCE
  );
  private static final ExpressionVirtualColumn SCALE_FLOAT = new ExpressionVirtualColumn(
      "expr",
      "x * 2",
      ColumnType.FLOAT,
      TestExprMacroTable.INSTANCE
  );

  private static final ExpressionVirtualColumn SCALE_LIST_IMPLICIT = new ExpressionVirtualColumn(
      "expr",
      "b * 2",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );

  private static final ExpressionVirtualColumn SCALE_LIST_EXPLICIT = new ExpressionVirtualColumn(
      "expr",
      "map(b -> b * 2, b)",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );

  private static final ExpressionVirtualColumn SCALE_LIST_SELF_IMPLICIT = new ExpressionVirtualColumn(
      "expr",
      "b * b",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );

  private static final ExpressionVirtualColumn SCALE_LIST_SELF_EXPLICIT = new ExpressionVirtualColumn(
      "expr",
      "map(b -> b * b, b)",
      ColumnType.STRING,
      TestExprMacroTable.INSTANCE
  );

  private static final ThreadLocal<Row> CURRENT_ROW = new ThreadLocal<>();
  private static final ColumnSelectorFactory COLUMN_SELECTOR_FACTORY = RowBasedColumnSelectorFactory.create(
      RowAdapters.standardRow(),
      CURRENT_ROW::get,
      RowSignature.empty(),
      false,
      false
  );

  @Test
  public void testObjectSelector()
  {
    final BaseObjectColumnValueSelector selector = X_PLUS_Y.makeColumnValueSelector("expr", COLUMN_SELECTOR_FACTORY);

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(null, selector.getObject());

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(4L, selector.getObject());
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
  public void testMultiObjectSelector()
  {
    DimensionSpec spec = new DefaultDimensionSpec("expr", "expr");

    final BaseObjectColumnValueSelector selectorImplicit = SCALE_LIST_IMPLICIT.makeDimensionSelector(
        spec,
        COLUMN_SELECTOR_FACTORY
    );
    CURRENT_ROW.set(ROWMULTI);
    Assert.assertEquals(ImmutableList.of("2.0", "4.0", "6.0"), selectorImplicit.getObject());
    CURRENT_ROW.set(ROWMULTI2);
    Assert.assertEquals(ImmutableList.of("6.0", "8.0", "10.0"), selectorImplicit.getObject());
    CURRENT_ROW.set(ROWMULTI3);
    Assert.assertEquals(
        Arrays.asList("6.0", NullHandling.replaceWithDefault() ? "0.0" : null, "10.0"),
        selectorImplicit.getObject()
    );

    final BaseObjectColumnValueSelector selectorExplicit = SCALE_LIST_EXPLICIT.makeDimensionSelector(
        spec,
        COLUMN_SELECTOR_FACTORY
    );
    CURRENT_ROW.set(ROWMULTI);
    Assert.assertEquals(ImmutableList.of("2.0", "4.0", "6.0"), selectorExplicit.getObject());
    CURRENT_ROW.set(ROWMULTI2);
    Assert.assertEquals(ImmutableList.of("6.0", "8.0", "10.0"), selectorExplicit.getObject());
    CURRENT_ROW.set(ROWMULTI3);
    Assert.assertEquals(
        Arrays.asList("6.0", NullHandling.replaceWithDefault() ? "0.0" : null, "10.0"),
        selectorExplicit.getObject()
    );
  }

  @Test
  public void testMultiObjectSelectorMakesRightSelector()
  {
    DimensionSpec spec = new DefaultDimensionSpec("expr", "expr");

    // do some ugly faking to test if SingleStringInputDeferredEvaluationExpressionDimensionSelector is created for multi-value expressions when possible
    ColumnSelectorFactory factory = new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        DimensionSelector delegate = COLUMN_SELECTOR_FACTORY.makeDimensionSelector(dimensionSpec);
        DimensionSelector faker = new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            return delegate.getRow();
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            return delegate.makeValueMatcher(value);
          }

          @Override
          public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
          {
            return delegate.makeValueMatcher(predicateFactory);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            delegate.inspectRuntimeShape(inspector);
          }

          @Nullable
          @Override
          public Object getObject()
          {
            return delegate.getObject();
          }

          @Override
          public Class<?> classOfObject()
          {
            return delegate.classOfObject();
          }

          @Override
          public int getValueCardinality()
          {
            // value doesn't matter as long as not CARDINALITY_UNKNOWN
            return 3;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            return null;
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            // fake this so when SingleStringInputDeferredEvaluationExpressionDimensionSelector it doesn't explode
            return true;
          }

          @Nullable
          @Override
          public IdLookup idLookup()
          {
            return name -> 0;
          }
        };
        return faker;
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        return COLUMN_SELECTOR_FACTORY.makeColumnValueSelector(columnName);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                           .setHasMultipleValues(true)
                                           .setDictionaryEncoded(true);
      }
    };
    final BaseObjectColumnValueSelector selectorImplicit =
        SCALE_LIST_SELF_IMPLICIT.makeDimensionSelector(spec, factory);
    final BaseObjectColumnValueSelector selectorExplicit =
        SCALE_LIST_SELF_EXPLICIT.makeDimensionSelector(spec, factory);

    Assert.assertTrue(selectorImplicit instanceof SingleStringInputDeferredEvaluationExpressionDimensionSelector);
    Assert.assertTrue(selectorExplicit instanceof ExpressionMultiValueDimensionSelector);
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
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(
        StringPredicateDruidPredicateFactory.of(
            value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.TRUE
        )
    );

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches(false));
    Assert.assertEquals(false, fiveMatcher.matches(false));
    Assert.assertEquals(false, nonNullMatcher.matches(false));
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(false, nullMatcher.matches(false));
      Assert.assertEquals(false, fiveMatcher.matches(false));
      Assert.assertEquals(true, nonNullMatcher.matches(false));
      Assert.assertEquals("4", selector.lookupName(selector.getRow().get(0)));
    } else {
      // y is null in row1
      Assert.assertEquals(true, nullMatcher.matches(false));
      Assert.assertEquals(false, fiveMatcher.matches(false));
      Assert.assertEquals(false, nonNullMatcher.matches(false));
      Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(false, nullMatcher.matches(false));
    Assert.assertEquals(false, fiveMatcher.matches(false));
    Assert.assertEquals(true, nonNullMatcher.matches(false));
    Assert.assertEquals("5.1", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(false, nullMatcher.matches(false));
    Assert.assertEquals(true, fiveMatcher.matches(false));
    Assert.assertEquals(true, nonNullMatcher.matches(false));
    Assert.assertEquals("5", selector.lookupName(selector.getRow().get(0)));
  }

  @Test
  public void testNullDimensionSelector()
  {
    final DimensionSelector selector = X_PLUS_Y.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "expr"),
        COLUMN_SELECTOR_FACTORY
    );

    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(
        StringPredicateDruidPredicateFactory.of(
            value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.TRUE
        )
    );

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(false, nonNullMatcher.matches(false));


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
    final ValueMatcher nonNullMatcher = selector.makeValueMatcher(
        StringPredicateDruidPredicateFactory.of(
            value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.TRUE
        )
    );

    CURRENT_ROW.set(ROW0);
    Assert.assertEquals(true, nullMatcher.matches(false));
    Assert.assertEquals(false, fiveMatcher.matches(false));
    Assert.assertEquals(false, nonNullMatcher.matches(false));
    Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW1);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(false, nullMatcher.matches(false));
      Assert.assertEquals(false, fiveMatcher.matches(false));
      Assert.assertEquals(true, nonNullMatcher.matches(false));
      Assert.assertEquals("4", selector.lookupName(selector.getRow().get(0)));
    } else {
      // y is null in row1
      Assert.assertEquals(true, nullMatcher.matches(false));
      Assert.assertEquals(false, fiveMatcher.matches(false));
      Assert.assertEquals(false, nonNullMatcher.matches(false));
      Assert.assertEquals(null, selector.lookupName(selector.getRow().get(0)));
    }

    CURRENT_ROW.set(ROW2);
    Assert.assertEquals(false, nullMatcher.matches(false));
    Assert.assertEquals(true, fiveMatcher.matches(false));
    Assert.assertEquals(true, nonNullMatcher.matches(false));
    Assert.assertEquals("5.1", selector.lookupName(selector.getRow().get(0)));

    CURRENT_ROW.set(ROW3);
    Assert.assertEquals(false, nullMatcher.matches(false));
    Assert.assertEquals(true, fiveMatcher.matches(false));
    Assert.assertEquals(true, nonNullMatcher.matches(false));
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
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getFloat(), 0.0f);
    Assert.assertEquals(DateTimes.of("2000-01-01").getMillis(), selector.getDouble(), 0.0d);
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
    Assert.assertEquals(ImmutableList.of("x", "z"), Z_CONCAT_X.requiredColumns());
  }

  @Test
  public void testExprEvalSelectorWithLongsAndNulls()
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(
        RowBasedColumnSelectorFactory.create(
            RowAdapters.standardRow(),
            CURRENT_ROW::get,
            RowSignature.builder().add("x", ColumnType.LONG).build(),
            false,
            false
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
            RowAdapters.standardRow(),
            CURRENT_ROW::get,
            RowSignature.builder().add("x", ColumnType.DOUBLE).build(),
            false,
            false
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
            RowAdapters.standardRow(),
            CURRENT_ROW::get,
            RowSignature.builder().add("x", ColumnType.FLOAT).build(),
            false,
            false
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

  @Test
  public void testCapabilities()
  {
    ColumnCapabilities caps = X_PLUS_Y.capabilities("expr");
    Assert.assertEquals(ValueType.FLOAT, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    Assert.assertTrue(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());

    caps = Z_CONCAT_X.capabilities("expr");
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    Assert.assertTrue(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testConstantDimensionSelectors()
  {
    ExpressionVirtualColumn constant = new ExpressionVirtualColumn(
        "constant",
        Parser.parse("1 + 2", TestExprMacroTable.INSTANCE),
        ColumnType.LONG
    );
    DimensionSelector constantSelector = constant.makeDimensionSelector(
        DefaultDimensionSpec.of("constant"),
        COLUMN_SELECTOR_FACTORY
    );
    Assert.assertTrue(constantSelector instanceof ConstantDimensionSelector);
    Assert.assertEquals("3", constantSelector.getObject());


    ExpressionVirtualColumn multiConstant = new ExpressionVirtualColumn(
        "multi",
        Parser.parse("string_to_array('a,b,c', ',')", TestExprMacroTable.INSTANCE),
        ColumnType.STRING
    );

    DimensionSelector multiConstantSelector = multiConstant.makeDimensionSelector(
        DefaultDimensionSpec.of("multiConstant"),
        COLUMN_SELECTOR_FACTORY
    );

    Assert.assertTrue(multiConstantSelector instanceof ConstantMultiValueDimensionSelector);
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), multiConstantSelector.getObject());
  }
}
