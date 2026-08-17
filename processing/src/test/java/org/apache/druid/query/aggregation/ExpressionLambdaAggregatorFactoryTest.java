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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.selector.TestColumnValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ExpressionLambdaAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private static ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        "customAccumulator",
        "0.0",
        "10.0",
        true,
        true,
        false,
        "customAccumulator + some_column + some_other_column",
        "customAccumulator + expr_agg_name",
        "if (o1 > o2, if (o1 == o2, 0, 1), -1)",
        "o + 100",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(agg, MAPPER.readValue(MAPPER.writeValueAsBytes(agg), ExpressionLambdaAggregatorFactory.class));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ExpressionLambdaAggregatorFactory.class)
                  .usingGetClass()
                  .withIgnoredFields(
                      "macroTable",
                      "initialValue",
                      "initialCombineValue",
                      "foldExpression",
                      "combineExpression",
                      "compareExpression",
                      "finalizeExpression",
                      "compareBindings",
                      "combineBindings",
                      "finalizeBindings",
                      "finalizeInspector"
                  )
                  .verify();
  }

  @Test
  public void testInitialValueMustBeConstant()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("initial value must be constant");

    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "x + y",
        null,
        true,
        false,
        false,
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    agg.getIntermediateType();
  }

  @Test
  public void testInitialCombineValueMustBeConstant()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("initial combining value must be constant");

    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        "x + y",
        true,
        false,
        false,
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    agg.getResultType();
  }

  @Test
  public void testSingleInputCombineExpressionIsOptional()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        false,
        false,
        "__acc + x",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(1L, agg.combine(0L, 1L));
  }

  @Test
  public void testCombineExpressionIgnoresNullsIfCombineSkipsNulls()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "ARRAY<STRING>",
        "ARRAY<STRING>[]",
        true,
        false,
        false,
        "array_append(__acc, x)",
        "array_concat(__acc, expr_agg_name)",
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertArrayEquals(new Object[]{"hello"}, (Object[]) agg.combine(null, new Object[]{"hello"}));
    Assert.assertArrayEquals(
        new Object[]{"hello", "world"},
        (Object[]) agg.combine(new Object[]{"hello"}, new Object[]{"world"})
    );
  }

  @Test
  public void testCombineExpressionDoesntIgnoreNullsIfCombineDoesntSkipsNulls()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "ARRAY<STRING>",
        "ARRAY<STRING>[]",
        true,
        false,
        true,
        "array_append(__acc, x)",
        "array_concat(__acc, expr_agg_name)",
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertNull(agg.combine(null, new Object[]{"hello"}));
    Assert.assertArrayEquals(
        new Object[]{"hello", "world"},
        (Object[]) agg.combine(new Object[]{"hello"}, new Object[]{"world"})
    );
  }

  @Test
  public void testFinalizeCanDo()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        false,
        false,
        "__acc + x",
        null,
        null,
        "o + 100",
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(100L, agg.finalizeComputation(0L));
  }

  @Test
  public void testFinalizeCanDoArrays()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        "ARRAY<STRING>[]",
        true,
        true,
        false,
        "array_set_add(__acc, x)",
        "array_set_add_all(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ',')",
        null,
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals("a,b,c", agg.finalizeComputation(new String[]{"a", "b", "c"}));
    Assert.assertEquals("a,b,c", agg.finalizeComputation(ImmutableList.of("a", "b", "c")));
  }

  @Test
  public void testStringType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "''",
        "''",
        true,
        true,
        true,
        "concat(__acc, some_column, some_other_column)",
        "concat(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.STRING, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.STRING, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.STRING, agg.getResultType());
  }

  @Test
  public void testLongType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0",
        null,
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.LONG, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.LONG, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.LONG, agg.getResultType());
  }

  @Test
  public void testDoubleType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        null,
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.DOUBLE, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE, agg.getResultType());
  }

  @Test
  public void testStringArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "''",
        "ARRAY<STRING>[]",
        null,
        false,
        false,
        "concat(__acc, some_column, some_other_column)",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.STRING, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.STRING_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.STRING_ARRAY, agg.getResultType());
  }

  @Test
  public void testStringArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "''",
        "ARRAY<STRING>[]",
        null,
        false,
        false,
        "concat(__acc, some_column, some_other_column)",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.STRING, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.STRING_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.STRING, agg.getResultType());
  }

  @Test
  public void testLongArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0",
        "ARRAY<LONG>[]",
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.LONG, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.LONG_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.LONG_ARRAY, agg.getResultType());
  }

  @Test
  public void testLongArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0",
        "ARRAY<LONG>[]",
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.LONG, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.LONG_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.STRING, agg.getResultType());
  }

  @Test
  public void testDoubleArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        "ARRAY<DOUBLE>[]",
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.DOUBLE, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, agg.getResultType());
  }

  @Test
  public void testDoubleArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        "ARRAY<DOUBLE>[]",
        null,
        false,
        false,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.DOUBLE, agg.getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.STRING, agg.getResultType());
  }

  @Test
  public void testComplexType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column"),
        null,
        "hyper_unique()",
        null,
        null,
        false,
        false,
        "hyper_unique_add(some_column, __acc)",
        "hyper_unique_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, agg.getIntermediateType());
    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, agg.getResultType());
  }

  @Test
  public void testComplexTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column"),
        null,
        "hyper_unique()",
        null,
        null,
        false,
        false,
        "hyper_unique_add(some_column, __acc)",
        "hyper_unique_add(__acc, expr_agg_name)",
        null,
        "hyper_unique_estimate(o)",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, agg.getIntermediateType());
    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, agg.getCombiningFactory().getIntermediateType());
    Assert.assertEquals(ColumnType.DOUBLE, agg.getResultType());
  }

  @Test
  public void testLongAggregateCombiner()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        false,
        false,
        "__acc + x",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    AggregateCombiner combiner = agg.makeAggregateCombiner();
    TestColumnValueSelector<Long> selector = TestColumnValueSelector.of(
        Long.class,
        Arrays.asList(1L, 2L, 3L)
    );
    selector.advance();
    combiner.reset(selector);
    Assert.assertEquals(1L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(3L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(6L, combiner.getLong());
  }

  @Test
  public void testDoubleAggregateCombiner()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0.0",
        null,
        true,
        false,
        false,
        "__acc + x",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    AggregateCombiner combiner = agg.makeAggregateCombiner();
    TestColumnValueSelector<Double> selector = TestColumnValueSelector.of(
        Double.class,
        Arrays.asList(1.5, 2.25, 0.25)
    );
    selector.advance();
    combiner.reset(selector);
    Assert.assertEquals(1.5, combiner.getDouble(), 0.0);

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(3.75, combiner.getDouble(), 0.0);

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(4.0, combiner.getDouble(), 0.0);
  }

  @Test
  public void testNullableAggregateCombinerSkipsNulls()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        false,
        false,
        "__acc + x",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    AggregateCombiner combiner = agg.makeNullableAggregateCombiner();
    NullableLongSelector selector = new NullableLongSelector(Arrays.asList(null, 5L, null, 7L));
    selector.advance();
    combiner.reset(selector);
    Assert.assertTrue(combiner.isNull());

    selector.advance();
    combiner.fold(selector);
    Assert.assertFalse(combiner.isNull());
    Assert.assertEquals(5L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(5L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(12L, combiner.getLong());
  }

  @Test
  public void testNullableAggregateCombinerWhenCombineAggregatesNullsExpressionSeesNulls()
  {
    // shouldCombineAggregateNullInputs=true means the combine expression sees null inputs directly. The expression
    // itself is responsible for handling them; here `nvl` coalesces nulls to 0 so the accumulator keeps advancing.
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        true,
        true,
        "nvl(__acc, 0) + nvl(x, 0)",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    AggregateCombiner combiner = agg.makeNullableAggregateCombiner();
    NullableLongSelector selector = new NullableLongSelector(Arrays.asList(1L, null, 3L));
    selector.advance();
    combiner.reset(selector);
    Assert.assertEquals(1L, combiner.getLong());

    // null is passed through to the expression, which coalesces to 0
    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(1L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertEquals(4L, combiner.getLong());
  }

  @Test
  public void testNullableAggregateCombinerNullExpressionResultPropagates()
  {
    // shouldCombineAggregateNullInputs=true with an expression that doesn't handle nulls: `__acc + null` evaluates
    // to null in Druid expression semantics, and the combiner reports isNull accordingly.
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
        true,
        true,
        "__acc + x",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    AggregateCombiner combiner = agg.makeNullableAggregateCombiner();
    NullableLongSelector selector = new NullableLongSelector(Arrays.asList(1L, null));
    selector.advance();
    combiner.reset(selector);
    Assert.assertFalse(combiner.isNull());
    Assert.assertEquals(1L, combiner.getLong());

    selector.advance();
    combiner.fold(selector);
    Assert.assertTrue(combiner.isNull());
  }


  private static final class NullableLongSelector implements ColumnValueSelector<Long>
  {
    private final List<Long> values;
    private int index = -1;

    NullableLongSelector(List<Long> values)
    {
      this.values = values;
    }

    void advance()
    {
      index++;
    }

    @Override
    public long getLong()
    {
      Long v = values.get(index);
      return v == null ? 0L : v;
    }

    @Override
    public double getDouble()
    {
      return getLong();
    }

    @Override
    public float getFloat()
    {
      return getLong();
    }

    @Override
    public boolean isNull()
    {
      return values.get(index) == null;
    }

    @Nullable
    @Override
    public Long getObject()
    {
      return values.get(index);
    }

    @Override
    public Class<Long> classOfObject()
    {
      return Long.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }
  }

  @Test(expected = UOE.class)
  public void testAggregateCombinerNotSupportedForNonNumericTypes()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "''",
        "''",
        true,
        true,
        true,
        "concat(__acc, x)",
        "concat(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    agg.makeAggregateCombiner();
  }

  @Test(expected = UOE.class)
  public void testAggregateCombinerNotSupportedWhenFoldAndCombineTypesDiffer()
  {
    // fold seed is LONG (intermediate column type), but combine seed is LONG_ARRAY — combining a long segment column
    // with an expression that expects arrays would silently produce wrong values, so the combiner refuses to handle it.
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        "ARRAY<LONG>[]",
        null,
        false,
        false,
        "__acc + x",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ColumnType.LONG, agg.getIntermediateType());
    agg.makeAggregateCombiner();
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new ExpressionLambdaAggregatorFactory(
                      "string_expr",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "''",
                      "''",
                      null,
                      false,
                      false,
                      "concat(__acc, some_column, some_other_column)",
                      "concat(__acc, string_expr)",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "double_expr",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "0.0",
                      null,
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "__acc + double_expr",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "long_expr",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "0",
                      null,
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "__acc + long_expr",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "string_array_expr",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "ARRAY<STRING>[]",
                      "ARRAY<STRING>[]",
                      null,
                      true,
                      false,
                      "array_set_add(__acc, concat(some_column, some_other_column))",
                      "array_set_add_all(__acc, string_array_expr)",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "double_array_expr",
                      ImmutableSet.of("some_column", "some_other_column_expr"),
                      null,
                      "0.0",
                      "ARRAY<DOUBLE>[]",
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "array_set_add(__acc, double_array)",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "long_array_expr",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "0",
                      "ARRAY<LONG>[]",
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "array_set_add(__acc, long_array_expr)",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "string_array_expr_finalized",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "''",
                      "ARRAY<STRING>[]",
                      null,
                      false,
                      false,
                      "concat(__acc, some_column, some_other_column)",
                      "array_set_add(__acc, string_array_expr)",
                      null,
                      "array_to_string(o, ';')",
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "double_array_expr_finalized",
                      ImmutableSet.of("some_column", "some_other_column_expr"),
                      null,
                      "0.0",
                      "ARRAY<DOUBLE>[]",
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "array_set_add(__acc, double_array)",
                      null,
                      "array_to_string(o, ';')",
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "long_array_expr_finalized",
                      ImmutableSet.of("some_column", "some_other_column"),
                      null,
                      "0",
                      "ARRAY<LONG>[]",
                      null,
                      false,
                      false,
                      "__acc + some_column + some_other_column",
                      "array_set_add(__acc, long_array_expr)",
                      null,
                      "fold((x, acc) -> x + acc, o, 0)",
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "complex_expr",
                      ImmutableSet.of("some_column"),
                      null,
                      "hyper_unique()",
                      null,
                      null,
                      false,
                      false,
                      "hyper_unique_add(some_column, __acc)",
                      "hyper_unique_add(__acc, expr_agg_name)",
                      null,
                      null,
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionLambdaAggregatorFactory(
                      "complex_expr_finalized",
                      ImmutableSet.of("some_column"),
                      null,
                      "hyper_unique()",
                      null,
                      null,
                      false,
                      false,
                      "hyper_unique_add(some_column, __acc)",
                      "hyper_unique_add(__acc, expr_agg_name)",
                      null,
                      "hyper_unique_estimate(o)",
                      new HumanReadableBytes(2048),
                      TestExprMacroTable.INSTANCE
                  )
              )
              .postAggregators(
                  new FieldAccessPostAggregator("string-array-expr-access", "string_array_expr_finalized"),
                  new FinalizingFieldAccessPostAggregator("string-array-expr-finalize", "string_array_expr_finalized"),
                  new FieldAccessPostAggregator("double-array-expr-access", "double_array_expr_finalized"),
                  new FinalizingFieldAccessPostAggregator("double-array-expr-finalize", "double_array_expr_finalized"),
                  new FieldAccessPostAggregator("long-array-expr-access", "long_array_expr_finalized"),
                  new FinalizingFieldAccessPostAggregator("long-array-expr-finalize", "long_array_expr_finalized"),
                  new FieldAccessPostAggregator("complex-expr-access", "complex_expr_finalized"),
                  new FinalizingFieldAccessPostAggregator("complex-expr-finalize", "complex_expr_finalized")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("string_expr", ColumnType.STRING)
                    .add("double_expr", ColumnType.DOUBLE)
                    .add("long_expr", ColumnType.LONG)
                    .add("string_array_expr", ColumnType.STRING_ARRAY)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("double_array_expr", null)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("long_array_expr", null)
                    // string because fold type equals finalized type, even though merge type is array
                    .add("string_array_expr_finalized", ColumnType.STRING)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("double_array_expr_finalized", null)
                    // long because fold type equals finalized type, even though merge type is array
                    .add("long_array_expr_finalized", ColumnType.LONG)
                    .add("complex_expr", HyperUniquesAggregatorFactory.TYPE)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("complex_expr_finalized", null)
                    // fold type is string
                    .add("string-array-expr-access", ColumnType.STRING)
                    // finalized type is string
                    .add("string-array-expr-finalize", ColumnType.STRING)
                    // double because fold type is double
                    .add("double-array-expr-access", ColumnType.DOUBLE)
                    // string because finalize type is string
                    .add("double-array-expr-finalize", ColumnType.STRING)
                    // long because fold type is long
                    .add("long-array-expr-access", ColumnType.LONG)
                    // finalized type is long
                    .add("long-array-expr-finalize", ColumnType.LONG)
                    .add("complex-expr-access", HyperUniquesAggregatorFactory.TYPE)
                    .add("complex-expr-finalize", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
