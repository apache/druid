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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

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
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    agg.getType();
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
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    agg.getFinalizedType();
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
  public void testFinalizeCanDo()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("x"),
        null,
        "0",
        null,
        true,
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
        null,
        true,
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
        "concat(__acc, some_column, some_other_column)",
        "concat(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.STRING, agg.getType());
    Assert.assertEquals(ValueType.STRING, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.STRING, agg.getFinalizedType());
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
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.LONG, agg.getType());
    Assert.assertEquals(ValueType.LONG, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.LONG, agg.getFinalizedType());
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
        "__acc + some_column + some_other_column",
        "__acc + expr_agg_name",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.DOUBLE, agg.getType());
    Assert.assertEquals(ValueType.DOUBLE, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.DOUBLE, agg.getFinalizedType());
  }

  @Test
  public void testStringArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "''",
        "<STRING>[]",
        null,
        "concat(__acc, some_column, some_other_column)",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.STRING, agg.getType());
    Assert.assertEquals(ValueType.STRING_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.STRING_ARRAY, agg.getFinalizedType());
  }

  @Test
  public void testStringArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "''",
        "<STRING>[]",
        null,
        "concat(__acc, some_column, some_other_column)",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.STRING, agg.getType());
    Assert.assertEquals(ValueType.STRING_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.STRING, agg.getFinalizedType());
  }

  @Test
  public void testLongArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0",
        "<LONG>[]",
        null,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.LONG, agg.getType());
    Assert.assertEquals(ValueType.LONG_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.LONG_ARRAY, agg.getFinalizedType());
  }

  @Test
  public void testLongArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0",
        "<LONG>[]",
        null,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.LONG, agg.getType());
    Assert.assertEquals(ValueType.LONG_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.STRING, agg.getFinalizedType());
  }

  @Test
  public void testDoubleArrayType()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        "<DOUBLE>[]",
        null,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        null,
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.DOUBLE, agg.getType());
    Assert.assertEquals(ValueType.DOUBLE_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.DOUBLE_ARRAY, agg.getFinalizedType());
  }

  @Test
  public void testDoubleArrayTypeFinalized()
  {
    ExpressionLambdaAggregatorFactory agg = new ExpressionLambdaAggregatorFactory(
        "expr_agg_name",
        ImmutableSet.of("some_column", "some_other_column"),
        null,
        "0.0",
        "<DOUBLE>[]",
        null,
        "__acc + some_column + some_other_column",
        "array_set_add(__acc, expr_agg_name)",
        null,
        "array_to_string(o, ';')",
        new HumanReadableBytes(2048),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertEquals(ValueType.DOUBLE, agg.getType());
    Assert.assertEquals(ValueType.DOUBLE_ARRAY, agg.getCombiningFactory().getType());
    Assert.assertEquals(ValueType.STRING, agg.getFinalizedType());
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
                      "<STRING>[]",
                      "<STRING>[]",
                      null,
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
                      "<DOUBLE>[]",
                      null,
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
                      "<LONG>[]",
                      null,
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
                      "<STRING>[]",
                      null,
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
                      "<DOUBLE>[]",
                      null,
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
                      "<LONG>[]",
                      null,
                      "__acc + some_column + some_other_column",
                      "array_set_add(__acc, long_array_expr)",
                      null,
                      "fold((x, acc) -> x + acc, o, 0)",
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
                  new FinalizingFieldAccessPostAggregator("long-array-expr-finalize", "long_array_expr_finalized")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("string_expr", ValueType.STRING)
                    .add("double_expr", ValueType.DOUBLE)
                    .add("long_expr", ValueType.LONG)
                    .add("string_array_expr", ValueType.STRING_ARRAY)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("double_array_expr", null)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("long_array_expr", null)
                    // string because fold type equals finalized type, even though merge type is array
                    .add("string_array_expr_finalized", ValueType.STRING)
                    // type does not equal finalized type. (combining factory type does equal finalized type,
                    // but this signature doesn't use combining factory)
                    .add("double_array_expr_finalized", null)
                    // long because fold type equals finalized type, even though merge type is array
                    .add("long_array_expr_finalized", ValueType.LONG)
                    // fold type is string
                    .add("string-array-expr-access", ValueType.STRING)
                    // finalized type is string
                    .add("string-array-expr-finalize", ValueType.STRING)
                    // double because fold type is double
                    .add("double-array-expr-access", ValueType.DOUBLE)
                    // string because finalize type is string
                    .add("double-array-expr-finalize", ValueType.STRING)
                    // long because fold type is long
                    .add("long-array-expr-access", ValueType.LONG)
                    // finalized type is long
                    .add("long-array-expr-finalize", ValueType.LONG)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
