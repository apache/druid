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

import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AggregatorUtilTest
{

  @Test
  public void testPruneDependentPostAgg()
  {
    PostAggregator agg1 = new ArithmeticPostAggregator(
        "abc", "+", Lists.newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("2", 2L)
    )
    );
    PostAggregator dependency1 = new ArithmeticPostAggregator(
        "dep1", "+", Lists.newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("4", 4L)
    )
    );
    PostAggregator agg2 = new FieldAccessPostAggregator("def", "def");
    PostAggregator dependency2 = new FieldAccessPostAggregator("dep2", "dep2");
    PostAggregator aggregator = new ArithmeticPostAggregator(
        "finalAgg",
        "+",
        Lists.newArrayList(
            new FieldAccessPostAggregator("dep1", "dep1"),
            new FieldAccessPostAggregator("dep2", "dep2")
        )
    );
    List<PostAggregator> prunedAgg = AggregatorUtil.pruneDependentPostAgg(
        Lists.newArrayList(
            agg1,
            dependency1,
            agg2,
            dependency2,
            aggregator
        ), aggregator.getName()
    );
    Assert.assertEquals(Lists.newArrayList(dependency1, dependency2, aggregator), prunedAgg);
  }

  @Test
  public void testOutOfOrderPruneDependentPostAgg()
  {
    PostAggregator agg1 = new ArithmeticPostAggregator(
        "abc", "+", Lists.newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("2", 2L)
    )
    );
    PostAggregator dependency1 = new ArithmeticPostAggregator(
        "dep1", "+", Lists.newArrayList(
        new ConstantPostAggregator("1", 1L), new ConstantPostAggregator("4", 4L)
    )
    );
    PostAggregator agg2 = new FieldAccessPostAggregator("def", "def");
    PostAggregator dependency2 = new FieldAccessPostAggregator("dep2", "dep2");
    PostAggregator aggregator = new ArithmeticPostAggregator(
        "finalAgg",
        "+",
        Lists.newArrayList(
            new FieldAccessPostAggregator("dep1", "dep1"),
            new FieldAccessPostAggregator("dep2", "dep2")
        )
    );
    List<PostAggregator> prunedAgg = AggregatorUtil.pruneDependentPostAgg(
        Lists.newArrayList(
            agg1,
            dependency1,
            aggregator, // dependency is added later than the aggregator
            agg2,
            dependency2
        ), aggregator.getName()
    );
    Assert.assertEquals(Lists.newArrayList(dependency1, aggregator), prunedAgg);
  }

  @Test
  public void testCondenseAggregators()
  {

    ArrayList<AggregatorFactory> aggregatorFactories = Lists.newArrayList(
        Iterables.concat(
            QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS,
            Lists.newArrayList(
                new DoubleMaxAggregatorFactory("maxIndex", "index"),
                new DoubleMinAggregatorFactory("minIndex", "index")
            )
        )
    );

    List<PostAggregator> postAggregatorList = Arrays.asList(
        QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
        QueryRunnerTestHelper.DEPENDENT_POST_AGG
    );
    Pair<List<AggregatorFactory>, List<PostAggregator>> aggregatorsPair = AggregatorUtil.condensedAggregators(
        aggregatorFactories,
        postAggregatorList,
        QueryRunnerTestHelper.dependentPostAggMetric
    );
    // verify aggregators
    Assert.assertEquals(
        Lists.newArrayList(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.INDEX_DOUBLE_SUM),
        aggregatorsPair.lhs
    );
    Assert.assertEquals(
        Lists.newArrayList(
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
            QueryRunnerTestHelper.DEPENDENT_POST_AGG
        ), aggregatorsPair.rhs
    );

  }

  @Test
  public void testNullPostAggregatorNames()
  {
    AggregatorFactory agg1 = new DoubleSumAggregatorFactory("agg1", "value");
    AggregatorFactory agg2 = new DoubleSumAggregatorFactory("agg2", "count");
    PostAggregator postAgg1 = new ArithmeticPostAggregator(
        null,
        "*",
        Lists.newArrayList(new FieldAccessPostAggregator(null, "agg1"), new FieldAccessPostAggregator(null, "agg2"))
    );

    PostAggregator postAgg2 = new ArithmeticPostAggregator(
        "postAgg",
        "/",
        Lists.newArrayList(new FieldAccessPostAggregator(null, "agg1"), new FieldAccessPostAggregator(null, "agg2"))
    );

    Assert.assertEquals(
        new Pair<>(Lists.newArrayList(agg1, agg2), Collections.singletonList(postAgg2)),
        AggregatorUtil.condensedAggregators(
            Lists.newArrayList(agg1, agg2),
            Lists.newArrayList(postAgg1, postAgg2),
            "postAgg"
        )
    );

  }

  @Test
  public void testCasing()
  {
    AggregatorFactory agg1 = new DoubleSumAggregatorFactory("Agg1", "value");
    AggregatorFactory agg2 = new DoubleSumAggregatorFactory("Agg2", "count");
    PostAggregator postAgg1 = new ArithmeticPostAggregator(
        null,
        "*",
        Lists.newArrayList(new FieldAccessPostAggregator(null, "Agg1"), new FieldAccessPostAggregator(null, "Agg2"))
    );

    PostAggregator postAgg2 = new ArithmeticPostAggregator(
        "postAgg",
        "/",
        Lists.newArrayList(new FieldAccessPostAggregator(null, "Agg1"), new FieldAccessPostAggregator(null, "Agg2"))
    );

    Assert.assertEquals(
        new Pair<>(Lists.newArrayList(agg1, agg2), Collections.singletonList(postAgg2)),
        AggregatorUtil.condensedAggregators(
            Lists.newArrayList(agg1, agg2),
            Lists.newArrayList(postAgg1, postAgg2),
            "postAgg"
        )
    );
  }

  @Test
  public void testCanVectorizeFieldNameWithNumericCapabilities()
  {
    ColumnInspector inspector = makeInspector(
        Map.of("col", ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG))
    );
    Assert.assertTrue(AggregatorUtil.canVectorize(inspector, "col", null, Suppliers.ofInstance(null)));
  }

  @Test
  public void testCanVectorizeFieldNameWithNullCapabilities()
  {
    // null capabilities (unknown column) is treated as vectorizable
    ColumnInspector inspector = makeInspector(Map.of());
    Assert.assertTrue(AggregatorUtil.canVectorize(inspector, "unknown_col", null, Suppliers.ofInstance(null)));
  }

  @Test
  public void testCanVectorizeFieldNameWithStringCapabilities()
  {
    ColumnInspector inspector = makeInspector(
        Map.of("col", ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities())
    );
    Assert.assertFalse(AggregatorUtil.canVectorize(inspector, "col", null, Suppliers.ofInstance(null)));
  }

  @Test
  public void testCanVectorizeExpressionOnNumericColumns()
  {
    ColumnInspector inspector = makeInspector(
        Map.of(
            "long1", ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG),
            "long2", ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
        )
    );
    String expression = "long1 + long2";
    Expr expr = Parser.parse(expression, TestExprMacroTable.INSTANCE);
    Assert.assertTrue(AggregatorUtil.canVectorize(inspector, null, expression, Suppliers.ofInstance(expr)));
  }

  @Test
  public void testCanVectorizeExpressionWithIncompleteInputs()
  {
    // string column with unknown multi-valuedness results in INCOMPLETE_INPUTS which blocks vectorization,
    // even though the expression itself supports vectorization
    ColumnInspector inspector = makeInspector(
        Map.of(
            "string_unknown", new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
        )
    );
    String expression = "concat(string_unknown, 'x')";
    Expr expr = Parser.parse(expression, TestExprMacroTable.INSTANCE);
    Assert.assertFalse(AggregatorUtil.canVectorize(inspector, null, expression, Suppliers.ofInstance(expr)));
  }

  @Test
  public void testCanVectorizeExpressionWithNeedsApplied()
  {
    // multi-valued string column used in scalar context results in NEEDS_APPLIED which blocks vectorization
    ColumnInspector inspector = makeInspector(
        Map.of(
            "multi_string", new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                        .setDictionaryEncoded(true)
                                                        .setHasBitmapIndexes(true)
                                                        .setDictionaryValuesUnique(true)
                                                        .setDictionaryValuesSorted(true)
                                                        .setHasMultipleValues(true)
        )
    );
    String expression = "concat(multi_string, 'x')";
    Expr expr = Parser.parse(expression, TestExprMacroTable.INSTANCE);
    Assert.assertFalse(AggregatorUtil.canVectorize(inspector, null, expression, Suppliers.ofInstance(expr)));
  }

  @Test
  public void testCanVectorizeNeitherFieldNameNorExpression()
  {
    ColumnInspector inspector = makeInspector(Map.of());
    Assert.assertFalse(AggregatorUtil.canVectorize(inspector, null, null, Suppliers.ofInstance(null)));
  }

  private static ColumnInspector makeInspector(Map<String, ColumnCapabilities> capabilitiesMap)
  {
    return capabilitiesMap::get;
  }
}