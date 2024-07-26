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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionSelectors;
import org.apache.druid.segment.virtual.ExpressionVectorSelectors;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@PublicApi
public class AggregatorUtil
{
  public static final byte STRING_SEPARATOR = (byte) 0xFF;
  public static final byte COUNT_CACHE_TYPE_ID = 0x0;
  public static final byte LONG_SUM_CACHE_TYPE_ID = 0x1;
  public static final byte DOUBLE_SUM_CACHE_TYPE_ID = 0x2;
  public static final byte DOUBLE_MAX_CACHE_TYPE_ID = 0x3;
  public static final byte DOUBLE_MIN_CACHE_TYPE_ID = 0x4;
  public static final byte HYPER_UNIQUE_CACHE_TYPE_ID = 0x5;
  public static final byte JS_CACHE_TYPE_ID = 0x6;
  public static final byte HIST_CACHE_TYPE_ID = 0x7;
  public static final byte CARD_CACHE_TYPE_ID = 0x8;
  public static final byte FILTERED_AGG_CACHE_TYPE_ID = 0x9;
  public static final byte LONG_MAX_CACHE_TYPE_ID = 0xA;
  public static final byte LONG_MIN_CACHE_TYPE_ID = 0xB;
  public static final byte FLOAT_SUM_CACHE_TYPE_ID = 0xC;
  public static final byte FLOAT_MAX_CACHE_TYPE_ID = 0xD;
  public static final byte FLOAT_MIN_CACHE_TYPE_ID = 0xE;
  public static final byte SKETCH_MERGE_CACHE_TYPE_ID = 0xF;
  public static final byte DISTINCT_COUNT_CACHE_KEY = 0x10;
  public static final byte FLOAT_LAST_CACHE_TYPE_ID = 0x11;
  public static final byte APPROX_HIST_CACHE_TYPE_ID = 0x12;
  public static final byte APPROX_HIST_FOLDING_CACHE_TYPE_ID = 0x13;
  public static final byte DOUBLE_FIRST_CACHE_TYPE_ID = 0x14;
  public static final byte DOUBLE_LAST_CACHE_TYPE_ID = 0x15;
  public static final byte FLOAT_FIRST_CACHE_TYPE_ID = 0x16;
  public static final byte LONG_FIRST_CACHE_TYPE_ID = 0x17;
  public static final byte LONG_LAST_CACHE_TYPE_ID = 0x18;
  public static final byte TIMESTAMP_CACHE_TYPE_ID = 0x19;
  public static final byte VARIANCE_CACHE_TYPE_ID = 0x1A;

  // Quantiles sketch aggregator
  public static final byte QUANTILES_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID = 0x1B;
  public static final byte QUANTILES_DOUBLES_SKETCH_MERGE_CACHE_TYPE_ID = 0x1C;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_HISTOGRAM_CACHE_TYPE_ID = 0x1D;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_QUANTILE_CACHE_TYPE_ID = 0x1E;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_QUANTILES_CACHE_TYPE_ID = 0x1F;
  public static final byte QUANTILES_DOUBLES_SKETCH_TO_STRING_CACHE_TYPE_ID = 0x20;

  // ArrayOfDoublesSketch aggregator
  public static final byte ARRAY_OF_DOUBLES_SKETCH_CACHE_TYPE_ID = 0x21;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_SET_OP_CACHE_TYPE_ID = 0x22;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_CACHE_TYPE_ID = 0x23;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_AND_BOUNDS_CACHE_TYPE_ID = 0x24;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_MEANS_CACHE_TYPE_ID = 0x25;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_VARIANCES_CACHE_TYPE_ID = 0x26;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_NUM_ENTRIES_CACHE_TYPE_ID = 0x27;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_QUANTILES_SKETCH_CACHE_TYPE_ID = 0x28;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_T_TEST_CACHE_TYPE_ID = 0x29;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_STRING_CACHE_TYPE_ID = 0x2A;

  // StringFirst, StringLast aggregator
  public static final byte STRING_FIRST_CACHE_TYPE_ID = 0x2B;
  public static final byte STRING_LAST_CACHE_TYPE_ID = 0x2C;

  // Suppressed aggregator
  public static final byte SUPPRESSED_AGG_CACHE_TYPE_ID = 0x2D;

  // HllSketch module in datasketches extension
  public static final byte HLL_SKETCH_BUILD_CACHE_TYPE_ID = 0x2E;
  public static final byte HLL_SKETCH_MERGE_CACHE_TYPE_ID = 0x2F;
  public static final byte HLL_SKETCH_UNION_CACHE_TYPE_ID = 0x30;
  public static final byte HLL_SKETCH_TO_STRING_CACHE_TYPE_ID = 0x31;
  public static final byte HLL_SKETCH_TO_ESTIMATE_AND_BOUNDS_CACHE_TYPE_ID = 0x32;

  // Fixed buckets histogram aggregator
  public static final byte FIXED_BUCKET_HIST_CACHE_TYPE_ID = 0x33;

  // bloom filter extension
  public static final byte BLOOM_FILTER_CACHE_TYPE_ID = 0x34;
  public static final byte BLOOM_FILTER_MERGE_CACHE_TYPE_ID = 0x35;

  // Quantiles sketch in momentsketch extension
  public static final byte MOMENTS_SKETCH_BUILD_CACHE_TYPE_ID = 0x36;
  public static final byte MOMENTS_SKETCH_MERGE_CACHE_TYPE_ID = 0x37;

  // TDigest sketch aggregators
  public static final byte TDIGEST_BUILD_SKETCH_CACHE_TYPE_ID = 0x38;

  // Spectator histogram aggregators
  public static final byte SPECTATOR_HISTOGRAM_CACHE_TYPE_ID = 0x39;
  public static final byte SPECTATOR_HISTOGRAM_DISTRIBUTION_CACHE_TYPE_ID = 0x3A;
  public static final byte SPECTATOR_HISTOGRAM_TIMER_CACHE_TYPE_ID = 0x3B;

  public static final byte MEAN_CACHE_TYPE_ID = 0x41;

  // ANY aggregator
  public static final byte LONG_ANY_CACHE_TYPE_ID = 0x42;
  public static final byte DOUBLE_ANY_CACHE_TYPE_ID = 0x43;
  public static final byte FLOAT_ANY_CACHE_TYPE_ID = 0x44;
  public static final byte STRING_ANY_CACHE_TYPE_ID = 0x45;

  // GROUPING aggregator
  public static final byte GROUPING_CACHE_TYPE_ID = 0x46;

  // expression lambda aggregator
  public static final byte EXPRESSION_LAMBDA_CACHE_TYPE_ID = 0x47;

  // KLL sketch aggregator
  public static final byte KLL_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID = 0x48;
  public static final byte KLL_DOUBLES_SKETCH_MERGE_CACHE_TYPE_ID = 0x49;
  public static final byte KLL_FLOATS_SKETCH_BUILD_CACHE_TYPE_ID = 0x4A;
  public static final byte KLL_FLOATS_SKETCH_MERGE_CACHE_TYPE_ID = 0x4B;


  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_BASE64_STRING_CACHE_TYPE_ID = 0x4C;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_CONSTANT_SKETCH_CACHE_TYPE_ID = 0x4D;
  public static final byte ARRAY_OF_DOUBLES_SKETCH_TO_METRICS_SUM_ESTIMATE_CACHE_TYPE_ID = 0x4E;
  public static final byte SINGLE_VALUE_CACHE_TYPE_ID = 0x4F;

  // DDSketch aggregator
  public static final byte DDSKETCH_CACHE_TYPE_ID = 0x50;

  /**
   * Given a list of PostAggregators and the name of an output column, returns the minimal list of PostAggregators
   * required to compute the output column.
   * <p>
   * If the outputColumn does not exist in the list of PostAggregators, the return list will be empty (under the
   * assumption that the outputColumn comes from a project, aggregation or really anything other than a
   * PostAggregator).
   * <p>
   * If the outputColumn <strong>does</strong> exist in the list of PostAggregators, then the return list will have at
   * least one element.  If the PostAggregator with outputName depends on any other PostAggregators, then the returned
   * list will contain all PostAggregators required to compute the outputColumn.
   * <p>
   * Note that PostAggregators are processed in list-order, meaning that for a PostAggregator to depend on another
   * PostAggregator, the "depender" must exist *after* the "dependee" in the list.  That is, if PostAggregator A
   * depends on PostAggregator B, then the list should be [B, A], such that A is computed after B.
   *
   * @param postAggregatorList List of postAggregator, there is a restriction that the list should be in an order such
   *                           that all the dependencies of any given aggregator should occur before that aggregator.
   *                           See AggregatorUtilTest.testOutOfOrderPruneDependentPostAgg for example.
   * @param outputName         name of the postAgg on which dependency is to be calculated
   * @return the list of dependent postAggregators
   */
  public static List<PostAggregator> pruneDependentPostAgg(List<PostAggregator> postAggregatorList, String outputName)
  {
    if (postAggregatorList.isEmpty()) {
      return postAggregatorList;
    }

    ArrayList<PostAggregator> rv = new ArrayList<>();
    Set<String> deps = new HashSet<>();
    deps.add(outputName);
    // Iterate backwards to find the last calculated aggregate and add dependent aggregator as we find dependencies
    // in reverse order
    for (PostAggregator agg : Lists.reverse(postAggregatorList)) {
      if (deps.contains(agg.getName())) {
        rv.add(agg); // add to the beginning of List
        deps.remove(agg.getName());
        deps.addAll(agg.getDependentFields());
      }
    }

    Collections.reverse(rv);
    return rv;
  }

  public static Pair<List<AggregatorFactory>, List<PostAggregator>> condensedAggregators(
      List<AggregatorFactory> aggList,
      List<PostAggregator> postAggList,
      String metric
  )
  {

    List<PostAggregator> condensedPostAggs = AggregatorUtil.pruneDependentPostAgg(postAggList, metric);
    // calculate dependent aggregators for these postAgg
    Set<String> dependencySet = new HashSet<>();
    dependencySet.add(metric);
    for (PostAggregator postAggregator : condensedPostAggs) {
      dependencySet.addAll(postAggregator.getDependentFields());
    }

    List<AggregatorFactory> condensedAggs = new ArrayList<>();
    for (AggregatorFactory aggregatorSpec : aggList) {
      if (dependencySet.contains(aggregatorSpec.getName())) {
        condensedAggs.add(aggregatorSpec);
      }
    }
    return new Pair<>(condensedAggs, condensedPostAggs);
  }

  /**
   * Only one of fieldName and fieldExpression should be non-null
   */
  static ColumnValueSelector makeColumnValueSelectorWithFloatDefault(
      final ColumnSelectorFactory columnSelectorFactory,
      @Nullable final String fieldName,
      @Nullable final Expr fieldExpression,
      final float nullValue
  )
  {
    if ((fieldName == null) == (fieldExpression == null)) {
      throw new IllegalArgumentException("Only one of fieldName or expression should be non-null");
    }
    if (fieldName != null) {
      return columnSelectorFactory.makeColumnValueSelector(fieldName);
    } else {
      final ColumnValueSelector<ExprEval> baseSelector = ExpressionSelectors.makeExprEvalSelector(
          columnSelectorFactory,
          fieldExpression
      );
      class ExpressionFloatColumnSelector implements FloatColumnSelector
      {
        @Override
        public float getFloat()
        {
          // Although baseSelector.getObject is nullable
          // exprEval returned from Expression selectors is never null.
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval.isNumericNull() ? nullValue : (float) exprEval.asDouble();
        }

        @Override
        public boolean isNull()
        {
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval == null || exprEval.isNumericNull();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
        }
      }
      return new ExpressionFloatColumnSelector();
    }
  }

  /**
   * Only one of fieldName and fieldExpression should be non-null
   */
  static ColumnValueSelector<?> makeColumnValueSelectorWithLongDefault(
      final ColumnSelectorFactory columnSelectorFactory,
      @Nullable final String fieldName,
      @Nullable final Expr fieldExpression,
      final long nullValue
  )
  {
    if ((fieldName == null) == (fieldExpression == null)) {
      throw new IllegalArgumentException("Only one of fieldName and fieldExpression should be non-null");
    }
    if (fieldName != null) {
      return columnSelectorFactory.makeColumnValueSelector(fieldName);
    } else {
      final ColumnValueSelector<ExprEval> baseSelector = ExpressionSelectors.makeExprEvalSelector(
          columnSelectorFactory,
          fieldExpression
      );
      class ExpressionLongColumnSelector implements LongColumnSelector
      {
        @Override
        public long getLong()
        {
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval.isNumericNull() ? nullValue : exprEval.asLong();
        }

        @Override
        public boolean isNull()
        {
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval == null || exprEval.isNumericNull();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
        }
      }
      return new ExpressionLongColumnSelector();
    }
  }

  /**
   * Only one of fieldName and fieldExpression should be non-null
   */
  static ColumnValueSelector<?> makeColumnValueSelectorWithDoubleDefault(
      final ColumnSelectorFactory columnSelectorFactory,
      @Nullable final String fieldName,
      @Nullable final Expr fieldExpression,
      final double nullValue
  )
  {
    if ((fieldName == null) == (fieldExpression == null)) {
      throw new IllegalArgumentException("Only one of fieldName and fieldExpression should be non-null");
    }
    if (fieldName != null) {
      return columnSelectorFactory.makeColumnValueSelector(fieldName);
    } else {
      final ColumnValueSelector<ExprEval> baseSelector = ExpressionSelectors.makeExprEvalSelector(
          columnSelectorFactory,
          fieldExpression
      );
      class ExpressionDoubleColumnSelector implements DoubleColumnSelector
      {
        @Override
        public double getDouble()
        {
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval.isNumericNull() ? nullValue : exprEval.asDouble();
        }

        @Override
        public boolean isNull()
        {
          final ExprEval<?> exprEval = baseSelector.getObject();
          return exprEval == null || exprEval.isNumericNull();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("baseSelector", baseSelector);
        }
      }
      return new ExpressionDoubleColumnSelector();
    }
  }

  public static boolean canVectorize(
      ColumnInspector columnInspector,
      @Nullable String fieldName,
      @Nullable String expression,
      Supplier<Expr> fieldExpression
  )
  {
    if (fieldName != null) {
      final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
      return capabilities == null || capabilities.isNumeric();
    }
    if (expression != null) {
      return fieldExpression.get().canVectorize(columnInspector);
    }
    return false;
  }

  /**
   * Make a {@link VectorValueSelector} for primitive numeric or expression virtual column inputs.
   */
  public static VectorValueSelector makeVectorValueSelector(
      VectorColumnSelectorFactory columnSelectorFactory,
      @Nullable String fieldName,
      @Nullable String expression,
      Supplier<Expr> fieldExpression
  )
  {
    if ((fieldName == null) == (expression == null)) {
      throw new IllegalArgumentException("Only one of fieldName or expression should be non-null");
    }
    if (expression != null) {
      return ExpressionVectorSelectors.makeVectorValueSelector(columnSelectorFactory, fieldExpression.get());
    }
    return columnSelectorFactory.makeValueSelector(fieldName);
  }

  public static Supplier<byte[]> getSimpleAggregatorCacheKeySupplier(
      byte aggregatorType,
      String fieldName,
      Supplier<Expr> fieldExpression
  )
  {
    return Suppliers.memoize(() -> {
      byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
      byte[] expressionBytes = Optional.ofNullable(fieldExpression.get())
                                       .map(Expr::getCacheKey)
                                       .orElse(StringUtils.EMPTY_BYTES);

      return ByteBuffer.allocate(2 + fieldNameBytes.length + expressionBytes.length)
                       .put(aggregatorType)
                       .put(fieldNameBytes)
                       .put(AggregatorUtil.STRING_SEPARATOR)
                       .put(expressionBytes)
                       .array();
    });
  }
}
