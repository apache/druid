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

import com.google.common.base.Preconditions;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.function.IntFunction;

/**
 * Like {@link ExpressionColumnValueSelector}, but caches results for the first CACHE_SIZE dictionary IDs of
 * a string column. Must only be used on selectors with dictionaries.
 */
public class SingleStringInputCachingExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  // Number of entries to cache. Each one is a primitive + overhead, so 12500 entries occupies about 250KB
  private static final int CACHE_SIZE = 12500;

  private final DimensionSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();
  private final IntFunction<ExprEval> singleValueEvalCache;
  private ExprEval nullEval = null;

  public SingleStringInputCachingExpressionColumnValueSelector(
      final DimensionSelector selector,
      final Expr expression,
      final int numRows
  )
  {
    // Verify expression has just one binding.
    if (Parser.findRequiredBindings(expression).size() != 1) {
      throw new ISE("WTF?! Expected expression with just one binding");
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");

    this.singleValueEvalCache = DimensionSelectorUtils.cacheIfPossible(
        selector,
        id -> {
          bindings.set(selector.lookupName(id));
          return expression.eval(bindings);
        },
        numRows,
        CACHE_SIZE
    );
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("expression", expression);
    inspector.visit("singleValueEvalCache", singleValueEvalCache);
  }

  @Override
  public double getDouble()
  {
    return eval().asDouble();
  }

  @Override
  public float getFloat()
  {
    return (float) eval().asDouble();
  }

  @Override
  public long getLong()
  {
    return eval().asLong();
  }

  @Nullable
  @Override
  public ExprEval getObject()
  {
    return eval();
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  private ExprEval eval()
  {
    final IndexedInts row = selector.getRow();

    if (row.size() == 1) {
      return singleValueEvalCache.apply(row.get(0));
    } else {
      // Tread non-singly-valued rows as nulls, just like ExpressionSelectors.supplierFromDimensionSelector.
      if (nullEval == null) {
        bindings.set(null);
        nullEval = expression.eval(bindings);
      }

      return nullEval;
    }
  }
}
