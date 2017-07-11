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

package io.druid.segment.filter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.BitmapResultFactory;
import io.druid.query.expression.ExprUtils;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.virtual.ExpressionSelectors;

import java.util.Set;

public class ExpressionFilter implements Filter
{
  private final Expr expr;
  private final Set<String> requiredBindings;

  public ExpressionFilter(final Expr expr)
  {
    this.expr = expr;
    this.requiredBindings = ImmutableSet.copyOf(Parser.findRequiredBindings(expr));
  }

  @Override
  public ValueMatcher makeMatcher(final ColumnSelectorFactory factory)
  {
    final LongColumnSelector selector = ExpressionSelectors.makeLongColumnSelector(factory, expr, 0L);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return Evals.asBoolean(selector.get());
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Override
  public boolean supportsBitmapIndex(final BitmapIndexSelector selector)
  {
    if (requiredBindings.isEmpty()) {
      // Constant expression.
      return true;
    } else if (requiredBindings.size() == 1) {
      // Single-column expression. We can use bitmap indexes if this column has an index and does not have
      // multiple values. The lack of multiple values is important because expression filters treat multi-value
      // arrays as nulls, which doesn't permit index based filtering.
      final String column = Iterables.getOnlyElement(requiredBindings);
      return selector.getBitmapIndex(column) != null && !selector.hasMultipleValues(column);
    } else {
      // Multi-column expression.
      return false;
    }
  }

  @Override
  public <T> T getBitmapResult(final BitmapIndexSelector selector, final BitmapResultFactory<T> bitmapResultFactory)
  {
    if (requiredBindings.isEmpty()) {
      // Constant expression.
      if (expr.eval(ExprUtils.nilBindings()).asBoolean()) {
        return bitmapResultFactory.wrapAllTrue(Filters.allTrue(selector));
      } else {
        return bitmapResultFactory.wrapAllFalse(Filters.allFalse(selector));
      }
    } else {
      // Can assume there's only one binding and it has a bitmap index, otherwise supportsBitmapIndex would have
      // returned false and the caller should not have called us.
      final String column = Iterables.getOnlyElement(requiredBindings);
      return Filters.matchPredicate(
          column,
          selector,
          bitmapResultFactory,
          value -> expr.eval(identifierName -> {
            // There's only one binding, and it must be the single column, so it can safely be ignored in production.
            assert column.equals(identifierName);
            return value;
          }).asBoolean()
      );
    }
  }

  @Override
  public boolean supportsSelectivityEstimation(
      final ColumnSelector columnSelector,
      final BitmapIndexSelector indexSelector
  )
  {
    return false;
  }

  @Override
  public double estimateSelectivity(final BitmapIndexSelector indexSelector)
  {
    // Selectivity estimation not supported.
    throw new UnsupportedOperationException();
  }
}
