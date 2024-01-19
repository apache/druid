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

package org.apache.druid.query.filter.vector;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class LongVectorValueMatcher implements VectorValueMatcherFactory
{
  private final VectorValueSelector selector;

  public LongVectorValueMatcher(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable final String value)
  {
    if (value == null) {
      return VectorValueMatcher.nullMatcher(selector);
    }

    final Long matchVal = DimensionHandlerUtils.convertObjectToLong(value);

    if (matchVal == null) {
      return VectorValueMatcher.allFalseValueMatcher(selector);
    }

    final long matchValLong = matchVal;

    return makeLongMatcher(matchValLong);
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
    final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.LONG);
    if (castForComparison == null || castForComparison.isNumericNull()) {
      return VectorValueMatcher.allFalseValueMatcher(selector);
    }
    return makeLongMatcher(castForComparison.asLong());
  }

  private BaseVectorValueMatcher makeLongMatcher(long matchValLong)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final long[] vector = selector.getLongVector();
        final int[] selection = match.getSelection();
        final boolean[] nulls = selector.getNullVector();
        final boolean hasNulls = nulls != null;

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if (hasNulls && nulls[rowNum]) {
            if (includeUnknown) {
              selection[numRows++] = rowNum;
            }
          } else if (vector[rowNum] == matchValLong) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }

  @Override
  public VectorValueMatcher makeMatcher(final DruidPredicateFactory predicateFactory)
  {
    final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();

    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final long[] vector = selector.getLongVector();
        final int[] selection = match.getSelection();
        final boolean[] nulls = selector.getNullVector();
        final boolean hasNulls = nulls != null;

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if (hasNulls && nulls[rowNum]) {
            if (predicate.applyNull().matches(includeUnknown)) {
              selection[numRows++] = rowNum;
            }
          } else if (predicate.applyLong(vector[rowNum]).matches(includeUnknown)) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }
}
