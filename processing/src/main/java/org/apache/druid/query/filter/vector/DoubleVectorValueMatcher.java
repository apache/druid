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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class DoubleVectorValueMatcher implements VectorValueMatcherFactory
{
  private final VectorValueSelector selector;
  private final boolean canHaveNulls = !NullHandling.replaceWithDefault();

  public DoubleVectorValueMatcher(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable final String value)
  {
    if (value == null && canHaveNulls) {
      return makeNullValueMatcher(selector);
    }

    final Double matchVal = DimensionHandlerUtils.convertObjectToDouble(value);

    if (matchVal == null) {
      return BooleanVectorValueMatcher.of(selector, false);
    }

    final double matchValDouble = matchVal;

    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask)
      {
        final double[] vector = selector.getDoubleVector();
        final int[] selection = match.getSelection();
        final boolean[] nulls = selector.getNullVector();
        final boolean hasNulls = canHaveNulls && nulls != null;
        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if (hasNulls && nulls[rowNum]) {
            continue;
          }
          if (vector[rowNum] == matchValDouble) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        assert match.isValid(mask);
        return match;
      }
    };
  }

  @Override
  public VectorValueMatcher makeMatcher(final DruidPredicateFactory predicateFactory)
  {
    final DruidDoublePredicate predicate = predicateFactory.makeDoublePredicate();

    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask)
      {
        final double[] vector = selector.getDoubleVector();
        final int[] selection = match.getSelection();
        final boolean[] nulls = selector.getNullVector();
        final boolean hasNulls = canHaveNulls && nulls != null;

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if (hasNulls && nulls[rowNum]) {
            if (predicate.applyNull()) {
              selection[numRows++] = rowNum;
            }
          } else if (predicate.applyDouble(vector[rowNum])) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        assert match.isValid(mask);
        return match;
      }
    };
  }
}
