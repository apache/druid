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
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * String matcher on top of a {@link VectorObjectSelector}
 */
public class StringObjectVectorValueMatcher implements VectorValueMatcherFactory
{
  protected final VectorObjectSelector selector;

  public StringObjectVectorValueMatcher(final VectorObjectSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable String value)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final Object[] vector = selector.getObjectVector();
        final int[] selection = match.getSelection();

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if ((value == null && includeUnknown) || Objects.equals(value, vector[rowNum])) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
    final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.STRING);
    if (castForComparison == null || castForComparison.asString() == null) {
      return VectorValueMatcher.allFalseObjectMatcher(selector);
    }
    return makeMatcher(castForComparison.asString());
  }

  @Override
  public VectorValueMatcher makeMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();

    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        final Object[] vector = selector.getObjectVector();
        final int[] selection = match.getSelection();

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          final String val = (String) vector[rowNum];
          if (predicate.apply(val).matches(includeUnknown)) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        return match;
      }
    };
  }
}
