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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.ConstantMatcherType;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.util.BitSet;

public class SingleValueStringVectorValueMatcher implements VectorValueMatcherFactory
{
  private final SingleValueDimensionVectorSelector selector;

  public SingleValueStringVectorValueMatcher(final SingleValueDimensionVectorSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable final String value)
  {
    final String etnValue = NullHandling.emptyToNullIfNeeded(value);

    final ConstantMatcherType constantMatcherType = ValueMatchers.toConstantMatcherTypeIfPossible(
        selector,
        false,
        etnValue == null ? DruidObjectPredicate.isNull() : DruidObjectPredicate.equalTo(etnValue)
    );
    if (constantMatcherType != null) {
      return constantMatcherType.asVectorMatcher(selector);
    }

    final IdLookup idLookup = selector.idLookup();
    final int id;

    if (idLookup != null) {
      // Optimization when names can be looked up to IDs ahead of time.
      id = idLookup.lookupId(etnValue);

      if (id < 0) {
        // Value doesn't exist in this column.
        return VectorValueMatcher.allFalseSingleValueDimensionMatcher(selector);
      }
      final boolean hasNull = NullHandling.isNullOrEquivalent(selector.lookupName(0));

      // Check for "id".
      return new BaseVectorValueMatcher(selector)
      {
        final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
        {
          final int[] vector = selector.getRowVector();
          final int[] selection = match.getSelection();

          int numRows = 0;

          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int rowNum = mask.getSelection()[i];
            final int rowId = vector[rowNum];
            if ((includeUnknown && hasNull && rowId == 0) || rowId == id) {
              selection[numRows++] = rowNum;
            }
          }

          match.setSelectionSize(numRows);
          return match;
        }
      };
    } else {
      return makeMatcher(DruidObjectPredicate.equalTo(etnValue));
    }
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
    final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.STRING);
    if (castForComparison == null || castForComparison.asString() == null) {
      return VectorValueMatcher.allFalseSingleValueDimensionMatcher(selector);
    }
    return makeMatcher(castForComparison.asString());
  }

  @Override
  public VectorValueMatcher makeMatcher(final DruidPredicateFactory predicateFactory)
  {
    return makeMatcher(predicateFactory.makeStringPredicate());
  }

  private VectorValueMatcher makeMatcher(final DruidObjectPredicate<String> predicate)
  {
    final ConstantMatcherType constantMatcherType = ValueMatchers.toConstantMatcherTypeIfPossible(
        selector,
        false,
        predicate
    );

    if (constantMatcherType != null) {
      return constantMatcherType.asVectorMatcher(selector);
    }

    if (selector.getValueCardinality() > 0) {
      final BitSet checkedIds = new BitSet(selector.getValueCardinality());
      final BitSet matchingIds = new BitSet(selector.getValueCardinality());

      // Lazy matcher; only check an id if matches() is called.
      return new BaseVectorValueMatcher(selector)
      {
        private final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
        {
          final int[] vector = selector.getRowVector();
          final int[] selection = match.getSelection();

          int numRows = 0;

          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int rowNum = mask.getSelection()[i];
            final int id = vector[rowNum];
            final boolean matches;

            if (checkedIds.get(id)) {
              matches = matchingIds.get(id);
            } else {
              final String val = selector.lookupName(id);
              matches = predicate.apply(val).matches(includeUnknown);
              checkedIds.set(id);
              if (matches) {
                matchingIds.set(id);
              }
            }

            if (matches) {
              selection[numRows++] = rowNum;
            }
          }

          match.setSelectionSize(numRows);
          return match;
        }
      };
    } else {
      // Evaluate "lookupName" and "predicate" on every row.
      return new BaseVectorValueMatcher(selector)
      {
        final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
        {
          final int[] vector = selector.getRowVector();
          final int[] selection = match.getSelection();

          int numRows = 0;

          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int rowNum = mask.getSelection()[i];
            final String val = selector.lookupName(vector[rowNum]);
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
}
