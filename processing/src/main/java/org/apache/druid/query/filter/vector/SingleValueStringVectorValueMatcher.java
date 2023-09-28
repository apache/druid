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

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.ConstantMatcherType;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.Objects;

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
        s -> Objects.equals(s, etnValue)
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
        return makeAllFalseMatcher();
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
      return makeMatcher(s -> Objects.equals(s, etnValue));
    }
  }

  @Override
  public VectorValueMatcher makeMatcher(Object matchValue, ColumnType matchValueType)
  {
    final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnType(matchValueType), matchValue);
    final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.STRING);
    if (castForComparison == null || castForComparison.asString() == null) {
      return makeAllFalseMatcher();
    }
    return makeMatcher(castForComparison.asString());
  }

  @Override
  public VectorValueMatcher makeMatcher(final DruidPredicateFactory predicateFactory)
  {
    return makeMatcher(predicateFactory.makeStringPredicate());
  }

  private VectorValueMatcher makeMatcher(final Predicate<String> predicate)
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
              matches = (includeUnknown && val == null) || predicate.apply(val);
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
            if ((includeUnknown && val == null) || predicate.apply(val)) {
              selection[numRows++] = rowNum;
            }
          }

          match.setSelectionSize(numRows);
          return match;
        }
      };
    }
  }

  private VectorValueMatcher makeAllFalseMatcher()
  {
    final IdLookup idLookup = selector.idLookup();
    final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

    if (idLookup == null || !selector.nameLookupPossibleInAdvance()) {
      // must call selector.lookupName on every id to check for nulls
      return new BaseVectorValueMatcher(selector)
      {
        @Override
        public ReadableVectorMatch match(ReadableVectorMatch mask, boolean includeUnknown)
        {
          if (includeUnknown) {
            final int[] vector = selector.getRowVector();
            final int[] inputSelection = mask.getSelection();
            final int inputSelectionSize = mask.getSelectionSize();
            final int[] outputSelection = match.getSelection();
            int outputSelectionSize = 0;

            for (int i = 0; i < inputSelectionSize; i++) {
              final int rowNum = inputSelection[i];
              if (NullHandling.isNullOrEquivalent(selector.lookupName(vector[rowNum]))) {
                outputSelection[outputSelectionSize++] = rowNum;
              }
            }
            match.setSelectionSize(outputSelectionSize);
            return match;
          }
          return VectorMatch.allFalse();
        }
      };
    } else {
      final int nullId = idLookup.lookupId(null);
      // column doesn't have nulls, can safely return an 'all false' matcher
      if (nullId < 0) {
        return ConstantMatcherType.ALL_FALSE.asVectorMatcher(selector);
      }

      return new BaseVectorValueMatcher(selector)
      {
        @Override
        public ReadableVectorMatch match(ReadableVectorMatch mask, boolean includeUnknown)
        {
          if (includeUnknown) {
            final int[] vector = selector.getRowVector();
            final int[] inputSelection = mask.getSelection();
            final int inputSelectionSize = mask.getSelectionSize();
            final int[] outputSelection = match.getSelection();
            int outputSelectionSize = 0;

            for (int i = 0; i < inputSelectionSize; i++) {
              final int rowNum = inputSelection[i];
              if (vector[rowNum] == nullId) {
                outputSelection[outputSelectionSize++] = rowNum;
              }
            }
            match.setSelectionSize(outputSelectionSize);
            return match;
          }
          return VectorMatch.allFalse();
        }
      };
    }
  }
}
