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
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IdLookup;
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

  @Nullable
  private static BooleanVectorValueMatcher toBooleanMatcherIfPossible(
      final SingleValueDimensionVectorSelector selector,
      final Predicate<String> predicate
  )
  {
    final Boolean booleanValue = ValueMatchers.toBooleanIfPossible(
        selector,
        false,
        predicate
    );

    return booleanValue == null ? null : BooleanVectorValueMatcher.of(selector, booleanValue);
  }

  @Override
  public VectorValueMatcher makeMatcher(@Nullable final String value)
  {
    final String etnValue = NullHandling.emptyToNullIfNeeded(value);

    final VectorValueMatcher booleanMatcher = toBooleanMatcherIfPossible(selector, s -> Objects.equals(s, etnValue));
    if (booleanMatcher != null) {
      return booleanMatcher;
    }

    final IdLookup idLookup = selector.idLookup();
    final int id;

    if (idLookup != null) {
      // Optimization when names can be looked up to IDs ahead of time.
      id = idLookup.lookupId(etnValue);

      if (id < 0) {
        // Value doesn't exist in this column.
        return BooleanVectorValueMatcher.of(selector, false);
      }

      // Check for "id".
      return new BaseVectorValueMatcher(selector)
      {
        final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask)
        {
          final int[] vector = selector.getRowVector();
          final int[] selection = match.getSelection();

          int numRows = 0;

          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int rowNum = mask.getSelection()[i];
            if (vector[rowNum] == id) {
              selection[numRows++] = rowNum;
            }
          }

          match.setSelectionSize(numRows);
          assert match.isValid(mask);
          return match;
        }
      };
    } else {
      return makeMatcher(s -> Objects.equals(s, etnValue));
    }
  }

  @Override
  public VectorValueMatcher makeMatcher(final DruidPredicateFactory predicateFactory)
  {
    return makeMatcher(predicateFactory.makeStringPredicate());
  }

  private VectorValueMatcher makeMatcher(final Predicate<String> predicate)
  {
    final VectorValueMatcher booleanMatcher = toBooleanMatcherIfPossible(selector, predicate);
    if (booleanMatcher != null) {
      return booleanMatcher;
    }

    if (selector.getValueCardinality() > 0) {
      final BitSet checkedIds = new BitSet(selector.getValueCardinality());
      final BitSet matchingIds = new BitSet(selector.getValueCardinality());

      // Lazy matcher; only check an id if matches() is called.
      return new BaseVectorValueMatcher(selector)
      {
        private final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask)
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
              matches = predicate.apply(selector.lookupName(id));
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
          assert match.isValid(mask);
          return match;
        }
      };
    } else {
      // Evaluate "lookupName" and "predicate" on every row.
      return new BaseVectorValueMatcher(selector)
      {
        final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

        @Override
        public ReadableVectorMatch match(final ReadableVectorMatch mask)
        {
          final int[] vector = selector.getRowVector();
          final int[] selection = match.getSelection();

          int numRows = 0;

          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int rowNum = mask.getSelection()[i];
            if (predicate.apply(selector.lookupName(vector[rowNum]))) {
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
}
