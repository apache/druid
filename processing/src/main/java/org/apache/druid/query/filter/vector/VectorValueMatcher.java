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
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.ConstantMatcherType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorSizeInspector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

/**
 * An object that returns a boolean indicating if the "current" row should be selected or not. The most prominent use
 * of this interface is that it is returned by the {@link Filter} "makeVectorMatcher" method, where it is used to
 * identify selected rows for filtered cursors and filtered aggregators.
 *
 * @see org.apache.druid.query.filter.ValueMatcher, the non-vectorized version
 */
public interface VectorValueMatcher extends VectorSizeInspector
{
  /**
   * Examine the current vector and return a match indicating what is accepted.
   * <p>
   * Does not modify "mask".
   *
   * @param mask           must not be null; use {@link VectorMatch#allTrue} if you don't need a mask.
   * @param includeUnknown mapping for Druid native two state logic system into SQL three-state logic system. If set
   *                       to true, match vectors returned by this method should include true values wherever the
   *                       matching result is 'unknown', such as from the input being null valued.
   *                       See {@link NullHandling#useThreeValueLogic()}.
   *
   * @return the subset of "mask" that this value matcher accepts. May be the same instance as {@param mask} if
   * every row in the mask matches the filter.
   */
  ReadableVectorMatch match(ReadableVectorMatch mask, boolean includeUnknown);

  /**
   * Make a {@link VectorValueMatcher} that only selects input rows with null values
   * @param selector
   * @return
   */
  static VectorValueMatcher nullMatcher(VectorValueSelector selector)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        return matchNulls(mask, match, selector.getNullVector());
      }
    };
  }

  /**
   * Make an always false {@link VectorValueMatcher} for a {@link SingleValueDimensionVectorSelector}. When
   * {@code includeUnknown} is specified to the {@link VectorValueMatcher#match(ReadableVectorMatch, boolean)} function,
   * this matcher will add all rows of {@link SingleValueDimensionVectorSelector#getRowVector()} which are null to the
   * {@link ReadableVectorMatch} as selections, to participate in Druid 2-state logic system to SQL 3-state logic
   * system conversion.
   */
  static VectorValueMatcher allFalseSingleValueDimensionMatcher(SingleValueDimensionVectorSelector selector)
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

  /**
   * Make an always false {@link VectorValueMatcher} for a {@link MultiValueDimensionVectorSelector}. When
   * {@code includeUnknown} is specified to the {@link VectorValueMatcher#match(ReadableVectorMatch, boolean)} function,
   * this matcher will add all rows of {@link MultiValueDimensionVectorSelector#getRowVector()} which are empty or have
   * any null elements to the {@link ReadableVectorMatch} as selections, to participate in Druid 2-state logic system
   * to SQL 3-state logic system conversion (as best as a multi-value dimension can).
   */
  static VectorValueMatcher allFalseMultiValueDimensionMatcher(MultiValueDimensionVectorSelector selector)
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
            int numRows = 0;
            final IndexedInts[] vector = selector.getRowVector();
            final int[] selection = match.getSelection();

            for (int i = 0; i < mask.getSelectionSize(); i++) {
              final int rowNum = mask.getSelection()[i];
              final IndexedInts row = vector[rowNum];
              if (row.size() == 0) {
                selection[numRows++] = rowNum;
              } else {
                final int size = row.size();
                for (int j = 0; j < size; j++) {
                  if (NullHandling.isNullOrEquivalent(selector.lookupName(row.get(j)))) {
                    selection[numRows++] = rowNum;
                    break;
                  }
                }
              }
            }
            match.setSelectionSize(numRows);
            return match;
          }
          return VectorMatch.allFalse();
        }
      };
    } else {
      final int nullId = idLookup.lookupId(null);
      // null value doesn't exist in column, can safely return all false matcher
      if (nullId < 0) {
        return ConstantMatcherType.ALL_FALSE.asVectorMatcher(selector);
      }

      return new BaseVectorValueMatcher(selector)
      {
        @Override
        public ReadableVectorMatch match(ReadableVectorMatch mask, boolean includeUnknown)
        {
          if (includeUnknown) {
            int numRows = 0;
            final IndexedInts[] vector = selector.getRowVector();
            final int[] selection = match.getSelection();

            for (int i = 0; i < mask.getSelectionSize(); i++) {
              final int rowNum = mask.getSelection()[i];
              final IndexedInts row = vector[rowNum];
              if (row.size() == 0) {
                selection[numRows++] = rowNum;
              } else {
                final int size = row.size();
                for (int j = 0; j < size; j++) {
                  if (row.get(j) == nullId) {
                    selection[numRows++] = rowNum;
                    break;
                  }
                }
              }
            }
            match.setSelectionSize(numRows);
            return match;
          }
          return VectorMatch.allFalse();
        }
      };
    }
  }

  /**
   * Make an always false {@link VectorValueMatcher} for a {@link VectorValueSelector}. When {@code includeUnknown} is
   * specified to the {@link VectorValueMatcher#match(ReadableVectorMatch, boolean)} function, this matcher will add
   * the rows indicated as null values of {@link VectorValueSelector#getNullVector()} to the {@link ReadableVectorMatch}
   * as selections, to participate in Druid 2-state logic system to SQL 3-state logic system conversion.
   */
  static BaseVectorValueMatcher allFalseValueMatcher(VectorValueSelector selector)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        if (includeUnknown) {
          return matchNulls(mask, match, selector.getNullVector());
        }
        return VectorMatch.allFalse();
      }
    };
  }

  /**
   * Make an always false {@link VectorValueMatcher} for a {@link VectorObjectSelector}. When {@code includeUnknown} is
   * specified to the {@link VectorValueMatcher#match(ReadableVectorMatch, boolean)} function, this matcher will add
   * all rows of {@link VectorObjectSelector#getObjectVector()} which are null to the {@link ReadableVectorMatch} as
   * selections, to participate in Druid 2-state logic system to SQL 3-state logic system conversion.
   */
  static VectorValueMatcher allFalseObjectMatcher(VectorObjectSelector selector)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        if (includeUnknown) {
          final Object[] vector = selector.getObjectVector();
          final int[] inputSelection = mask.getSelection();
          final int inputSelectionSize = mask.getSelectionSize();
          final int[] outputSelection = match.getSelection();
          int outputSelectionSize = 0;

          for (int i = 0; i < inputSelectionSize; i++) {
            final int rowNum = inputSelection[i];
            if (vector[rowNum] == null) {
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

  static ReadableVectorMatch matchNulls(
      ReadableVectorMatch mask,
      VectorMatch match,
      @Nullable boolean[] nullVector
  )
  {
    if (nullVector == null) {
      return VectorMatch.allFalse();
    }
    final int[] inputSelection = mask.getSelection();
    final int inputSelectionSize = mask.getSelectionSize();
    final int[] outputSelection = match.getSelection();
    int outputSelectionSize = 0;

    for (int i = 0; i < inputSelectionSize; i++) {
      final int rowNum = inputSelection[i];
      if (nullVector[rowNum]) {
        outputSelection[outputSelectionSize++] = rowNum;
      }
    }

    match.setSelectionSize(outputSelectionSize);
    return match;
  }
}
