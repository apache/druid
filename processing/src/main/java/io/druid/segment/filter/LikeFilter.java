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

import com.google.common.base.Strings;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Indexed;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LikeFilter implements Filter
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final LikeDimFilter.LikeMatcher likeMatcher;

  public LikeFilter(
      final String dimension,
      final ExtractionFn extractionFn,
      final LikeDimFilter.LikeMatcher likeMatcher
  )
  {
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.likeMatcher = likeMatcher;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    if (emptyExtractFn() && emptySuffixMatch()) {
      // dimension equals prefix
      return selector.getBitmapIndex(dimension, likeMatcher.getPrefix());
    } else if (emptyExtractFn() && nonEmptyPrefix()) {
      // dimension startsWith prefix and is accepted by likeMatcher.matchesSuffixOnly
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

      if (bitmapIndex == null) {
        // Treat this as a column full of nulls
        return likeMatcher.matches(null) ? Filters.allTrue(selector) : Filters.allFalse(selector);
      }

      // search for start, end indexes in the bitmaps; then include all matching bitmaps between those points
      final Indexed<String> dimValues = selector.getDimensionValues(dimension);

      // Union bitmaps for all matching dimension values in range.
      // Use lazy iterator to allow unioning bitmaps one by one and avoid materializing all of them at once.
      return selector.getBitmapFactory().union(getBitmapIterator(bitmapIndex, likeMatcher, dimValues));
    } else {
      // fallback
      return Filters.matchPredicate(
          dimension,
          selector,
          likeMatcher.predicateFactory(extractionFn).makeStringPredicate()
      );
    }
  }

  @Override
  public double estimateSelectivity(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    if (emptyExtractFn() && emptySuffixMatch()) {
      // dimension equals prefix
      return (double) indexSelector.getBitmapIndex(dimension, likeMatcher.getPrefix()).size()
             / indexSelector.getNumRows();
    } else if (emptyExtractFn() && nonEmptyPrefix()) {
      // dimension startsWith prefix and is accepted by likeMatcher.matchesSuffixOnly
      final BitmapIndex bitmapIndex = indexSelector.getBitmapIndex(dimension);

      if (bitmapIndex == null) {
        // Treat this as a column full of nulls
        return likeMatcher.matches(null) ? 1. : 0.;
      }

      // search for start, end indexes in the bitmaps; then include all matching bitmaps between those points
      final Indexed<String> dimValues = indexSelector.getDimensionValues(dimension);

      // Use lazy iterator to allow getting bitmap size one by one and avoid materializing all of them at once.
      return Filters.estimatePredicateSelectivity(
          bitmapIndex,
          columnSelector,
          dimension,
          getBitmapIndexIterator(bitmapIndex, likeMatcher, dimValues),
          indexSelector.getNumRows()
      );
    } else {
      // fallback
      return Filters.estimatePredicateSelectivity(
          columnSelector,
          dimension,
          indexSelector,
          likeMatcher.predicateFactory(extractionFn).makeStringPredicate()
      );
    }
  }

  private boolean emptyExtractFn()
  {
    return extractionFn == null;
  }

  private boolean emptySuffixMatch()
  {
    return likeMatcher.getSuffixMatch() == LikeDimFilter.LikeMatcher.SuffixMatch.MATCH_EMPTY;
  }

  private boolean nonEmptyPrefix()
  {
    return !likeMatcher.getPrefix().isEmpty();
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, likeMatcher.predicateFactory(extractionFn));
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  private static Iterable<ImmutableBitmap> getBitmapIterator(
      final BitmapIndex bitmapIndex,
      final LikeDimFilter.LikeMatcher likeMatcher,
      final Indexed<String> dimValues
  )
  {
    return Filters.bitmapsFromIndexes(getBitmapIndexIterator(bitmapIndex, likeMatcher, dimValues), bitmapIndex);
  }

  private static Iterable<Integer> getBitmapIndexIterator(
      final BitmapIndex bitmapIndex,
      final LikeDimFilter.LikeMatcher likeMatcher,
      final Indexed<String> dimValues
  )
  {
    final String lower = Strings.nullToEmpty(likeMatcher.getPrefix());
    final String upper = Strings.nullToEmpty(likeMatcher.getPrefix()) + Character.MAX_VALUE;
    final int startIndex; // inclusive
    final int endIndex; // exclusive

    final int lowerFound = bitmapIndex.getIndex(lower);
    startIndex = lowerFound >= 0 ? lowerFound : -(lowerFound + 1);

    final int upperFound = bitmapIndex.getIndex(upper);
    endIndex = upperFound >= 0 ? upperFound + 1 : -(upperFound + 1);

    return new Iterable<Integer>()
    {
      @Override
      public Iterator<Integer> iterator()
      {
        return new Iterator<Integer>()
        {
          int currIndex = startIndex;
          Integer found;

          {
            found = findNext();
          }

          private Integer findNext()
          {
            while (currIndex < endIndex && !likeMatcher.matchesSuffixOnly(dimValues.get(currIndex))) {
              currIndex++;
            }

            if (currIndex < endIndex) {
              return currIndex++;
            } else {
              return null;
            }
          }

          @Override
          public boolean hasNext()
          {
            return found != null;
          }

          @Override
          public Integer next()
          {
            Integer cur = found;

            if (cur == null) {
              throw new NoSuchElementException();
            }

            found = findNext();
            return cur;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
