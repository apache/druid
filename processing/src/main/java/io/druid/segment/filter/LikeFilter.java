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
import com.google.common.collect.ImmutableList;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.BitmapResultFactory;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Indexed;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

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
  public <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable(selector));
  }

  @Override
  public double estimateSelectivity(BitmapIndexSelector selector)
  {
    return Filters.estimateSelectivity(getBitmapIterable(selector).iterator(), selector.getNumRows());
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

  @Override
  public boolean supportsSelectivityEstimation(
      ColumnSelector columnSelector, BitmapIndexSelector indexSelector
  )
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final BitmapIndexSelector selector)
  {
    if (isSimpleEquals()) {
      // Verify that dimension equals prefix.
      return ImmutableList.of(selector.getBitmapIndex(dimension, likeMatcher.getPrefix()));
    } else if (isSimplePrefix()) {
      // Verify that dimension startsWith prefix, and is accepted by likeMatcher.matchesSuffixOnly.
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

      if (bitmapIndex == null) {
        // Treat this as a column full of nulls
        return ImmutableList.of(likeMatcher.matches(null) ? Filters.allTrue(selector) : Filters.allFalse(selector));
      }

      // search for start, end indexes in the bitmaps; then include all matching bitmaps between those points
      final Indexed<String> dimValues = selector.getDimensionValues(dimension);

      // Union bitmaps for all matching dimension values in range.
      // Use lazy iterator to allow unioning bitmaps one by one and avoid materializing all of them at once.
      return Filters.bitmapsFromIndexes(
          getDimValueIndexIterableForPrefixMatch(bitmapIndex, dimValues),
          bitmapIndex
      );
    } else {
      // fallback
      return Filters.matchPredicateNoUnion(
          dimension,
          selector,
          likeMatcher.predicateFactory(extractionFn).makeStringPredicate()
      );
    }
  }

  /**
   * Returns true if this filter is a simple equals filter: dimension = 'value' with no extractionFn.
   */
  private boolean isSimpleEquals()
  {
    return extractionFn == null && likeMatcher.getSuffixMatch() == LikeDimFilter.LikeMatcher.SuffixMatch.MATCH_EMPTY;
  }

  /**
   * Returns true if this filter is a simple prefix filter: dimension startsWith 'value' with no extractionFn.
   */
  private boolean isSimplePrefix()
  {
    return extractionFn == null && !likeMatcher.getPrefix().isEmpty();
  }

  private IntIterable getDimValueIndexIterableForPrefixMatch(
      final BitmapIndex bitmapIndex,
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

    return new IntIterable()
    {
      @Override
      public IntIterator iterator()
      {
        return new AbstractIntIterator()
        {
          int currIndex = startIndex;
          int found = -1;

          {
            found = findNext();
          }

          private int findNext()
          {
            while (currIndex < endIndex && !likeMatcher.matchesSuffixOnly(dimValues, currIndex)) {
              currIndex++;
            }

            if (currIndex < endIndex) {
              return currIndex++;
            } else {
              return -1;
            }
          }

          @Override
          public boolean hasNext()
          {
            return found != -1;
          }

          @Override
          public int nextInt()
          {
            int cur = found;

            if (cur == -1) {
              throw new NoSuchElementException();
            }

            found = findNext();
            return cur;
          }
        };
      }
    };
  }
}
