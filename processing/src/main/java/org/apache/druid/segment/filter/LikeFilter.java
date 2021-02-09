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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class LikeFilter implements Filter
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final LikeDimFilter.LikeMatcher likeMatcher;
  private final FilterTuning filterTuning;

  public LikeFilter(
      final String dimension,
      final ExtractionFn extractionFn,
      final LikeDimFilter.LikeMatcher likeMatcher,
      final FilterTuning filterTuning
  )
  {
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.likeMatcher = likeMatcher;
    this.filterTuning = filterTuning;
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
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(likeMatcher.predicateFactory(extractionFn));
  }

  @Override
  public boolean canVectorizeMatcher()
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          dimension
      );
    }

    return new LikeFilter(
        rewriteDimensionTo,
        extractionFn,
        likeMatcher,
        filterTuning
    );
  }

  @Override
  public boolean supportsBitmapIndex(BitmapIndexSelector selector)
  {
    return selector.getBitmapIndex(dimension) != null;
  }

  @Override
  public boolean shouldUseBitmapIndex(BitmapIndexSelector selector)
  {
    return Filters.shouldUseBitmapIndex(this, selector, filterTuning);
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final BitmapIndexSelector selector)
  {
    if (isSimpleEquals()) {
      // Verify that dimension equals prefix.
      return ImmutableList.of(
          selector.getBitmapIndex(
              dimension,
              NullHandling.emptyToNullIfNeeded(likeMatcher.getPrefix())
          )
      );
    } else if (isSimplePrefix()) {
      // Verify that dimension startsWith prefix, and is accepted by likeMatcher.matchesSuffixOnly.
      final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

      if (bitmapIndex == null) {
        // Treat this as a column full of nulls
        return ImmutableList.of(likeMatcher.matches(null) ? Filters.allTrue(selector) : Filters.allFalse(selector));
      }

      // search for start, end indexes in the bitmaps; then include all matching bitmaps between those points
      try (final CloseableIndexed<String> dimValues = selector.getDimensionValues(dimension)) {

        // Union bitmaps for all matching dimension values in range.
        // Use lazy iterator to allow unioning bitmaps one by one and avoid materializing all of them at once.
        return Filters.bitmapsFromIndexes(getDimValueIndexIterableForPrefixMatch(bitmapIndex, dimValues), bitmapIndex);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
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

    final String lower = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix());
    final String upper = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix()) + Character.MAX_VALUE;

    final int startIndex; // inclusive
    final int endIndex; // exclusive

    if (lower == null) {
      // For Null values
      startIndex = bitmapIndex.getIndex(null);
      endIndex = startIndex + 1;
    } else {
      final int lowerFound = bitmapIndex.getIndex(lower);
      startIndex = lowerFound >= 0 ? lowerFound : -(lowerFound + 1);

      final int upperFound = bitmapIndex.getIndex(upper);
      endIndex = upperFound >= 0 ? upperFound + 1 : -(upperFound + 1);
    }

    return new IntIterable()
    {
      @Override
      public IntIterator iterator()
      {
        return new IntIterator()
        {
          int currIndex = startIndex;
          int found;

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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LikeFilter that = (LikeFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(likeMatcher, that.likeMatcher) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, extractionFn, likeMatcher, filterTuning);
  }
}
