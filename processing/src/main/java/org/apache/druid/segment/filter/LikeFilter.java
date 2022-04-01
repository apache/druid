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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
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
  public <T> T getBitmapResult(ColumnIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable(selector));
  }

  @Override
  public double estimateSelectivity(ColumnIndexSelector selector)
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
  public boolean canVectorizeMatcher(ColumnInspector inspector)
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

  @Nullable
  @Override
  public ColumnIndexCapabilities getIndexCapabilities(ColumnIndexSelector selector)
  {
    if (isSimplePrefix()) {
      ColumnIndexCapabilities capabilities = selector.getIndexCapabilities(dimension, LexicographicalRangeIndex.class);
      if (capabilities != null) {
        return Filters.checkFilterTuning(
            selector,
            dimension,
            capabilities,
            filterTuning
        );
      }
    }
    return Filters.checkFilterTuning(
        selector,
        dimension,
        selector.getIndexCapabilities(dimension, StringValueSetIndex.class),
        filterTuning
    );
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, dimension, columnSelector, indexSelector);
  }

  private Iterable<ImmutableBitmap> getBitmapIterable(final ColumnIndexSelector selector)
  {
    if (isSimpleEquals()) {
      // Verify that dimension equals prefix.
      return ImmutableList.of(
          selector.as(dimension, StringValueSetIndex.class).getBitmapForValue(
              NullHandling.emptyToNullIfNeeded(likeMatcher.getPrefix())
          )
      );
    } else if (isSimplePrefix()) {
      final LexicographicalRangeIndex rangeIndex = selector.as(dimension, LexicographicalRangeIndex.class);
      if (rangeIndex == null) {
        // Treat this as a column full of nulls
        return ImmutableList.of(likeMatcher.matches(null) ? Filters.allTrue(selector) : Filters.allFalse(selector));
      }

      final String lower = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix());
      final String upper = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix()) + Character.MAX_VALUE;

      // Union bitmaps for all matching dimension values in range.
      // Use lazy iterator to allow unioning bitmaps one by one and avoid materializing all of them at once.
      return rangeIndex.getBitmapsInRange(
          lower,
          false,
          upper,
          false,
          (value) -> likeMatcher.matchesSuffixOnly(value)
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
