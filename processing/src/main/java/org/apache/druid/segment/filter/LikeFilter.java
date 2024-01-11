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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
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
  @Nullable
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(dimension);
    if (indexSupplier == null) {
      // Treat this as a column full of nulls
      return likeMatcher.matches(null).matches(false)
             ? new AllTrueBitmapColumnIndex(selector)
             : new AllUnknownBitmapColumnIndex(selector);
    }
    if (isSimpleEquals()) {
      StringValueSetIndexes valueIndexes = indexSupplier.as(StringValueSetIndexes.class);
      if (valueIndexes != null) {
        return valueIndexes.forValue(
            NullHandling.emptyToNullIfNeeded(likeMatcher.getPrefix())
        );
      }
    }
    if (isSimplePrefix()) {
      final LexicographicalRangeIndexes rangeIndexes = indexSupplier.as(LexicographicalRangeIndexes.class);
      if (rangeIndexes != null) {
        final String lower = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix());
        final String upper = NullHandling.nullToEmptyIfNeeded(likeMatcher.getPrefix()) + Character.MAX_VALUE;

        if (likeMatcher.getSuffixMatch() == LikeDimFilter.LikeMatcher.SuffixMatch.MATCH_ANY) {
          return rangeIndexes.forRange(lower, false, upper, false);
        } else {
          return rangeIndexes.forRange(lower, false, upper, false, likeMatcher::matchesSuffixOnly);
        }
      }
    }

    // fallback to predicate index
    return Filters.makePredicateIndex(
        dimension,
        selector,
        likeMatcher.predicateFactory(extractionFn)
    );
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
