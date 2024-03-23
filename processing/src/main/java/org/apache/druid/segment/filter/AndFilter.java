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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterBundle;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Logical AND filter operation
 */
public class AndFilter implements BooleanFilter
{
  public static final Joiner AND_JOINER = Joiner.on(" && ");

  private final LinkedHashSet<Filter> filters;

  public AndFilter(LinkedHashSet<Filter> filters)
  {
    Preconditions.checkArgument(filters.size() > 0, "Can't construct empty AndFilter");
    this.filters = filters;
  }

  @VisibleForTesting
  public AndFilter(List<Filter> filters)
  {
    this(new LinkedHashSet<>(filters));
  }

  @Override
  public <T> FilterBundle makeFilterBundle(
      ColumnIndexSelector columnIndexSelector,
      BitmapResultFactory<T> bitmapResultFactory,
      int selectionRowCount,
      int totalRowCount,
      boolean includeUnknown
  )
  {
    final List<FilterBundle.IndexBundleInfo> indexBundleInfos = new ArrayList<>();
    final List<FilterBundle.MatcherBundle> matcherBundles = new ArrayList<>();
    final List<FilterBundle.MatcherBundleInfo> matcherBundleInfos = new ArrayList<>();

    int selectionCount = selectionRowCount;
    ImmutableBitmap index = null;
    ColumnIndexCapabilities merged = new SimpleColumnIndexCapabilities(true, true);
    // AND filter can be partitioned into a bundle that has both indexes and value matchers. The filters which support
    // indexes are computed into bitmaps and intersected together incrementally, feeding forward the selected row count
    // (number of set bits on the bitmap), allowing filters to skip index computation if it would be more expensive
    // than using a ValueMatcher for the remaining number of rows. Any filter which was not computed into an index is
    // converted into a ValueMatcher which are combined using the makeMatcher/makeVectorMatcher functions.
    // We delegate to the makeFilterBundle of the AND clauses to allow this partitioning to be pushed down. For example,
    // a nested AND filter might also partition itself into indexes and bundles, and since it is part of a logical AND
    // operation, this is valid (and even preferable).
    final long bitmapConstructionStartNs = System.nanoTime();
    for (Filter subfilter : filters) {
      final FilterBundle subBundle = subfilter.makeFilterBundle(
          columnIndexSelector,
          bitmapResultFactory,
          selectionCount,
          totalRowCount,
          includeUnknown
      );
      if (subBundle.getIndex() != null) {
        if (subBundle.getIndex().getBitmap().isEmpty()) {
          // if nothing matches for any sub filter, short-circuit, because nothing can possibly match
          return FilterBundle.allFalse(
              System.nanoTime() - bitmapConstructionStartNs,
              columnIndexSelector.getBitmapFactory().makeEmptyImmutableBitmap()
          );
        }
        merged = merged.merge(subBundle.getIndex().getIndexCapabilities());
        indexBundleInfos.add(subBundle.getIndex().getIndexInfo());
        if (index == null) {
          index = subBundle.getIndex().getBitmap();
        } else {
          index = index.intersection(subBundle.getIndex().getBitmap());
        }
        selectionCount = index.size();
      }
      if (subBundle.getMatcherBundle() != null) {
        matcherBundles.add(subBundle.getMatcherBundle());
        matcherBundleInfos.add(subBundle.getMatcherBundle().getMatcherInfo());
      }
    }

    final FilterBundle.IndexBundle indexBundle;
    if (index != null) {
      if (indexBundleInfos.size() == 1) {
        indexBundle = new FilterBundle.SimpleIndexBundle(
            indexBundleInfos.get(0),
            index,
            merged
        );
      } else {
        indexBundle = new FilterBundle.SimpleIndexBundle(
            new FilterBundle.IndexBundleInfo(
                () -> "AND",
                selectionCount,
                System.nanoTime() - bitmapConstructionStartNs,
                indexBundleInfos
            ),
            index,
            merged
        );
      }
    } else {
      indexBundle = null;
    }

    final FilterBundle.MatcherBundle matcherBundle;
    if (!matcherBundles.isEmpty()) {
      matcherBundle = new FilterBundle.MatcherBundle()
      {
        @Override
        public FilterBundle.MatcherBundleInfo getMatcherInfo()
        {
          if (matcherBundles.size() == 1) {
            return matcherBundleInfos.get(0);
          }
          return new FilterBundle.MatcherBundleInfo(
              () -> "AND",
              null,
              matcherBundleInfos
          );
        }

        @Override
        public ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending)
        {
          final ValueMatcher[] matchers = new ValueMatcher[matcherBundles.size()];
          for (int i = 0; i < matcherBundles.size(); i++) {
            matchers[i] = matcherBundles.get(i).valueMatcher(selectorFactory, baseOffset, descending);
          }
          return makeMatcher(matchers);
        }

        @Override
        public VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory, ReadableVectorOffset baseOffset)
        {
          final VectorValueMatcher[] vectorMatchers = new VectorValueMatcher[matcherBundles.size()];
          for (int i = 0; i < matcherBundles.size(); i++) {
            vectorMatchers[i] = matcherBundles.get(i).vectorMatcher(selectorFactory, baseOffset);
          }
          return makeVectorMatcher(vectorMatchers);
        }
      };
    } else {
      matcherBundle = null;
    }

    return new FilterBundle(
        indexBundle,
        matcherBundle
    );
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (filters.size() == 1) {
      return Iterables.getOnlyElement(filters).getBitmapColumnIndex(selector);
    }

    final List<BitmapColumnIndex> bitmapColumnIndices = new ArrayList<>(filters.size());
    ColumnIndexCapabilities merged = new SimpleColumnIndexCapabilities(true, true);
    for (final Filter filter : filters) {
      final BitmapColumnIndex index = filter.getBitmapColumnIndex(selector);
      if (index == null) {
        // all or nothing
        return null;
      }
      merged = merged.merge(index.getIndexCapabilities());
      bitmapColumnIndices.add(index);
    }

    final ColumnIndexCapabilities finalMerged = merged;
    return new BitmapColumnIndex()
    {
      @Override
      public ColumnIndexCapabilities getIndexCapabilities()
      {
        return finalMerged;
      }

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        final List<T> bitmapResults = new ArrayList<>(bitmapColumnIndices.size());
        for (final BitmapColumnIndex index : bitmapColumnIndices) {
          final T bitmapResult = index.computeBitmapResult(bitmapResultFactory, includeUnknown);
          if (bitmapResultFactory.isEmpty(bitmapResult)) {
            // Short-circuit.
            return bitmapResultFactory.wrapAllFalse(selector.getBitmapFactory().makeEmptyImmutableBitmap());
          }
          bitmapResults.add(bitmapResult);
        }
        return bitmapResultFactory.intersection(bitmapResults);
      }

      @Nullable
      @Override
      public <T> T computeBitmapResult(
          BitmapResultFactory<T> bitmapResultFactory,
          int selectionRowCount,
          int totalRowCount,
          boolean includeUnknown
      )
      {
        final List<T> bitmapResults = new ArrayList<>(bitmapColumnIndices.size());
        for (final BitmapColumnIndex index : bitmapColumnIndices) {
          final T bitmapResult = index.computeBitmapResult(
              bitmapResultFactory,
              selectionRowCount,
              totalRowCount,
              includeUnknown
          );
          if (bitmapResult == null) {
            // all or nothing
            return null;
          }
          if (bitmapResultFactory.isEmpty(bitmapResult)) {
            // Short-circuit.
            return bitmapResultFactory.wrapAllFalse(selector.getBitmapFactory().makeEmptyImmutableBitmap());
          }
          bitmapResults.add(bitmapResult);
        }
        return bitmapResultFactory.intersection(bitmapResults);
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    final ValueMatcher[] matchers = new ValueMatcher[filters.size()];

    int i = 0;
    for (Filter filter : filters) {
      matchers[i++] = filter.makeMatcher(factory);
    }
    return makeMatcher(matchers);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    final VectorValueMatcher[] matchers = new VectorValueMatcher[filters.size()];

    int i = 0;
    for (Filter filter : filters) {
      matchers[i++] = filter.makeVectorMatcher(factory);
    }
    return makeVectorMatcher(matchers);
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return filters.stream().allMatch(filter -> filter.canVectorizeMatcher(inspector));
  }

  @Override
  public LinkedHashSet<Filter> getFilters()
  {
    return filters;
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    for (Filter filter : filters) {
      if (!filter.supportsRequiredColumnRewrite()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    final List<Filter> newFilters = new ArrayList<>(filters.size());
    for (Filter filter : filters) {
      newFilters.add(filter.rewriteRequiredColumns(columnRewrites));
    }
    return new AndFilter(newFilters);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s)", AND_JOINER.join(filters));
  }

  public static ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers)
  {
    Preconditions.checkState(baseMatchers.length > 0);
    if (baseMatchers.length == 1) {
      return baseMatchers[0];
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        for (ValueMatcher matcher : baseMatchers) {
          if (!matcher.matches(includeUnknown)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("firstBaseMatcher", baseMatchers[0]);
        inspector.visit("secondBaseMatcher", baseMatchers[1]);
        // Don't inspect the 3rd and all consequent baseMatchers, cut runtime shape combinations at this point.
        // Anyway if the filter is so complex, Hotspot won't inline all calls because of the inline limit.
      }
    };
  }

  public static VectorValueMatcher makeVectorMatcher(final VectorValueMatcher[] baseMatchers)
  {
    Preconditions.checkState(baseMatchers.length > 0);
    if (baseMatchers.length == 1) {
      return baseMatchers[0];
    }

    return new BaseVectorValueMatcher(baseMatchers[0])
    {
      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        ReadableVectorMatch match = mask;

        for (VectorValueMatcher matcher : baseMatchers) {
          if (match.isAllFalse()) {
            // Short-circuit if the entire vector is false.
            break;
          }
          match = matcher.match(match, includeUnknown);
        }

        assert match.isValid(mask);
        return match;
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
    AndFilter andFilter = (AndFilter) o;
    return Objects.equals(getFilters(), andFilter.getFilters());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getFilters());
  }
}
