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
import org.apache.druid.query.filter.RowOffsetMatcherFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BitmapOffset;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Logical OR filter operation
 */
public class OrFilter implements BooleanFilter
{
  private static final Joiner OR_JOINER = Joiner.on(" || ");

  private final LinkedHashSet<Filter> filters;

  public OrFilter(LinkedHashSet<Filter> filters)
  {
    Preconditions.checkArgument(filters.size() > 0, "Can't construct empty OrFilter (the universe does not exist)");
    this.filters = filters;
  }

  public OrFilter(List<Filter> filters)
  {
    this(new LinkedHashSet<>(filters));
  }

  @Override
  public <T> FilterBundle makeFilterBundle(
      ColumnIndexSelector columnIndexSelector,
      BitmapResultFactory<T> bitmapResultFactory,
      int selectionRowCount,
      int totalRowCount,
      boolean includeUnknown,
      boolean allowPartialIndex
  )
  {
    final List<T> indexes = new ArrayList<>();
    final List<Filter> indexFilters = new ArrayList<>();
    final List<Filter> matcherFilters = new ArrayList<>();

    for (Filter subfilter : filters) {
      boolean needMatcher = true;
      final BitmapColumnIndex index = subfilter.getBitmapColumnIndex(columnIndexSelector);
      if (index != null) {
        final T bitmapResult = index.computeBitmapResult(
            bitmapResultFactory,
            selectionRowCount,
            totalRowCount,
            includeUnknown
        );
        if (bitmapResult != null) {
          indexes.add(bitmapResult);
          indexFilters.add(subfilter);
          needMatcher = !index.getIndexCapabilities().isExact();
        } else if (!allowPartialIndex) {
          break;
        }
      } else if (!allowPartialIndex) {
        break;
      }

      if (needMatcher) {
        matcherFilters.add(subfilter);
      }
    }

    final ImmutableBitmap index;
    final ImmutableBitmap partialIndex;
    if (indexes.size() == filters.size()) {
      // all or nothing
      index = bitmapResultFactory.toImmutableBitmap(bitmapResultFactory.union(indexes));
      partialIndex = null;
    } else {
      index = null;
      if (allowPartialIndex) {
        if (indexes.isEmpty()) {
          partialIndex = null;
        } else if (indexes.size() == 1) {
          partialIndex = bitmapResultFactory.toImmutableBitmap(indexes.get(0));
        } else {
          partialIndex = bitmapResultFactory.toImmutableBitmap(bitmapResultFactory.union(indexes));
        }
      } else {
        partialIndex = null;
        matcherFilters.clear();
        // need to convert to all matchers
        matcherFilters.addAll(filters);
      }
    }

    final FilterBundle.MatcherBundle matcherBundle;
    if (!matcherFilters.isEmpty()) {
      matcherBundle = new FilterBundle.MatcherBundle()
      {
        @Override
        public String getFilterString()
        {
          if (partialIndex != null) {
            return StringUtils.format(
                "OrFilter:[partialIndex[%s], matcher[%s]]",
                OR_JOINER.join(indexFilters),
                OR_JOINER.join(matcherFilters)
            );
          }
          return OR_JOINER.join(matcherFilters);
        }

        @Override
        public ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending)
        {
          final ValueMatcher[] matchers = new ValueMatcher[matcherFilters.size()];
          for (int i = 0; i < matcherFilters.size(); i++) {
            matchers[i] = matcherFilters.get(i).makeMatcher(selectorFactory);
          }
          ValueMatcher orMatcher = makeMatcher(matchers);
          if (partialIndex != null) {
            RowOffsetMatcherFactory rowOffsetMatcherFactory = new CursorOffsetHolderRowOffsetMatcherFactory(
                baseOffset.getBaseReadableOffset(),
                descending
            );
            ValueMatcher offsetMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(partialIndex);
            return new ValueMatcher()
            {
              @Override
              public boolean matches(boolean includeUnknown)
              {
                return offsetMatcher.matches(includeUnknown) || orMatcher.matches(includeUnknown);
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("offsetMatcher", offsetMatcher);
                inspector.visit("filterMatcher", orMatcher);
              }
            };
          } else {
            return orMatcher;
          }
        }

        @Override
        public VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory)
        {
          final VectorValueMatcher[] vectorMatchers = new VectorValueMatcher[matcherFilters.size()];
          for (int i = 0; i < matcherFilters.size(); i++) {
            vectorMatchers[i] = matcherFilters.get(i).makeVectorMatcher(selectorFactory);
          }
          return makeVectorMatcher(vectorMatchers);
        }
      };
    } else {
      matcherBundle = null;
    }

    return new FilterBundle(
        index == null ? null : new FilterBundle.SimpleIndexBundle(OR_JOINER.join(indexFilters), index),
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

    List<BitmapColumnIndex> bitmapColumnIndices = new ArrayList<>(filters.size());
    ColumnIndexCapabilities merged = new SimpleColumnIndexCapabilities(true, true);
    for (Filter filter : filters) {
      BitmapColumnIndex index = filter.getBitmapColumnIndex(selector);
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
        return bitmapResultFactory.union(
            () -> bitmapColumnIndices.stream().map(x -> x.computeBitmapResult(bitmapResultFactory, includeUnknown)).iterator()
        );
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
    return new OrFilter(newFilters);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s)", OR_JOINER.join(filters));
  }

  private static ValueMatcher makeMatcher(final ValueMatcher[] baseMatchers)
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
          if (matcher.matches(includeUnknown)) {
            return true;
          }
        }
        return false;
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

  private static VectorValueMatcher makeVectorMatcher(final VectorValueMatcher[] baseMatchers)
  {
    Preconditions.checkState(baseMatchers.length > 0);
    if (baseMatchers.length == 1) {
      return baseMatchers[0];
    }

    return new BaseVectorValueMatcher(baseMatchers[0])
    {
      final VectorMatch currentMask = VectorMatch.wrap(new int[getMaxVectorSize()]);
      final VectorMatch scratch = VectorMatch.wrap(new int[getMaxVectorSize()]);
      final VectorMatch retVal = VectorMatch.wrap(new int[getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask, boolean includeUnknown)
      {
        ReadableVectorMatch currentMatch = baseMatchers[0].match(mask, includeUnknown);

        // Initialize currentMask = mask, then progressively remove rows from the mask as we find matches for them.
        // This isn't necessary for correctness (we could use the original "mask" on every call to "match") but it
        // allows for short-circuiting on a row-by-row basis.
        currentMask.copyFrom(mask);

        // Initialize retVal = currentMatch, the rows matched by the first matcher. We'll add more as we loop over
        // the rest of the matchers.
        retVal.copyFrom(currentMatch);

        for (int i = 1; i < baseMatchers.length; i++) {
          if (retVal.isAllTrue(getCurrentVectorSize())) {
            // Short-circuit if the entire vector is true.
            break;
          }

          currentMask.removeAll(currentMatch);
          currentMatch = baseMatchers[i].match(currentMask, false);
          retVal.addAll(currentMatch, scratch);

          if (currentMatch == currentMask) {
            // baseMatchers[i] matched every remaining row. Short-circuit out.
            break;
          }
        }

        assert retVal.isValid(mask);
        return retVal;
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
    OrFilter orFilter = (OrFilter) o;
    return Objects.equals(getFilters(), orFilter.getFilters());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getFilters());
  }

  private static class CursorOffsetHolderRowOffsetMatcherFactory implements RowOffsetMatcherFactory
  {
    private final ReadableOffset offset;
    private final boolean descending;

    CursorOffsetHolderRowOffsetMatcherFactory(ReadableOffset offset, boolean descending)
    {
      this.offset = offset;
      this.descending = descending;
    }

    // Use an iterator-based implementation, ImmutableBitmap.get(index) works differently for Concise and Roaring.
    // ImmutableConciseSet.get(index) is also inefficient, it performs a linear scan on each call
    @Override
    public ValueMatcher makeRowOffsetMatcher(final ImmutableBitmap rowBitmap)
    {
      final IntIterator iter = descending ?
                               BitmapOffset.getReverseBitmapOffsetIterator(rowBitmap) :
                               rowBitmap.iterator();

      if (!iter.hasNext()) {
        return ValueMatchers.allFalse();
      }

      if (descending) {
        return new ValueMatcher()
        {
          int iterOffset = Integer.MAX_VALUE;

          @Override
          public boolean matches(boolean includeUnknown)
          {
            int currentOffset = offset.getOffset();
            while (iterOffset > currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("offset", offset);
            inspector.visit("iter", iter);
          }
        };
      } else {
        return new ValueMatcher()
        {
          int iterOffset = -1;

          @Override
          public boolean matches(boolean includeUnknown)
          {
            int currentOffset = offset.getOffset();
            while (iterOffset < currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("offset", offset);
            inspector.visit("iter", iter);
          }
        };
      }
    }
  }
}
