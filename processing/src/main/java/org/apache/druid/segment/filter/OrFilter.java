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
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

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
  public <T> FilterBundle forCursor(
      ColumnIndexSelector columnIndexSelector,
      BitmapResultFactory<T> bitmapResultFactory,
      int selectionRowCount,
      int totalRowCount,
      boolean includeUnknown,
      boolean allowPartialIndex
  )
  {
    // todo (clint): if we drop all the index stuff from FilteredOffset, we can delete this override
    //  since the default implementation will be sufficient...
    final List<T> indexes = new ArrayList<>();
    final List<Function<ColumnSelectorFactory, ValueMatcher>> matcherFns = new ArrayList<>();
    final List<Function<VectorColumnSelectorFactory, VectorValueMatcher>> vectorMatcherFns = new ArrayList<>();

    for (Filter subfilter : filters) {
      boolean needMatcher = true;
      final BitmapColumnIndex index = subfilter.getBitmapColumnIndex(columnIndexSelector);
      if (index != null) {
        final T bitmapResult = index.computeBitmapResult(
            bitmapResultFactory,
            totalRowCount,
            totalRowCount,
            includeUnknown
        );
        if (bitmapResult != null) {
          indexes.add(bitmapResult);
          needMatcher = !index.getIndexCapabilities().isExact();
        } else if (!allowPartialIndex) {
          break;
        }
      } else if (!allowPartialIndex) {
        break;
      }

      if (needMatcher) {
        matcherFns.add(subfilter::makeMatcher);
        vectorMatcherFns.add(subfilter::makeVectorMatcher);
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
        matcherFns.clear();
        vectorMatcherFns.clear();
        // need to convert to all matchers
        for (Filter subfilter : filters) {
          matcherFns.add(subfilter::makeMatcher);
          vectorMatcherFns.add(subfilter::makeVectorMatcher);
        }
      }
    }

    final Function<ColumnSelectorFactory, ValueMatcher> matcherFunction;
    final Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFunction;
    if (!matcherFns.isEmpty()) {
      matcherFunction = selectorFactory -> {
        final ValueMatcher[] matchers = new ValueMatcher[matcherFns.size()];
        for (int i = 0; i < matcherFns.size(); i++) {
          matchers[i] = matcherFns.get(i).apply(selectorFactory);
        }
        return makeMatcher(matchers);
      };
      vectorMatcherFunction = selectorFactory -> {
        final VectorValueMatcher[] vectorMatchers = new VectorValueMatcher[vectorMatcherFns.size()];
        for (int i = 0; i < vectorMatcherFns.size(); i++) {
          vectorMatchers[i] = vectorMatcherFns.get(i).apply(selectorFactory);
        }
        return makeVectorMatcher(vectorMatchers);
      };
    } else {
      matcherFunction = null;
      vectorMatcherFunction = null;
    }

    return new FilterBundle(
        index,
        partialIndex,
        matcherFunction,
        vectorMatcherFunction
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
}
