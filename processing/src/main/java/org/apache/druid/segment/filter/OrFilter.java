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
import com.google.common.collect.Lists;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterBundle;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BaseVectorValueMatcher;
import org.apache.druid.query.filter.vector.BooleanVectorValueMatcher;
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
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
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
      boolean includeUnknown
  )
  {
    // for OR filters, we have a few possible outcomes:
    // 1 - all clauses are index only bundles. in this case we union the bitmaps together and make an index only bundle
    // 2 - some clauses support indexes. in this case, we union the bitmaps of any index only bundles together to form a
    //     partial index which is constructed into a matcher bundle with convertIndexToMatcherBundle. We translate any
    //     index AND matcher bundles into a matcher only bundle with convertBundleToMatcherOnlyBundle. Finally, we
    //     combine these with the remaining matcher only bundles to with makeMatcher/makeVectorMatcher to make a matcher
    //     only bundle
    // 3 - no clauses support indexes. in this case, we make a matcher only bundle using makeMatcher/makeVectorMatcher

    final List<FilterBundle.IndexBundle> indexOnlyBundles = new ArrayList<>();
    final List<FilterBundle.IndexBundleInfo> indexOnlyBundlesInfo = new ArrayList<>();
    final List<FilterBundle.MatcherBundle> partialIndexBundles = new ArrayList<>();
    final List<FilterBundle.MatcherBundle> matcherOnlyBundles = new ArrayList<>();
    ImmutableBitmap index = null;
    ColumnIndexCapabilities merged = new SimpleColumnIndexCapabilities(true, true);
    int emptyCount = 0;

    final long bitmapConstructionStartNs = System.nanoTime();

    for (Filter subfilter : filters) {
      final FilterBundle bundle = subfilter.makeFilterBundle(
          columnIndexSelector,
          bitmapResultFactory,
          selectionRowCount,
          totalRowCount,
          includeUnknown
      );
      if (bundle.hasIndex()) {
        final ImmutableBitmap bundleIndex = bundle.getIndex().getBitmap();
        if (bundleIndex.isEmpty()) {
          // we leave any indexes which are empty out of index, indexOnlyBundles, and partialIndexBundles
          // even though we skip them, we still keep track of them to check for the case when we can build the OR into
          // an index only bundle. We can count index and matcher bundles here too because the AND operation means that
          // an empty index means the matcher can be skipped
          emptyCount++;
        } else {
          if (bundle.hasMatcher()) {
            // index and matcher bundles must be handled separately, they will need to be a single value matcher built
            // by doing an AND operation between the index and the value matcher
            // (a bundle is basically an AND operation between the index and matcher if the matcher is present)
            partialIndexBundles.add(convertBundleToMatcherOnlyBundle(bundle, bundleIndex));
          } else {
            indexOnlyBundles.add(bundle.getIndex());
            indexOnlyBundlesInfo.add(bundle.getIndex().getIndexInfo());
            merged.merge(bundle.getIndex().getIndexCapabilities());
            // union index only bitmaps together; if all sub-filters are 'index only' bundles we will make an index only
            // bundle ourselves, else we will use this index as a single value matcher
            if (index == null) {
              index = bundle.getIndex().getBitmap();
            } else {
              index = index.union(bundle.getIndex().getBitmap());
            }
          }
        }
      } else {
        matcherOnlyBundles.add(bundle.getMatcherBundle());
      }
    }
    final long totalBitmapConstructTimeNs = System.nanoTime() - bitmapConstructionStartNs;


    // if all the filters are 'index only', we can make an index only bundle
    if (indexOnlyBundles.size() + emptyCount == filters.size()) {
      if (index == null || index.isEmpty()) {
        return FilterBundle.allFalse(
            totalBitmapConstructTimeNs,
            columnIndexSelector.getBitmapFactory().makeEmptyImmutableBitmap()
        );
      }
      if (indexOnlyBundles.size() == 1) {
        return new FilterBundle(
            indexOnlyBundles.get(0),
            null
        );
      }
      return new FilterBundle(
          new FilterBundle.SimpleIndexBundle(
              new FilterBundle.IndexBundleInfo(
                  () -> "OR",
                  selectionRowCount,
                  totalBitmapConstructTimeNs,
                  indexOnlyBundlesInfo
              ),
              index,
              merged
          ),
          null
      );
    }

    // if not the index only outcome, we build a matcher only bundle from all the matchers
    final int estimatedSize = (indexOnlyBundles.isEmpty() ? 0 : 1)
                              + partialIndexBundles.size()
                              + matcherOnlyBundles.size();
    final List<FilterBundle.MatcherBundle> allMatcherBundles = Lists.newArrayListWithCapacity(estimatedSize);
    final List<FilterBundle.MatcherBundleInfo> allMatcherBundlesInfo = Lists.newArrayListWithCapacity(estimatedSize);
    if (!indexOnlyBundles.isEmpty()) {
      // translate the indexOnly bundles into a single matcher
      final FilterBundle.MatcherBundle matcherBundle = convertIndexToMatcherBundle(
          selectionRowCount,
          indexOnlyBundles,
          indexOnlyBundlesInfo,
          totalBitmapConstructTimeNs,
          index
      );
      allMatcherBundles.add(matcherBundle);
      allMatcherBundlesInfo.add(matcherBundle.getMatcherInfo());
    }
    for (FilterBundle.MatcherBundle bundle : partialIndexBundles) {
      allMatcherBundles.add(bundle);
      allMatcherBundlesInfo.add(bundle.getMatcherInfo());
    }
    for (FilterBundle.MatcherBundle bundle : matcherOnlyBundles) {
      allMatcherBundles.add(bundle);
      allMatcherBundlesInfo.add(bundle.getMatcherInfo());
    }

    return new FilterBundle(
        null,
        new FilterBundle.MatcherBundle()
        {
          @Override
          public FilterBundle.MatcherBundleInfo getMatcherInfo()
          {
            return new FilterBundle.MatcherBundleInfo(
                () -> "OR",
                null,
                allMatcherBundlesInfo
            );
          }

          @Override
          public ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending)
          {
            final ValueMatcher[] matchers = new ValueMatcher[allMatcherBundles.size()];
            for (int i = 0; i < allMatcherBundles.size(); i++) {
              matchers[i] = allMatcherBundles.get(i).valueMatcher(selectorFactory, baseOffset, descending);
            }
            return makeMatcher(matchers);
          }

          @Override
          public VectorValueMatcher vectorMatcher(
              VectorColumnSelectorFactory selectorFactory,
              ReadableVectorOffset baseOffset
          )
          {
            final VectorValueMatcher[] matchers = new VectorValueMatcher[allMatcherBundles.size()];
            for (int i = 0; i < allMatcherBundles.size(); i++) {
              matchers[i] = allMatcherBundles.get(i).vectorMatcher(selectorFactory, baseOffset);
            }
            return makeVectorMatcher(matchers);
          }
        }
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

      @Nullable
      @Override
      public <T> T computeBitmapResult(
          BitmapResultFactory<T> bitmapResultFactory,
          int selectionRowCount,
          int totalRowCount,
          boolean includeUnknown
      )
      {
        List<T> results = Lists.newArrayListWithCapacity(bitmapColumnIndices.size());
        for (BitmapColumnIndex index : bitmapColumnIndices) {
          final T r = index.computeBitmapResult(bitmapResultFactory, selectionRowCount, totalRowCount, includeUnknown);
          if (r == null) {
            // all or nothing
            return null;
          }
          results.add(r);
        }
        return bitmapResultFactory.union(results);
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

  /**
   * Convert a {@link FilterBundle} that has both {@link FilterBundle#getIndex()} and
   * {@link FilterBundle#getMatcherBundle()} into a 'matcher only' bundle by converting the index into a matcher
   * with {@link #convertIndexToValueMatcher(ReadableOffset, ImmutableBitmap, boolean)} and
   * {@link #convertIndexToVectorValueMatcher(ReadableVectorOffset, ImmutableBitmap)} and then doing a logical AND
   * with the bundles matchers.
   */
  private static FilterBundle.MatcherBundle convertBundleToMatcherOnlyBundle(
      FilterBundle bundle,
      ImmutableBitmap bundleIndex
  )
  {
    return new FilterBundle.MatcherBundle()
    {
      @Override
      public FilterBundle.MatcherBundleInfo getMatcherInfo()
      {
        return new FilterBundle.MatcherBundleInfo(
            () -> "AND",
            bundle.getIndex().getIndexInfo(),
            Collections.singletonList(bundle.getMatcherBundle().getMatcherInfo())
        );
      }

      @Override
      public ValueMatcher valueMatcher(
          ColumnSelectorFactory selectorFactory,
          Offset baseOffset,
          boolean descending
      )
      {
        return AndFilter.makeMatcher(
            new ValueMatcher[]{
                convertIndexToValueMatcher(baseOffset.getBaseReadableOffset(), bundleIndex, descending),
                bundle.getMatcherBundle().valueMatcher(selectorFactory, baseOffset, descending)
            }
        );
      }

      @Override
      public VectorValueMatcher vectorMatcher(
          VectorColumnSelectorFactory selectorFactory,
          ReadableVectorOffset baseOffset
      )
      {
        return AndFilter.makeVectorMatcher(
            new VectorValueMatcher[]{
                convertIndexToVectorValueMatcher(
                    baseOffset,
                    bundleIndex
                ),
                bundle.getMatcherBundle().vectorMatcher(selectorFactory, baseOffset)
            }
        );
      }
    };
  }

  /**
   * Convert an index into a matcher bundle, using
   * {@link #convertIndexToValueMatcher(ReadableOffset, ImmutableBitmap, boolean)} and
   * {@link #convertIndexToVectorValueMatcher(ReadableVectorOffset, ImmutableBitmap)}
   */
  private static FilterBundle.MatcherBundle convertIndexToMatcherBundle(
      int selectionRowCount,
      List<FilterBundle.IndexBundle> indexOnlyBundles,
      List<FilterBundle.IndexBundleInfo> indexOnlyBundlesInfo,
      long totalBitmapConstructTimeNs,
      ImmutableBitmap partialIndex
  )
  {
    return new FilterBundle.MatcherBundle()
    {
      @Override
      public FilterBundle.MatcherBundleInfo getMatcherInfo()
      {
        if (indexOnlyBundles.size() == 1) {
          return new FilterBundle.MatcherBundleInfo(
              indexOnlyBundles.get(0).getIndexInfo()::getFilter,
              indexOnlyBundles.get(0).getIndexInfo(),
              null
          );
        }
        return new FilterBundle.MatcherBundleInfo(
            () -> "OR",
            new FilterBundle.IndexBundleInfo(
                () -> "OR",
                selectionRowCount,
                totalBitmapConstructTimeNs,
                indexOnlyBundlesInfo
            ),
            null
        );
      }

      @Override
      public ValueMatcher valueMatcher(
          ColumnSelectorFactory selectorFactory,
          Offset baseOffset,
          boolean descending
      )
      {
        return convertIndexToValueMatcher(baseOffset.getBaseReadableOffset(), partialIndex, descending);
      }

      @Override
      public VectorValueMatcher vectorMatcher(
          VectorColumnSelectorFactory selectorFactory,
          ReadableVectorOffset baseOffset
      )
      {
        return convertIndexToVectorValueMatcher(baseOffset, partialIndex);
      }
    };
  }

  private static ValueMatcher convertIndexToValueMatcher(
      final ReadableOffset offset,
      final ImmutableBitmap rowBitmap,
      boolean descending
  )
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

  private static VectorValueMatcher convertIndexToVectorValueMatcher(
      final ReadableVectorOffset vectorOffset,
      final ImmutableBitmap bitmap
  )
  {
    final IntIterator bitmapIterator = bitmap.iterator();
    if (!bitmapIterator.hasNext()) {
      return BooleanVectorValueMatcher.of(vectorOffset, ConstantMatcherType.ALL_FALSE);
    }

    return new VectorValueMatcher()
    {
      final VectorMatch match = VectorMatch.wrap(new int[vectorOffset.getMaxVectorSize()]);
      int iterOffset = -1;
      @Override
      public ReadableVectorMatch match(ReadableVectorMatch mask, boolean includeUnknown)
      {
        final int[] selection = match.getSelection();
        if (vectorOffset.isContiguous()) {
          int numRows = 0;
          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int maskNum = mask.getSelection()[i];
            final int rowNum = vectorOffset.getStartOffset() + maskNum;
            while (iterOffset < rowNum && bitmapIterator.hasNext()) {
              iterOffset = bitmapIterator.next();
            }
            if (iterOffset == rowNum) {
              selection[numRows++] = maskNum;
            }
          }
          match.setSelectionSize(numRows);
          return match;
        } else {
          final int[] currentOffsets = vectorOffset.getOffsets();
          int numRows = 0;
          for (int i = 0; i < mask.getSelectionSize(); i++) {
            final int maskNum = mask.getSelection()[i];
            final int rowNum = currentOffsets[mask.getSelection()[i]];
            while (iterOffset < rowNum && bitmapIterator.hasNext()) {
              iterOffset = bitmapIterator.next();
            }
            if (iterOffset == rowNum) {
              selection[numRows++] = maskNum;
            }
          }
          match.setSelectionSize(numRows);
          return match;
        }
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorOffset.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return vectorOffset.getCurrentVectorSize();
      }
    };
  }
}
