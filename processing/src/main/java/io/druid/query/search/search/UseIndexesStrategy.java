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

package io.druid.query.search.search;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.java.util.common.Pair;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.CursorOnlyStrategy.CursorBasedExecutor;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class UseIndexesStrategy extends SearchStrategy
{
  public static final String NAME = "useIndexes";

  private final ImmutableBitmap timeFilteredBitmap;
  private final boolean needToMakeFilteredBitmap;

  public static UseIndexesStrategy of(SearchQuery query)
  {
    return new UseIndexesStrategy(query, true, null);
  }

  public static UseIndexesStrategy withTimeFilteredBitmap(
      SearchQuery query,
      @Nullable ImmutableBitmap timeFilteredBitmap
  )
  {
    return new UseIndexesStrategy(query, false, timeFilteredBitmap);
  }

  private UseIndexesStrategy(
      SearchQuery query,
      boolean needToMakeFilteredBitmap,
      @Nullable ImmutableBitmap timeFilteredBitmap
  )
  {
    super(query);
    this.needToMakeFilteredBitmap = needToMakeFilteredBitmap;
    this.timeFilteredBitmap = timeFilteredBitmap;
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final ImmutableList.Builder<SearchQueryExecutor> builder = ImmutableList.builder();
    final QueryableIndex index = segment.asQueryableIndex();
    final StorageAdapter adapter = segment.asStorageAdapter();
    final List<DimensionSpec> searchDims = getDimsToSearch(adapter.getAvailableDimensions(), query.getDimensions());

    if (index != null) {
      final Pair<List<DimensionSpec>, List<DimensionSpec>> pair = // pair of bitmap dims and non-bitmap dims
          partitionDimensionList(adapter, searchDims);
      final List<DimensionSpec> bitmapSuppDims = pair.lhs;
      final List<DimensionSpec> nonBitmapSuppDims = pair.rhs;

      if (bitmapSuppDims.size() > 0) {
        final BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
            index.getBitmapFactoryForDimensions(),
            index
        );

        // Index-only plan is used only when any filter is not specified or the filter supports bitmap indexes.
        //
        // Note: if some filters support bitmap indexes but others are not, the current implementation always employs
        // the cursor-based plan. This can be more optimized. One possible optimization is generating a bitmap index
        // from the non-bitmap-support filter, and then use it to compute the filtered result by intersecting bitmaps.
        if (filter == null || filter.supportsBitmapIndex(selector)) {
          final ImmutableBitmap timeFilteredBitmap = this.needToMakeFilteredBitmap ?
                                                     makeTimeFilteredBitmap(index, segment, filter, interval) :
                                                     this.timeFilteredBitmap;
          builder.add(new IndexOnlyExecutor(query, segment, timeFilteredBitmap, bitmapSuppDims));
        } else {
          // Fall back to cursor-based execution strategy
          nonBitmapSuppDims.addAll(bitmapSuppDims);
        }
      }

      if (nonBitmapSuppDims.size() > 0) {
        builder.add(new CursorBasedExecutor(query, segment, filter, interval, nonBitmapSuppDims));
      }
    } else {
      builder.add(new CursorBasedExecutor(query, segment, filter, interval, searchDims));
    }

    return builder.build();
  }

  // Split dimension list into bitmap-supporting list and non-bitmap supporting list
  private static Pair<List<DimensionSpec>, List<DimensionSpec>> partitionDimensionList(
      StorageAdapter adapter,
      List<DimensionSpec> dimensions
  )
  {
    final List<DimensionSpec> bitmapDims = Lists.newArrayList();
    final List<DimensionSpec> nonBitmapDims = Lists.newArrayList();
    final List<DimensionSpec> dimsToSearch = getDimsToSearch(
        adapter.getAvailableDimensions(),
        dimensions
    );

    for (DimensionSpec spec : dimsToSearch) {
      ColumnCapabilities capabilities = adapter.getColumnCapabilities(spec.getDimension());
      if (capabilities == null) {
        continue;
      }

      if (capabilities.hasBitmapIndexes()) {
        bitmapDims.add(spec);
      } else {
        nonBitmapDims.add(spec);
      }
    }

    return new Pair<List<DimensionSpec>, List<DimensionSpec>>(
        ImmutableList.copyOf(bitmapDims),
        ImmutableList.copyOf(nonBitmapDims)
    );
  }

  static ImmutableBitmap makeTimeFilteredBitmap(
      final QueryableIndex index,
      final Segment segment,
      final Filter filter,
      final Interval interval
  )
  {
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final ImmutableBitmap baseFilter;
    if (filter == null) {
      baseFilter = null;
    } else {
      final BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
          index.getBitmapFactoryForDimensions(),
          index
      );
      Preconditions.checkArgument(filter.supportsBitmapIndex(selector), "filter[%s] should support bitmap", filter);
      baseFilter = filter.getBitmapIndex(selector);
    }

    final ImmutableBitmap timeFilteredBitmap;
    if (!interval.contains(segment.getDataInterval())) {
      final MutableBitmap timeBitmap = bitmapFactory.makeEmptyMutableBitmap();
      final Column timeColumn = index.getColumn(Column.TIME_COLUMN_NAME);
      try (final GenericColumn timeValues = timeColumn.getGenericColumn()) {

        int startIndex = Math.max(0, getStartIndexOfTime(timeValues, interval.getStartMillis(), true));
        int endIndex = Math.min(
            timeValues.length() - 1,
            getStartIndexOfTime(timeValues, interval.getEndMillis(), false)
        );

        for (int i = startIndex; i <= endIndex; i++) {
          timeBitmap.add(i);
        }

        final ImmutableBitmap finalTimeBitmap = bitmapFactory.makeImmutableBitmap(timeBitmap);
        timeFilteredBitmap =
            (baseFilter == null) ? finalTimeBitmap : finalTimeBitmap.intersection(baseFilter);
      }
    } else {
      timeFilteredBitmap = baseFilter;
    }

    return timeFilteredBitmap;
  }

  private static int getStartIndexOfTime(GenericColumn timeValues, long time, boolean inclusive)
  {
    int low = 0;
    int high = timeValues.length() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = timeValues.getLongSingleValueRow(mid);

      if (midVal < time) {
        low = mid + 1;
      } else if (midVal > time) {
        high = mid - 1;
      } else { // key found
        int i;
        // rewind the index of the same time values
        for (i = mid - 1; i >= 0; i--) {
          long prev = timeValues.getLongSingleValueRow(i);
          if (time != prev) {
            break;
          }
        }
        return inclusive ? i + 1 : i;
      }
    }
    // key not found.
    // return insert index
    return inclusive ? low : low - 1;
  }

  public static class IndexOnlyExecutor extends SearchQueryExecutor
  {

    private final ImmutableBitmap timeFilteredBitmap;

    public IndexOnlyExecutor(
        SearchQuery query, Segment segment,
        ImmutableBitmap timeFilteredBitmap,
        List<DimensionSpec> dimensionSpecs
    )
    {
      super(query, segment, dimensionSpecs);
      this.timeFilteredBitmap = timeFilteredBitmap;
    }

    @Override
    public Object2IntRBTreeMap<SearchHit> execute(int limit)
    {
      final QueryableIndex index = segment.asQueryableIndex();
      Preconditions.checkArgument(index != null, "Index should not be null");

      final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
      retVal.defaultReturnValue(0);

      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

      for (DimensionSpec dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension.getDimension());
        if (column == null) {
          continue;
        }

        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        Preconditions.checkArgument(bitmapIndex != null,
                                    "Dimension [%s] should support bitmap index", dimension.getDimension()
        );

        ExtractionFn extractionFn = dimension.getExtractionFn();
        if (extractionFn == null) {
          extractionFn = IdentityExtractionFn.getInstance();
        }
        for (int i = 0; i < bitmapIndex.getCardinality(); ++i) {
          String dimVal = Strings.nullToEmpty(extractionFn.apply(bitmapIndex.getValue(i)));
          if (!searchQuerySpec.accept(dimVal)) {
            continue;
          }
          ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
          if (timeFilteredBitmap != null) {
            bitmap = bitmapFactory.intersection(Arrays.asList(timeFilteredBitmap, bitmap));
          }
          if (bitmap.size() > 0) {
            retVal.addTo(new SearchHit(dimension.getOutputName(), dimVal), bitmap.size());
            if (retVal.size() >= limit) {
              return retVal;
            }
          }
        }
      }

      return retVal;
    }
  }
}
