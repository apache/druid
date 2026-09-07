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

package org.apache.druid.query.search;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.Order;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.search.CursorOnlyStrategy.CursorBasedExecutor;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class UseIndexesStrategy extends SearchStrategy
{
  public static final String NAME = "useIndexes";

  public static UseIndexesStrategy of(SearchQuery query)
  {
    return new UseIndexesStrategy(query);
  }

  private UseIndexesStrategy(
      SearchQuery query
  )
  {
    super(query);
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final ImmutableList.Builder<SearchQueryExecutor> builder = ImmutableList.builder();
    final List<DimensionSpec> searchDims = getDimsToSearch(segment, query.getDimensions());

    final QueryableIndex index = segment.as(QueryableIndex.class);
    if (index == null) {
      return Collections.singletonList(new CursorBasedExecutor(query, segment, searchDims));
    }

    try (final Closer closer = Closer.create()) {
      final ColumnIndexSelector selector = new ColumnCache(index, query.getVirtualColumns(), closer);

      // pair of bitmap dims and non-bitmap dims
      final NonnullPair<List<DimensionSpec>, List<DimensionSpec>> pair = partitionDimensionList(
          segment,
          selector,
          searchDims
      );
      final List<DimensionSpec> bitmapSuppDims = pair.lhs;
      final List<DimensionSpec> nonBitmapSuppDims = pair.rhs;

      if (!bitmapSuppDims.isEmpty()) {
        // Index-only plan is used only when any filter is not specified or the filter supports bitmap indexes.
        //
        // Note: if some filters support bitmap indexes but others are not, the current implementation always employs
        // the cursor-based plan. This can be more optimized. One possible optimization is generating a bitmap index
        // from the non-bitmap-support filter, and then use it to compute the filtered result by intersecting bitmaps.
        if ((filter == null || filter.getBitmapColumnIndex(selector) != null)
            && Cursors.getTimeOrdering(index.getOrdering()) == Order.ASCENDING) {
          final ImmutableBitmap timeFilteredBitmap = makeTimeFilteredBitmap(
              index,
              segment,
              query.getVirtualColumns(),
              filter,
              interval
          );
          builder.add(new IndexOnlyExecutor(query, segment, timeFilteredBitmap, bitmapSuppDims));
        } else {
          // Fall back to cursor-based execution strategy
          nonBitmapSuppDims.addAll(bitmapSuppDims);
        }
      }

      if (!nonBitmapSuppDims.isEmpty()) {
        builder.add(new CursorBasedExecutor(query, segment, nonBitmapSuppDims));
      }

      return builder.build();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Split the given dimensions list into columns which provide {@link DictionaryEncodedStringValueIndex} and those
   * which do not. Note that the returned lists are free to modify.
   */
  private static NonnullPair<List<DimensionSpec>, List<DimensionSpec>> partitionDimensionList(
      Segment segment,
      ColumnIndexSelector columnIndexSelector,
      List<DimensionSpec> dimensions
  )
  {
    final List<DimensionSpec> bitmapDims = new ArrayList<>();
    final List<DimensionSpec> nonBitmapDims = new ArrayList<>();
    final List<DimensionSpec> dimsToSearch = getDimsToSearch(
        segment,
        dimensions
    );
    for (DimensionSpec spec : dimsToSearch) {
      ColumnIndexSupplier indexSupplier = columnIndexSelector.getIndexSupplier(spec.getDimension());
      if (indexSupplier == null) {
        // column doesn't exist, ignore it
        continue;
      }

      if (indexSupplier.as(DictionaryEncodedStringValueIndex.class) != null) {
        bitmapDims.add(spec);
      } else {
        nonBitmapDims.add(spec);
      }
    }

    return new NonnullPair<>(bitmapDims, nonBitmapDims);
  }

  static ImmutableBitmap makeTimeFilteredBitmap(
      final QueryableIndex index,
      final Segment segment,
      final VirtualColumns virtualColumns,
      final Filter filter,
      final Interval interval
  )
  {
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final ImmutableBitmap baseFilter;
    if (filter == null) {
      baseFilter = null;
    } else {
      try (final Closer closer = Closer.create()) {
        final ColumnIndexSelector selector = new ColumnCache(index, virtualColumns, closer);
        final BitmapColumnIndex columnIndex = filter.getBitmapColumnIndex(selector);
        Preconditions.checkNotNull(
            columnIndex,
            "filter[%s] should support bitmap",
            filter
        );
        baseFilter = columnIndex.computeBitmapResult(
            new DefaultBitmapResultFactory(selector.getBitmapFactory()),
            false
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    final ImmutableBitmap timeFilteredBitmap;
    if (!interval.contains(segment.getDataInterval())) {
      final MutableBitmap timeBitmap = bitmapFactory.makeEmptyMutableBitmap();
      final ColumnHolder timeColumnHolder = index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
      try (final NumericColumn timeValues = (NumericColumn) timeColumnHolder.getColumn()) {

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

  private static int getStartIndexOfTime(NumericColumn timeValues, long time, boolean inclusive)
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
        SearchQuery query,
        Segment segment,
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
      final QueryableIndex index = segment.as(QueryableIndex.class);
      Preconditions.checkArgument(index != null, "Index should not be null");

      try (final Closer closer = Closer.create()) {
        final ColumnIndexSelector indexSelector = new ColumnCache(index, query.getVirtualColumns(), closer);

        final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
        retVal.defaultReturnValue(0);

        final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

        for (DimensionSpec dimension : dimsToSearch) {

          final ColumnIndexSupplier indexSupplier = indexSelector.getIndexSupplier(dimension.getDimension());

          ExtractionFn extractionFn = dimension.getExtractionFn();
          if (extractionFn == null) {
            extractionFn = IdentityExtractionFn.getInstance();
          }
          // if indexSupplier is null here, it means the column is missing
          if (indexSupplier == null) {
            String dimVal = extractionFn.apply(null);
            if (!searchQuerySpec.accept(dimVal)) {
              continue;
            }
            ImmutableBitmap bitmap =
                bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), index.getNumRows());
            if (timeFilteredBitmap != null) {
              bitmap = bitmapFactory.intersection(Arrays.asList(timeFilteredBitmap, bitmap));
            }
            if (!bitmap.isEmpty()) {
              retVal.addTo(new SearchHit(dimension.getOutputName(), dimVal), bitmap.size());
              if (retVal.size() >= limit) {
                return retVal;
              }
            }
          } else {
            // these were checked to be non-null in partitionDimensionList
            final DictionaryEncodedStringValueIndex bitmapIndex =
                indexSupplier.as(DictionaryEncodedStringValueIndex.class);
            final Iterator<String> iterator = bitmapIndex.getValueIterator();
            int i = 0;
            while (iterator.hasNext()) {
              final String dimVal = extractionFn.apply(iterator.next());
              if (!searchQuerySpec.accept(dimVal)) {
                i++;
                continue;
              }
              ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
              if (timeFilteredBitmap != null) {
                bitmap = bitmapFactory.intersection(Arrays.asList(timeFilteredBitmap, bitmap));
              }
              if (!bitmap.isEmpty()) {
                retVal.addTo(new SearchHit(dimension.getOutputName(), dimVal), bitmap.size());
                if (retVal.size() >= limit) {
                  return retVal;
                }
              }
              i++;
            }
          }
        }

        return retVal;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
