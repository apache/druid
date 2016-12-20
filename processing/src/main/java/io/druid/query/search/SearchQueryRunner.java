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

package io.druid.query.search;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  private static final SearchStrategyFactory STRATEGY_FACTORY = new SearchStrategyFactory();

  private static final EmittingLogger log = new EmittingLogger(SearchQueryRunner.class);
  private final Segment segment;

  private static class SearchStrategyFactory implements ColumnSelectorStrategyFactory<SearchColumnSelectorStrategy>
  {
    @Override
    public SearchColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
        case STRING:
          return new StringSearchColumnSelectorStrategy();
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  public interface SearchColumnSelectorStrategy<ValueSelectorType extends ColumnValueSelector> extends ColumnSelectorStrategy
  {
    /**
     * Read the current row from dimSelector and update the search result set.
     *
     * For each row value:
     * 1. Check if searchQuerySpec accept()s the value
     * 2. If so, add the value to the result set and increment the counter for that value
     * 3. If the size of the result set reaches the limit after adding a value, return early.
     *
     * @param outputName Output name for this dimension in the search query being served
     * @param dimSelector Dimension value selector
     * @param searchQuerySpec Spec for the search query
     * @param set The result set of the search query
     * @param limit The limit of the search query
     */
    void updateSearchResultSet(
        String outputName,
        ValueSelectorType dimSelector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    );
  }

  public static class StringSearchColumnSelectorStrategy implements SearchColumnSelectorStrategy<DimensionSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        DimensionSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        final Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final IndexedInts vals = selector.getRow();
        for (int i = 0; i < vals.size(); ++i) {
          final String dimVal = selector.lookupName(vals.get(i));
          if (searchQuerySpec.accept(dimVal)) {
            set.addTo(new SearchHit(outputName, dimVal), 1);
            if (set.size() >= limit) {
              return;
            }
          }
        }
      }
    }
  }

  public SearchQueryRunner(Segment segment)
  {
    this.segment = segment;
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final Query<Result<SearchResultValue>> input,
      Map<String, Object> responseContext
  )
  {
    if (!(input instanceof SearchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
    }

    final SearchQuery query = (SearchQuery) input;
    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));
    final List<DimensionSpec> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final int limit = query.getLimit();
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }
    final Interval interval = intervals.get(0);

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex();

    final StorageAdapter storageAdapter = segment.asStorageAdapter();

    final List<DimensionSpec> bitmapDims = Lists.newArrayList();
    final List<DimensionSpec> nonBitmapDims = Lists.newArrayList();
    partitionDimensionList(index, storageAdapter, dimensions, bitmapDims, nonBitmapDims);

    final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<SearchHit>(query.getSort().getComparator());
    retVal.defaultReturnValue(0);

    // Get results from bitmap supporting dims first
    if (!bitmapDims.isEmpty()) {
      processBitmapDims(index, filter, interval, bitmapDims, searchQuerySpec, limit, retVal);
      // If there are no non-bitmap dims to search, or we've already hit the result limit, just return now
      if (nonBitmapDims.size() == 0 || retVal.size() >= limit) {
        return makeReturnResult(limit, retVal);
      }
    }

    final StorageAdapter adapter = segment.asStorageAdapter();
    if (adapter == null) {
      log.makeAlert("WTF!? Unable to process search query on segment.")
         .addData("segment", segment.getIdentifier())
         .addData("query", query).emit();
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
    processNonBitmapDims(query, adapter, filter, interval, limit, nonBitmapDims, searchQuerySpec, retVal);
    return makeReturnResult(limit, retVal);
  }

  protected int getStartIndexOfTime(GenericColumn timeValues, long time, boolean inclusive)
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

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      int limit,
      Object2IntRBTreeMap<SearchHit> retVal
  )
  {
    Iterable<SearchHit> source = Iterables.transform(
        retVal.object2IntEntrySet(), new Function<Object2IntMap.Entry<SearchHit>, SearchHit>()
        {
          @Override
          public SearchHit apply(Object2IntMap.Entry<SearchHit> input)
          {
            SearchHit hit = input.getKey();
            return new SearchHit(hit.getDimension(), hit.getValue(), input.getIntValue());
          }
        }
    );

    return Sequences.simple(
        ImmutableList.of(
            new Result<SearchResultValue>(
                segment.getDataInterval().getStart(),
                new SearchResultValue(
                    Lists.newArrayList(new FunctionalIterable<SearchHit>(source).limit(limit))
                )
            )
        )
    );
  }

  // Split dimension list into bitmap-supporting list and non-bitmap supporting list
  private void partitionDimensionList(
      QueryableIndex index,
      StorageAdapter storageAdapter,
      List<DimensionSpec> dimensions,
      List<DimensionSpec> bitmapDims,
      List<DimensionSpec> nonBitmapDims
  )
  {
    List<DimensionSpec> dimsToSearch;
    if (dimensions == null || dimensions.isEmpty()) {
      dimsToSearch = Lists.newArrayList(Iterables.transform(
          storageAdapter.getAvailableDimensions(),
          Druids.DIMENSION_IDENTITY
      ));
    } else {
      dimsToSearch = dimensions;
    }

    if (index != null) {
      for (DimensionSpec spec : dimsToSearch) {
        if (spec.getDimension().equals(Column.TIME_COLUMN_NAME)) {
          bitmapDims.add(spec);
          continue;
        }
        ColumnCapabilities capabilities = storageAdapter.getColumnCapabilities(spec.getDimension());
        if (capabilities == null) {
          continue;
        }

        if (capabilities.hasBitmapIndexes()) {
          bitmapDims.add(spec);
        } else {
          nonBitmapDims.add(spec);
        }
      }
    } else {
      // no QueryableIndex available, so nothing has bitmaps
      nonBitmapDims.addAll(dimsToSearch);
    }
  }

  private void processNonBitmapDims(
      SearchQuery query,
      final StorageAdapter adapter,
      Filter filter,
      Interval interval,
      final int limit,
      final List<DimensionSpec> nonBitmapDims,
      final SearchQuerySpec searchQuerySpec,
      final Object2IntRBTreeMap<SearchHit> retVal
  )
  {
    final Sequence<Cursor> cursors = adapter.makeCursors(
        filter,
        interval,
        VirtualColumns.EMPTY,
        query.getGranularity(),
        query.isDescending()
    );

    cursors.accumulate(
        retVal,
        new Accumulator<Object2IntRBTreeMap<SearchHit>, Cursor>()
        {
          @Override
          public Object2IntRBTreeMap<SearchHit> accumulate(Object2IntRBTreeMap<SearchHit> set, Cursor cursor)
          {
            if (set.size() >= limit) {
              return set;
            }

            List<ColumnSelectorPlus<SearchColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                DimensionHandlerUtils.createColumnSelectorPluses(
                    STRATEGY_FACTORY,
                    nonBitmapDims,
                    cursor
                )
            );

            while (!cursor.isDone()) {
              for (ColumnSelectorPlus<SearchColumnSelectorStrategy> selectorPlus : selectorPlusList) {
                selectorPlus.getColumnSelectorStrategy().updateSearchResultSet(
                    selectorPlus.getOutputName(),
                    selectorPlus.getSelector(),
                    searchQuerySpec,
                    limit,
                    set
                );

                if (set.size() >= limit) {
                  return set;
                }
              }

              cursor.advance();
            }

            return set;
          }
        }
    );
  }

  private void processBitmapDims(
      QueryableIndex index,
      Filter filter,
      Interval interval,
      List<DimensionSpec> bitmapDims,
      SearchQuerySpec searchQuerySpec,
      int limit,
      final Object2IntRBTreeMap<SearchHit> retVal
  )
  {
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

    final ImmutableBitmap baseFilter =
        filter == null ? null : filter.getBitmapIndex(new ColumnSelectorBitmapIndexSelector(bitmapFactory, index));

    ImmutableBitmap timeFilteredBitmap;
    if (!interval.contains(segment.getDataInterval())) {
      MutableBitmap timeBitmap = bitmapFactory.makeEmptyMutableBitmap();
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

    for (DimensionSpec dimension : bitmapDims) {
      final Column column = index.getColumn(dimension.getDimension());
      if (column == null) {
        continue;
      }

      final BitmapIndex bitmapIndex = column.getBitmapIndex();
      ExtractionFn extractionFn = dimension.getExtractionFn();
      if (extractionFn == null) {
        extractionFn = IdentityExtractionFn.getInstance();
      }
      if (bitmapIndex != null) {
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
              return;
            }
          }
        }
      }
    }
  }
}
