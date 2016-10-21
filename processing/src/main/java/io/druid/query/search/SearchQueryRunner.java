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
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  private static final EmittingLogger log = new EmittingLogger(SearchQueryRunner.class);
  private final Segment segment;

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
    final boolean descending = query.isDescending();
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }
    final Interval interval = intervals.get(0);

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex();

    if (index != null) {
      final TreeMap<SearchHit, MutableInt> retVal = Maps.newTreeMap(query.getSort().getComparator());

      Iterable<DimensionSpec> dimsToSearch;
      if (dimensions == null || dimensions.isEmpty()) {
        dimsToSearch = Iterables.transform(index.getAvailableDimensions(), Druids.DIMENSION_IDENTITY);
      } else {
        dimsToSearch = dimensions;
      }

      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

      final ImmutableBitmap baseFilter =
          filter == null ? null : filter.getBitmapIndex(new ColumnSelectorBitmapIndexSelector(bitmapFactory, index));

      ImmutableBitmap timeFilteredBitmap;
      if (!interval.contains(segment.getDataInterval())) {
        MutableBitmap timeBitmap = bitmapFactory.makeEmptyMutableBitmap();
        final Column timeColumn = index.getColumn(Column.TIME_COLUMN_NAME);
        final GenericColumn timeValues = timeColumn.getGenericColumn();

        int startIndex = Math.max(0, getStartIndexOfTime(timeValues, interval.getStartMillis(), true));
        int endIndex = Math.min(timeValues.length() - 1, getStartIndexOfTime(timeValues, interval.getEndMillis(), false));

        for (int i = startIndex; i <= endIndex; i++) {
          timeBitmap.add(i);
        }

        final ImmutableBitmap finalTimeBitmap = bitmapFactory.makeImmutableBitmap(timeBitmap);
        timeFilteredBitmap =
            (baseFilter == null) ? finalTimeBitmap : finalTimeBitmap.intersection(baseFilter);
      } else {
        timeFilteredBitmap = baseFilter;
      }

      for (DimensionSpec dimension : dimsToSearch) {
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
              MutableInt counter = new MutableInt(bitmap.size());
              MutableInt prev = retVal.put(new SearchHit(dimension.getOutputName(), dimVal), counter);
              if (prev != null) {
                counter.add(prev.intValue());
              }
              if (retVal.size() >= limit) {
                return makeReturnResult(limit, retVal);
              }
            }
          }
        }
      }

      return makeReturnResult(limit, retVal);
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

    final Iterable<DimensionSpec> dimsToSearch;
    if (dimensions == null || dimensions.isEmpty()) {
      dimsToSearch = Iterables.transform(adapter.getAvailableDimensions(), Druids.DIMENSION_IDENTITY);
    } else {
      dimsToSearch = dimensions;
    }

    final Sequence<Cursor> cursors = adapter.makeCursors(filter, interval, query.getGranularity(), descending);

    final TreeMap<SearchHit, MutableInt> retVal = cursors.accumulate(
        Maps.<SearchHit, SearchHit, MutableInt>newTreeMap(query.getSort().getComparator()),
        new Accumulator<TreeMap<SearchHit, MutableInt>, Cursor>()
        {
          @Override
          public TreeMap<SearchHit, MutableInt> accumulate(TreeMap<SearchHit, MutableInt> set, Cursor cursor)
          {
            if (set.size() >= limit) {
              return set;
            }

            Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
            for (DimensionSpec dim : dimsToSearch) {
              dimSelectors.put(
                  dim.getOutputName(),
                  cursor.makeDimensionSelector(dim)
              );
            }

            while (!cursor.isDone()) {
              for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
                final DimensionSelector selector = entry.getValue();

                if (selector != null) {
                  final IndexedInts vals = selector.getRow();
                  for (int i = 0; i < vals.size(); ++i) {
                    final String dimVal = selector.lookupName(vals.get(i));
                    if (searchQuerySpec.accept(dimVal)) {
                      MutableInt counter = new MutableInt(1);
                      MutableInt prev = set.put(new SearchHit(entry.getKey(), dimVal), counter);
                      if (prev != null) {
                        counter.add(prev.intValue());
                      }
                      if (set.size() >= limit) {
                        return set;
                      }
                    }
                  }
                }
              }

              cursor.advance();
            }

            return set;
          }
        }
    );

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
      int limit, TreeMap<SearchHit, MutableInt> retVal)
  {
    Iterable<SearchHit> source = Iterables.transform(
        retVal.entrySet(), new Function<Map.Entry<SearchHit, MutableInt>, SearchHit>()
        {
          @Override
          public SearchHit apply(Map.Entry<SearchHit, MutableInt> input)
          {
            SearchHit hit = input.getKey();
            return new SearchHit(hit.getDimension(), hit.getValue(), input.getValue().intValue());
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
}
