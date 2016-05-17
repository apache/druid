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
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.granularity.QueryGranularities;
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
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.apache.commons.lang.mutable.MutableInt;

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
    final Filter filter = Filters.toFilter(query.getDimensionsFilter());
    final List<DimensionSpec> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final int limit = query.getLimit();
    final boolean descending = query.isDescending();

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
            if (baseFilter != null) {
              bitmap = bitmapFactory.intersection(Arrays.asList(baseFilter, bitmap));
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

    final Sequence<Cursor> cursors = adapter.makeCursors(filter, segment.getDataInterval(), QueryGranularities.ALL, descending);

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
