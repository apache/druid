/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.search;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
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
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

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
  public Sequence<Result<SearchResultValue>> run(final Query<Result<SearchResultValue>> input)
  {
    if (!(input instanceof SearchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
    }

    final SearchQuery query = (SearchQuery) input;
    final Filter filter = Filters.convertDimensionFilters(query.getDimensionsFilter());
    final List<String> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final int limit = query.getLimit();

    final QueryableIndex index = segment.asQueryableIndex();
    if (index != null) {
      final TreeSet<SearchHit> retVal = Sets.newTreeSet(query.getSort().getComparator());

      Iterable<String> dimsToSearch;
      if (dimensions == null || dimensions.isEmpty()) {
        dimsToSearch = index.getAvailableDimensions();
      } else {
        dimsToSearch = dimensions;
      }


      final ImmutableConciseSet baseFilter;
      if (filter == null) {
        // Accept all, and work around https://github.com/metamx/extendedset/issues/1
        if (index.getNumRows() == 1) {
          ConciseSet set = new ConciseSet();
          set.add(0);
          baseFilter = ImmutableConciseSet.newImmutableFromMutable(set);
        }
        else {
          baseFilter = ImmutableConciseSet.complement(new ImmutableConciseSet(), index.getNumRows());
        }
      }
      else {
        baseFilter = filter.goConcise(new ColumnSelectorBitmapIndexSelector(index));
      }

      for (String dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension.toLowerCase());
        if (column == null) {
          continue;
        }

        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex != null) {
          for (int i = 0; i < bitmapIndex.getCardinality(); ++i) {
            String dimVal = Strings.nullToEmpty(bitmapIndex.getValue(i));
            if (searchQuerySpec.accept(dimVal) &&
                ImmutableConciseSet.intersection(baseFilter, bitmapIndex.getConciseSet(i)).size() > 0) {
              retVal.add(new SearchHit(dimension, dimVal));
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
    if (adapter != null) {
      Iterable<String> dimsToSearch;
      if (dimensions == null || dimensions.isEmpty()) {
        dimsToSearch = adapter.getAvailableDimensions();
      } else {
        dimsToSearch = dimensions;
      }

      final TreeSet<SearchHit> retVal = Sets.newTreeSet(query.getSort().getComparator());

      final Iterable<Cursor> cursors = adapter.makeCursors(filter, segment.getDataInterval(), QueryGranularity.ALL);
      for (Cursor cursor : cursors) {
        Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
        for (String dim : dimsToSearch) {
          dimSelectors.put(dim, cursor.makeDimensionSelector(dim));
        }

        while (!cursor.isDone()) {
          for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
            final DimensionSelector selector = entry.getValue();
            final IndexedInts vals = selector.getRow();
            for (int i = 0; i < vals.size(); ++i) {
              final String dimVal = selector.lookupName(vals.get(i));
              if (searchQuerySpec.accept(dimVal)) {
                retVal.add(new SearchHit(entry.getKey(), dimVal));
                if (retVal.size() >= limit) {
                  return makeReturnResult(limit, retVal);
                }
              }
            }
          }

          cursor.advance();
        }
      }

      return makeReturnResult(limit, retVal);
    }

    log.makeAlert("WTF!? Unable to process search query on segment.")
       .addData("segment", segment.getIdentifier())
       .addData("query", query);
    return Sequences.empty();
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(int limit, TreeSet<SearchHit> retVal)
  {
    return Sequences.simple(
        ImmutableList.of(
            new Result<SearchResultValue>(
                segment.getDataInterval().getStart(),
                new SearchResultValue(
                    Lists.newArrayList(new FunctionalIterable<SearchHit>(retVal).limit(limit))
                )
            )
        )
    );
  }
}
