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

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.search.SearchQueryRunner;
import io.druid.query.search.SearchQueryRunner.SearchColumnSelectorStrategy;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

public class CursorOnlyStrategy extends SearchStrategy
{
  public static final String NAME = "cursorOnly";

  public static CursorOnlyStrategy of(SearchQuery query)
  {
    return new CursorOnlyStrategy(query);
  }

  private CursorOnlyStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();
    final List<DimensionSpec> dimensionSpecs = getDimsToSearch(adapter.getAvailableDimensions(), query.getDimensions());
    return ImmutableList.<SearchQueryExecutor>of(new CursorBasedExecutor(
        query,
        segment,
        filter,
        interval,
        dimensionSpecs
    ));
  }

  public static class CursorBasedExecutor extends SearchQueryExecutor
  {

    protected Filter filter;
    protected Interval interval;

    public CursorBasedExecutor(
        SearchQuery query,
        Segment segment,
        Filter filter,
        Interval interval, List<DimensionSpec> dimensionSpecs
    )
    {
      super(query, segment, dimensionSpecs);

      this.filter = filter;
      this.interval = interval;
    }

    @Override
    public Object2IntRBTreeMap<SearchHit> execute(final int limit)
    {
      final StorageAdapter adapter = segment.asStorageAdapter();

      final Sequence<Cursor> cursors = adapter.makeCursors(
          filter,
          interval,
          VirtualColumns.EMPTY,
          query.getGranularity(),
          query.isDescending(),
          null
      );

      final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
      retVal.defaultReturnValue(0);

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

              final List<ColumnSelectorPlus<SearchColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                  DimensionHandlerUtils.createColumnSelectorPluses(
                      SearchQueryRunner.SEARCH_COLUMN_SELECTOR_STRATEGY_FACTORY,
                      dimsToSearch,
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

      return retVal;
    }
  }
}
