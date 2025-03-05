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

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.search.SearchQueryRunner.SearchColumnSelectorStrategy;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.filter.Filters;

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
    final List<DimensionSpec> dimensionSpecs = getDimsToSearch(segment, query.getDimensions());
    return ImmutableList.of(
        new CursorBasedExecutor(
            query,
            segment,
            dimensionSpecs
        )
    );
  }

  public static class CursorBasedExecutor extends SearchQueryExecutor
  {
    public CursorBasedExecutor(
        SearchQuery query,
        Segment segment,
        List<DimensionSpec> dimensionSpecs
    )
    {
      super(query, segment, dimensionSpecs);
    }

    @Override
    public Object2IntRBTreeMap<SearchHit> execute(final int limit)
    {
      final CursorFactory adapter = segment.asCursorFactory();
      final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                       .setInterval(query.getSingleInterval())
                                                       .setFilter(
                                                           Filters.convertToCNFFromQueryContext(
                                                               query,
                                                               Filters.toFilter(query.getFilter())
                                                           )
                                                       )
                                                       .setVirtualColumns(query.getVirtualColumns())
                                                       .setPhysicalColumns(query.getRequiredColumns())
                                                       .setQueryContext(query.context())
                                                       .build();
      try (final CursorHolder cursorHolder = adapter.makeCursorHolder(buildSpec)) {
        final Cursor cursor = cursorHolder.asCursor();

        final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
        retVal.defaultReturnValue(0);

        if (cursor == null) {
          return retVal;
        }

        final ColumnSelectorPlus<SearchColumnSelectorStrategy>[] selectorPlusList = DimensionHandlerUtils.createColumnSelectorPluses(
            SearchQueryRunner.SEARCH_COLUMN_SELECTOR_STRATEGY_FACTORY,
            dimsToSearch,
            cursor.getColumnSelectorFactory()
        );

        while (!cursor.isDone()) {
          for (ColumnSelectorPlus<SearchColumnSelectorStrategy> selectorPlus : selectorPlusList) {
            selectorPlus.getColumnSelectorStrategy().updateSearchResultSet(
                selectorPlus.getOutputName(),
                selectorPlus.getSelector(),
                searchQuerySpec,
                limit,
                retVal
            );

            if (retVal.size() >= limit) {
              return retVal;
            }
          }

          cursor.advance();
        }

        return retVal;
      }
    }
  }
}
