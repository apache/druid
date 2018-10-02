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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.segment.ColumnSelectorBitmapIndexSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.List;

public class AutoStrategy extends SearchStrategy
{
  public static final String NAME = "auto";

  private static final EmittingLogger log = new EmittingLogger(AutoStrategy.class);

  public static AutoStrategy of(SearchQuery query)
  {
    return new AutoStrategy(query);
  }

  private AutoStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final QueryableIndex index = segment.asQueryableIndex();

    if (index != null) {
      final BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
          index.getBitmapFactoryForDimensions(),
          VirtualColumns.EMPTY,
          index
      );

      // Index-only plan is used only when any filter is not specified or the filter supports bitmap indexes.
      //
      // Note: if some filters support bitmap indexes but others are not, the current implementation always employs
      // the cursor-based plan. This can be more optimized. One possible optimization is generating a bitmap index
      // from the non-bitmap-support filters, and then use it to compute the filtered result by intersecting bitmaps.
      if (filter == null || filter.supportsSelectivityEstimation(index, selector)) {
        final List<DimensionSpec> dimsToSearch = getDimsToSearch(
            index.getAvailableDimensions(),
            query.getDimensions()
        );

        // Choose a search query execution strategy depending on the query.
        // The costs of index-only plan and cursor-based plan can be computed like below.
        //
        // c_index = (total cardinality of all search dimensions) * (bitmap intersection cost)
        //            * (search predicate processing cost)
        // c_cursor = (# of rows in a segment) * (filter selectivity) * (# of dimensions)
        //            * (search predicate processing cost)
        final SearchQueryDecisionHelper helper = getDecisionHelper(index);
        final double useIndexStrategyCost = helper.getBitmapIntersectCost() * computeTotalCard(index, dimsToSearch);
        final double cursorOnlyStrategyCost = (filter == null ? 1. : filter.estimateSelectivity(selector))
                                              * selector.getNumRows()
                                              * dimsToSearch.size();

        log.debug(
            "Use-index strategy cost: %f, cursor-only strategy cost: %f",
            useIndexStrategyCost,
            cursorOnlyStrategyCost
        );

        if (useIndexStrategyCost < cursorOnlyStrategyCost) {
          log.debug("Use-index execution strategy is selected, query id [%s]", query.getId());
          return UseIndexesStrategy.of(query).getExecutionPlan(query, segment);
        } else {
          log.debug("Cursor-only execution strategy is selected, query id [%s]", query.getId());
          return CursorOnlyStrategy.of(query).getExecutionPlan(query, segment);
        }
      } else {
        log.debug(
            "Filter doesn't support bitmap index. Fall back to cursor-only execution strategy, query id [%s]",
            query.getId()
        );
        return CursorOnlyStrategy.of(query).getExecutionPlan(query, segment);
      }

    } else {
      log.debug("Index doesn't exist. Fall back to cursor-only execution strategy, query id [%s]", query.getId());
      return CursorOnlyStrategy.of(query).getExecutionPlan(query, segment);
    }
  }

  private static long computeTotalCard(final QueryableIndex index, final Iterable<DimensionSpec> dimensionSpecs)
  {
    long totalCard = 0;
    for (DimensionSpec dimension : dimensionSpecs) {
      final ColumnHolder columnHolder = index.getColumnHolder(dimension.getDimension());
      if (columnHolder != null) {
        final BitmapIndex bitmapIndex = columnHolder.getBitmapIndex();
        if (bitmapIndex != null) {
          totalCard += bitmapIndex.getCardinality();
        }
      }
    }
    return totalCard;
  }
}
