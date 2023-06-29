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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FilterAnalysis
{
  @SuppressWarnings("rawtypes")
  public static FilterAnalysis analyzeFilter(
      @Nullable final Filter filter,
      ColumnSelectorColumnIndexSelector indexSelector,
      @Nullable QueryMetrics queryMetrics,
      int numRows
  )
  {

    /*
     * Filters can be applied in two stages:
     * pre-filtering: Use bitmap indexes to prune the set of rows to be scanned.
     * post-filtering: Iterate through rows and apply the filter to the row values
     *
     * The pre-filter and post-filter step have an implicit AND relationship. (i.e., final rows are those that
     * were not pruned AND those that matched the filter during row scanning)
     *
     * An AND filter can have its subfilters partitioned across the two steps. The subfilters that can be
     * processed entirely with bitmap indexes (subfilter returns non-null value for getBitmapColumnIndex)
     * will be moved to the pre-filtering stage.
     *
     * Any subfilters that cannot be processed entirely with bitmap indexes will be moved to the post-filtering stage.
     */
    final List<Filter> preFilters;
    final List<Filter> postFilters = new ArrayList<>();
    int preFilteredRows = numRows;
    if (filter == null) {
      preFilters = Collections.emptyList();
    } else {
      preFilters = new ArrayList<>();

      if (filter instanceof AndFilter) {
        // If we get an AndFilter, we can split the subfilters across both filtering stages
        for (Filter subfilter : ((AndFilter) filter).getFilters()) {

          final BitmapColumnIndex columnIndex = subfilter.getBitmapColumnIndex(indexSelector);

          if (columnIndex == null) {
            postFilters.add(subfilter);
          } else {
            preFilters.add(subfilter);
            if (!columnIndex.getIndexCapabilities().isExact()) {
              postFilters.add(subfilter);
            }
          }
        }
      } else {
        // If we get an OrFilter or a single filter, handle the filter in one stage
        final BitmapColumnIndex columnIndex = filter.getBitmapColumnIndex(indexSelector);
        if (columnIndex == null) {
          postFilters.add(filter);
        } else {
          preFilters.add(filter);
          if (!columnIndex.getIndexCapabilities().isExact()) {
            postFilters.add(filter);
          }
        }
      }
    }

    final ImmutableBitmap preFilterBitmap;
    if (preFilters.isEmpty()) {
      preFilterBitmap = null;
    } else {
      if (queryMetrics != null) {
        BitmapResultFactory<?> bitmapResultFactory =
            queryMetrics.makeBitmapResultFactory(indexSelector.getBitmapFactory());
        long bitmapConstructionStartNs = System.nanoTime();
        // Use AndFilter.getBitmapIndex to intersect the preFilters to get its short-circuiting behavior.
        preFilterBitmap = AndFilter.getBitmapIndex(indexSelector, bitmapResultFactory, preFilters);
        preFilteredRows = preFilterBitmap.size();
        queryMetrics.reportBitmapConstructionTime(System.nanoTime() - bitmapConstructionStartNs);
      } else {
        BitmapResultFactory<?> bitmapResultFactory = new DefaultBitmapResultFactory(indexSelector.getBitmapFactory());
        preFilterBitmap = AndFilter.getBitmapIndex(indexSelector, bitmapResultFactory, preFilters);
      }
    }

    if (queryMetrics != null) {
      queryMetrics.preFilters(new ArrayList<>(preFilters));
      queryMetrics.postFilters(postFilters);
      queryMetrics.reportSegmentRows(numRows);
      queryMetrics.reportPreFilteredRows(preFilteredRows);
    }

    return new FilterAnalysis(preFilterBitmap, Filters.maybeAnd(postFilters).orElse(null));
  }

  private final Filter postFilter;
  private final ImmutableBitmap preFilterBitmap;

  public FilterAnalysis(
      @Nullable final ImmutableBitmap preFilterBitmap,
      @Nullable final Filter postFilter
  )
  {
    this.preFilterBitmap = preFilterBitmap;
    this.postFilter = postFilter;
  }

  @Nullable
  public ImmutableBitmap getPreFilterBitmap()
  {
    return preFilterBitmap;
  }

  @Nullable
  public Filter getPostFilter()
  {
    return postFilter;
  }
}
