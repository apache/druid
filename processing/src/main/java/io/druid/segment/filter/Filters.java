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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.SpatialDimFilter;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class Filters
{
  public static List<Filter> convertDimensionFilters(List<DimFilter> filters){
    return Lists.transform(
        filters,
        new Function<DimFilter, Filter>()
        {
          @Override
          public Filter apply(@Nullable DimFilter input)
          {
            return convertDimensionFilters(input);
          }
        }
    );
  }

  public static Filter convertDimensionFilters(DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }

    Filter filter = null;
    if (dimFilter instanceof AndDimFilter) {
      filter = new AndFilter(convertDimensionFilters(((AndDimFilter) dimFilter).getFields()));
    } else if (dimFilter instanceof OrDimFilter) {
      filter = new OrFilter(convertDimensionFilters(((OrDimFilter) dimFilter).getFields()));
    } else if (dimFilter instanceof NotDimFilter) {
      filter = new NotFilter(convertDimensionFilters(((NotDimFilter) dimFilter).getField()));
    } else if (dimFilter instanceof SelectorDimFilter) {
      final SelectorDimFilter selectorDimFilter = (SelectorDimFilter) dimFilter;

      filter = new SelectorFilter(selectorDimFilter.getDimension(), selectorDimFilter.getValue());
    } else if (dimFilter instanceof ExtractionDimFilter) {
      final ExtractionDimFilter extractionDimFilter = (ExtractionDimFilter) dimFilter;

      filter = new ExtractionFilter(
          extractionDimFilter.getDimension(),
          extractionDimFilter.getValue(),
          extractionDimFilter.getDimExtractionFn()
      );
    } else if (dimFilter instanceof RegexDimFilter) {
      final RegexDimFilter regexDimFilter = (RegexDimFilter) dimFilter;

      filter = new RegexFilter(regexDimFilter.getDimension(), regexDimFilter.getPattern());
    } else if (dimFilter instanceof SearchQueryDimFilter) {
      final SearchQueryDimFilter searchQueryFilter = (SearchQueryDimFilter) dimFilter;

      filter = new SearchQueryFilter(searchQueryFilter.getDimension(), searchQueryFilter.getQuery());
    } else if (dimFilter instanceof JavaScriptDimFilter) {
      final JavaScriptDimFilter javaScriptDimFilter = (JavaScriptDimFilter) dimFilter;

      filter = new JavaScriptFilter(javaScriptDimFilter.getDimension(), javaScriptDimFilter.getFunction());
    } else if (dimFilter instanceof SpatialDimFilter) {
      final SpatialDimFilter spatialDimFilter = (SpatialDimFilter) dimFilter;

      filter = new SpatialFilter(spatialDimFilter.getDimension(), spatialDimFilter.getBound());
    }

    return filter;
  }
}
