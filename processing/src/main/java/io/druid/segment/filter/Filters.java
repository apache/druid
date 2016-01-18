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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.InDimFilter;
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
  public static List<Filter> convertDimensionFilters(List<DimFilter> filters)
  {
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
          extractionDimFilter.getExtractionFn()
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
    } else if (dimFilter instanceof InDimFilter) {
      final InDimFilter inDimFilter = (InDimFilter) dimFilter;
      final List<Filter> listFilters = Lists.transform(
          inDimFilter.getValues(), new Function<String, Filter>()
          {
            @Nullable
            @Override
            public Filter apply(@Nullable String input)
            {
              return new SelectorFilter(inDimFilter.getDimension(), input);
            }
          }
      );

      filter = new OrFilter(listFilters);
    } else if (dimFilter instanceof BoundDimFilter) {
      filter = new BoundFilter((BoundDimFilter) dimFilter);
    }

    return filter;
  }
}
