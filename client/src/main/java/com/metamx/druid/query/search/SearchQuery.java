/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchQuery extends BaseQuery<Result<SearchResultValue>>
{
  private final DimFilter dimFilter;
  private final SearchSortSpec sortSpec;
  private final QueryGranularity granularity;
  private final List<String> dimensions;
  private final SearchQuerySpec querySpec;
  private final int limit;

  @JsonCreator
  public SearchQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("searchDimensions") List<String> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("sort") SearchSortSpec sortSpec,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.sortSpec = sortSpec;
    this.granularity = granularity == null ? QueryGranularity.ALL : granularity;
    this.limit = (limit == 0) ? 1000 : limit;
    this.dimensions = (dimensions == null) ? null : Lists.transform(
        dimensions,
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input;
          }
        }
    );
    this.querySpec = querySpec;

    Preconditions.checkNotNull(querySegmentSpec, "Must specify an interval");
    Preconditions.checkNotNull(querySpec, "Must specify a query");
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return Query.SEARCH;
  }

  @Override
  public SearchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        limit,
        spec,
        dimensions,
        querySpec,
        sortSpec,
        getContext()
    );
  }

  @Override
  public SearchQuery withOverriddenContext(Map<String, String> contextOverrides)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        dimensions,
        querySpec,
        sortSpec,
        computeOverridenContext(contextOverrides)
    );
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty("searchDimensions")
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("query")
  public SearchQuerySpec getQuery()
  {
    return querySpec;
  }

  @JsonProperty("sort")
  public SearchSortSpec getSort()
  {
    return sortSpec == null ? querySpec.getSearchSortSpec() : sortSpec;
  }

  public SearchQuery withLimit(int newLimit)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        newLimit,
        getQuerySegmentSpec(),
        dimensions,
        querySpec,
        sortSpec,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SearchQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimFilter=" + dimFilter +
           ", granularity='" + granularity + '\'' +
           ", dimensions=" + dimensions +
           ", querySpec=" + querySpec +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limit=" + limit +
           '}';
  }
}
