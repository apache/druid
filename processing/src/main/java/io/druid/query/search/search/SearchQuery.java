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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.SearchResultValue;
import io.druid.query.spec.QuerySegmentSpec;

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
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("searchDimensions") List<String> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("sort") SearchSortSpec sortSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.sortSpec = sortSpec == null ? new LexicographicSearchSortSpec() : sortSpec;
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
  public SearchQuery withOverriddenContext(Map<String, Object> contextOverrides)
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
    return sortSpec;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SearchQuery that = (SearchQuery) o;

    if (limit != that.limit) return false;
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) return false;
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (querySpec != null ? !querySpec.equals(that.querySpec) : that.querySpec != null) return false;
    if (sortSpec != null ? !sortSpec.equals(that.sortSpec) : that.sortSpec != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (sortSpec != null ? sortSpec.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (querySpec != null ? querySpec.hashCode() : 0);
    result = 31 * result + limit;
    return result;
  }
}
