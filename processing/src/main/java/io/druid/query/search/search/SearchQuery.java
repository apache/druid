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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.SearchResultValue;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class SearchQuery extends BaseQuery<Result<SearchResultValue>>
{
  private static final SearchSortSpec DEFAULT_SORT_SPEC = new SearchSortSpec(StringComparators.LEXICOGRAPHIC);

  private final DimFilter dimFilter;
  private final SearchSortSpec sortSpec;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions;
  private final SearchQuerySpec querySpec;
  private final int limit;

  @JsonCreator
  public SearchQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("searchDimensions") List<DimensionSpec> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("sort") SearchSortSpec sortSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    Preconditions.checkNotNull(querySegmentSpec, "Must specify an interval");

    this.dimFilter = dimFilter;
    this.sortSpec = sortSpec == null ? DEFAULT_SORT_SPEC : sortSpec;
    this.granularity = granularity == null ? Granularities.ALL : granularity;
    this.limit = (limit == 0) ? 1000 : limit;
    this.dimensions = dimensions;
    this.querySpec = querySpec == null ? new AllSearchQuerySpec() : querySpec;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return Query.SEARCH;
  }

  @Override
  public SearchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return Druids.SearchQueryBuilder.copy(this).intervals(spec).build();
  }

  @Override
  public Query<Result<SearchResultValue>> withDataSource(DataSource dataSource)
  {
    return Druids.SearchQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public SearchQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverrides);
    return Druids.SearchQueryBuilder.copy(this).context(newContext).build();
  }

  public SearchQuery withDimFilter(DimFilter dimFilter)
  {
    return Druids.SearchQueryBuilder.copy(this).filters(dimFilter).build();
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty("searchDimensions")
  public List<DimensionSpec> getDimensions()
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
    return Druids.SearchQueryBuilder.copy(this).limit(newLimit).build();
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    SearchQuery that = (SearchQuery) o;

    if (limit != that.limit) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) {
      return false;
    }
    if (querySpec != null ? !querySpec.equals(that.querySpec) : that.querySpec != null) {
      return false;
    }
    if (sortSpec != null ? !sortSpec.equals(that.sortSpec) : that.sortSpec != null) {
      return false;
    }

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
