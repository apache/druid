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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("select")
public class SelectQuery extends BaseQuery<Result<SelectResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final PagingSpec pagingSpec;

  @JsonCreator
  public SelectQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("pagingSpec") PagingSpec pagingSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.pagingSpec = pagingSpec;

    Preconditions.checkNotNull(pagingSpec, "must specify a pagingSpec");
    Preconditions.checkArgument(checkPagingSpec(pagingSpec, descending), "invalid pagingSpec");
  }

  private boolean checkPagingSpec(PagingSpec pagingSpec, boolean descending)
  {
    for (Integer value : pagingSpec.getPagingIdentifiers().values()) {
      if (descending ^ (value < 0)) {
        return false;
      }
    }
    return pagingSpec.getThreshold() >= 0;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return Query.SELECT;
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
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public PagingSpec getPagingSpec()
  {
    return pagingSpec;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  public PagingOffset getPagingOffset(String identifier)
  {
    return pagingSpec.getOffset(identifier, isDescending());
  }

  public SelectQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new SelectQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        getContext()
    );
  }

  @Override
  public Query<Result<SelectResultValue>> withDataSource(DataSource dataSource)
  {
    return new SelectQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        getContext()
    );
  }

  public SelectQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        computeOverridenContext(contextOverrides)
    );
  }

  public SelectQuery withPagingSpec(PagingSpec pagingSpec)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        getContext()
    );
  }

  public SelectQuery withDimFilter(DimFilter dimFilter)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SelectQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", descending=" + isDescending() +
           ", dimFilter=" + dimFilter +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", pagingSpec=" + pagingSpec +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SelectQuery that = (SelectQuery) o;

    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) return false;
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
    if (pagingSpec != null ? !pagingSpec.equals(that.pagingSpec) : that.pagingSpec != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    result = 31 * result + (pagingSpec != null ? pagingSpec.hashCode() : 0);
    return result;
  }
}
