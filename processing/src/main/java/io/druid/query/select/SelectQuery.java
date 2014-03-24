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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
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
  private final List<String> dimensions;
  private final List<String> metrics;
  private final PagingSpec pagingSpec;

  @JsonCreator
  public SelectQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("pagingSpec") PagingSpec pagingSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.pagingSpec = pagingSpec;
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
  public List<String> getDimensions()
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

  public SelectQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new SelectQuery(
        getDataSource(),
        querySegmentSpec,
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
        dimFilter,
        granularity,
        dimensions,
        metrics,
        pagingSpec,
        computeOverridenContext(contextOverrides)
    );
  }

  @Override
  public String toString()
  {
    return "SelectQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
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
