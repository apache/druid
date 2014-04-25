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

package io.druid.query.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseQuery<Result<TimeseriesResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  @JsonCreator
  public TimeseriesQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;

    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return Query.TIMESERIES;
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

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  public TimeseriesQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        querySegmentSpec,
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TimeseriesQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        computeOverridenContext(contextOverrides)
    );
  }

  @Override
  public String toString()
  {
    return "TimeseriesQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", dimFilter=" + dimFilter +
        ", granularity='" + granularity + '\'' +
        ", aggregatorSpecs=" + aggregatorSpecs +
        ", postAggregatorSpecs=" + postAggregatorSpecs +
        ", context=" + getContext() +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TimeseriesQuery that = (TimeseriesQuery) o;

    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(that.aggregatorSpecs) : that.aggregatorSpecs != null)
      return false;
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(that.postAggregatorSpecs) : that.postAggregatorSpecs != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    return result;
  }
}
