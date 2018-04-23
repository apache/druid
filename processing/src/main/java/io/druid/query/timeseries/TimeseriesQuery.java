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

package io.druid.query.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseQuery<Result<TimeseriesResultValue>>
{
  static final String CTX_GRAND_TOTAL = "grandTotal";

  private final VirtualColumns virtualColumns;
  private final DimFilter dimFilter;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  @JsonCreator
  public TimeseriesQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context, granularity);

    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.dimFilter = dimFilter;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.of() : aggregatorSpecs;
    this.postAggregatorSpecs = Queries.prepareAggregations(
        ImmutableList.of(),
        this.aggregatorSpecs,
        postAggregatorSpecs == null ? ImmutableList.of() : postAggregatorSpecs
    );
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
    return Query.TIMESERIES;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
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

  public boolean isGrandTotal()
  {
    return getContextBoolean(CTX_GRAND_TOTAL, false);
  }

  public boolean isSkipEmptyBuckets()
  {
    return getContextBoolean("skipEmptyBuckets", false);
  }

  @Override
  public TimeseriesQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return Druids.TimeseriesQueryBuilder.copy(this).intervals(querySegmentSpec).build();
  }

  @Override
  public Query<Result<TimeseriesResultValue>> withDataSource(DataSource dataSource)
  {
    return Druids.TimeseriesQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public TimeseriesQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverrides);
    return Druids.TimeseriesQueryBuilder.copy(this).context(newContext).build();
  }

  public TimeseriesQuery withDimFilter(DimFilter dimFilter)
  {
    return Druids.TimeseriesQueryBuilder.copy(this).filters(dimFilter).build();
  }

  public TimeseriesQuery withPostAggregatorSpecs(final List<PostAggregator> postAggregatorSpecs)
  {
    return Druids.TimeseriesQueryBuilder.copy(this).postAggregators(postAggregatorSpecs).build();
  }

  @Override
  public String toString()
  {
    return "TimeseriesQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", descending=" + isDescending() +
           ", virtualColumns=" + virtualColumns +
           ", dimFilter=" + dimFilter +
           ", granularity='" + getGranularity() + '\'' +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
           ", context=" + getContext() +
           '}';
  }

  @Override
  public boolean equals(final Object o)
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
    final TimeseriesQuery that = (TimeseriesQuery) o;
    return Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(dimFilter, that.dimFilter) &&
           Objects.equals(aggregatorSpecs, that.aggregatorSpecs) &&
           Objects.equals(postAggregatorSpecs, that.postAggregatorSpecs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), virtualColumns, dimFilter, aggregatorSpecs, postAggregatorSpecs);
  }
}
