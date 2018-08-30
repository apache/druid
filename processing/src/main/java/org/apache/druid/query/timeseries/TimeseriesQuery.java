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

package org.apache.druid.query.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseQuery<Result<TimeseriesResultValue>>
{
  static final String CTX_GRAND_TOTAL = "grandTotal";
  public static final String SKIP_EMPTY_BUCKETS = "skipEmptyBuckets";

  private final VirtualColumns virtualColumns;
  private final DimFilter dimFilter;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final int limit;

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
      @JsonProperty("limit") int limit,
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
    this.limit = (limit == 0) ? Integer.MAX_VALUE : limit;
    Preconditions.checkArgument(this.limit > 0, "limit must be greater than 0");
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

  @JsonProperty("limit")
  public int getLimit()
  {
    return limit;
  }

  public boolean isGrandTotal()
  {
    return getContextBoolean(CTX_GRAND_TOTAL, false);
  }

  public boolean isSkipEmptyBuckets()
  {
    return getContextBoolean(SKIP_EMPTY_BUCKETS, false);
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
  public Query<Result<TimeseriesResultValue>> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    return Druids.TimeseriesQueryBuilder.copy(this).aggregators(optimizeAggs(optimizationContext)).build();
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

  private List<AggregatorFactory> optimizeAggs(PerSegmentQueryOptimizationContext optimizationContext)
  {
    List<AggregatorFactory> optimizedAggs = new ArrayList<>();
    for (AggregatorFactory aggregatorFactory : aggregatorSpecs) {
      optimizedAggs.add(aggregatorFactory.optimizeForSegment(optimizationContext));
    }
    return optimizedAggs;
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
           ", limit=" + limit +
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
    return limit == that.limit &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(dimFilter, that.dimFilter) &&
           Objects.equals(aggregatorSpecs, that.aggregatorSpecs) &&
           Objects.equals(postAggregatorSpecs, that.postAggregatorSpecs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), virtualColumns, dimFilter, aggregatorSpecs, postAggregatorSpecs, limit);
  }
}
