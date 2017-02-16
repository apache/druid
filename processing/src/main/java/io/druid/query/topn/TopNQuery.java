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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

/**
 */
public class TopNQuery extends BaseQuery<Result<TopNResultValue>>
{
  public static final String TOPN = "topN";

  private final VirtualColumns virtualColumns;
  private final DimensionSpec dimensionSpec;
  private final TopNMetricSpec topNMetricSpec;
  private final int threshold;
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  @JsonCreator
  public TopNQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("dimension") DimensionSpec dimensionSpec,
      @JsonProperty("metric") TopNMetricSpec topNMetricSpec,
      @JsonProperty("threshold") int threshold,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.dimensionSpec = dimensionSpec;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;

    Preconditions.checkNotNull(dimensionSpec, "dimensionSpec can't be null");
    Preconditions.checkNotNull(topNMetricSpec, "must specify a metric");

    Preconditions.checkArgument(threshold != 0, "Threshold cannot be equal to 0.");
    topNMetricSpec.verifyPreconditions(this.aggregatorSpecs, this.postAggregatorSpecs);

    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);
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
    return TOPN;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty("dimension")
  public DimensionSpec getDimensionSpec()
  {
    return dimensionSpec;
  }

  @JsonProperty("metric")
  public TopNMetricSpec getTopNMetricSpec()
  {
    return topNMetricSpec;
  }

  @JsonProperty("threshold")
  public int getThreshold()
  {
    return threshold;
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

  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    if (dimensionSpec.getExtractionFn() != null) {
      selector.setHasExtractionFn(true);
    }
    topNMetricSpec.initTopNAlgorithmSelector(selector);
  }

  public TopNQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        querySegmentSpec,
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TopNQuery withDimensionSpec(DimensionSpec spec)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        spec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TopNQuery withAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TopNQuery withPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  @Override
  public Query<Result<TopNResultValue>> withDataSource(DataSource dataSource)
  {
    return new TopNQuery(
        dataSource,
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TopNQuery withThreshold(int threshold)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public TopNQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        computeOverridenContext(contextOverrides)
    );
  }

  public TopNQuery withDimFilter(DimFilter dimFilter)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "TopNQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimensionSpec=" + dimensionSpec +
           ", topNMetricSpec=" + topNMetricSpec +
           ", threshold=" + threshold +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", virtualColumns=" + virtualColumns +
           ", dimFilter=" + dimFilter +
           ", granularity='" + granularity + '\'' +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
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

    TopNQuery query = (TopNQuery) o;

    if (threshold != query.threshold) {
      return false;
    }
    if (!virtualColumns.equals(query.virtualColumns)) {
      return false;
    }
    if (dimensionSpec != null ? !dimensionSpec.equals(query.dimensionSpec) : query.dimensionSpec != null) {
      return false;
    }
    if (topNMetricSpec != null ? !topNMetricSpec.equals(query.topNMetricSpec) : query.topNMetricSpec != null) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(query.dimFilter) : query.dimFilter != null) {
      return false;
    }
    if (granularity != null ? !granularity.equals(query.granularity) : query.granularity != null) {
      return false;
    }
    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(query.aggregatorSpecs) : query.aggregatorSpecs != null) {
      return false;
    }
    return postAggregatorSpecs != null
           ? postAggregatorSpecs.equals(query.postAggregatorSpecs)
           : query.postAggregatorSpecs == null;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + virtualColumns.hashCode();
    result = 31 * result + (dimensionSpec != null ? dimensionSpec.hashCode() : 0);
    result = 31 * result + (topNMetricSpec != null ? topNMetricSpec.hashCode() : 0);
    result = 31 * result + threshold;
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    return result;
  }
}
