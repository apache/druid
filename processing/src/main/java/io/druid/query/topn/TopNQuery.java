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
import io.druid.java.util.common.granularity.Granularity;
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
import java.util.Objects;

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
  private final Granularity granularity;
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
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);

    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.dimensionSpec = dimensionSpec;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = Queries.prepareAggregations(
        ImmutableList.of(dimensionSpec.getOutputName()),
        this.aggregatorSpecs,
        postAggregatorSpecs == null
            ? ImmutableList.<PostAggregator>of()
            : postAggregatorSpecs
    );

    Preconditions.checkNotNull(dimensionSpec, "dimensionSpec can't be null");
    Preconditions.checkNotNull(topNMetricSpec, "must specify a metric");

    Preconditions.checkArgument(threshold != 0, "Threshold cannot be equal to 0.");
    topNMetricSpec.verifyPreconditions(this.aggregatorSpecs, this.postAggregatorSpecs);
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
  public Granularity getGranularity()
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

  @Override
  public TopNQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TopNQueryBuilder(this).intervals(querySegmentSpec).build();
  }

  public TopNQuery withDimensionSpec(DimensionSpec spec)
  {
    return new TopNQueryBuilder(this).dimension(spec).build();
  }

  public TopNQuery withAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
  {
    return new TopNQueryBuilder(this).aggregators(aggregatorSpecs).build();
  }

  public TopNQuery withPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
  {
    return new TopNQueryBuilder(this).postAggregators(postAggregatorSpecs).build();
  }

  @Override
  public Query<Result<TopNResultValue>> withDataSource(DataSource dataSource)
  {
    return new TopNQueryBuilder(this).dataSource(dataSource).build();
  }

  public TopNQuery withThreshold(int threshold)
  {
    return new TopNQueryBuilder(this).threshold(threshold).build();
  }

  @Override
  public TopNQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TopNQueryBuilder(this).context(computeOverriddenContext(getContext(), contextOverrides)).build();
  }

  public TopNQuery withDimFilter(DimFilter dimFilter)
  {
    return new TopNQueryBuilder(this).filters(dimFilter).build();
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
    final TopNQuery topNQuery = (TopNQuery) o;
    return threshold == topNQuery.threshold &&
        Objects.equals(virtualColumns, topNQuery.virtualColumns) &&
        Objects.equals(dimensionSpec, topNQuery.dimensionSpec) &&
        Objects.equals(topNMetricSpec, topNQuery.topNMetricSpec) &&
        Objects.equals(dimFilter, topNQuery.dimFilter) &&
        Objects.equals(granularity, topNQuery.granularity) &&
        Objects.equals(aggregatorSpecs, topNQuery.aggregatorSpecs) &&
        Objects.equals(postAggregatorSpecs, topNQuery.postAggregatorSpecs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs
    );
  }
}
