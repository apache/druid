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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class TopNQuery extends BaseQuery<Result<TopNResultValue>>
{
  public static final String TOPN = "topN";

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
    super(dataSource, querySegmentSpec, context);
    this.dimensionSpec = dimensionSpec;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.aggregatorSpecs = aggregatorSpecs;
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
  public String getType()
  {
    return TOPN;
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
    if (dimensionSpec.getDimExtractionFn() != null) {
      selector.setHasDimExtractionFn(true);
    }
    topNMetricSpec.initTopNAlgorithmSelector(selector);
  }

  public TopNQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TopNQuery(
        getDataSource(),
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

  public TopNQuery withThreshold(int threshold)
  {
    return new TopNQuery(
        getDataSource(),
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

  @Override
  public String toString()
  {
    return "TopNQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimensionSpec=" + dimensionSpec +
           ", topNMetricSpec=" + topNMetricSpec +
           ", threshold=" + threshold +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", dimFilter=" + dimFilter +
           ", granularity='" + granularity + '\'' +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TopNQuery topNQuery = (TopNQuery) o;

    if (threshold != topNQuery.threshold) return false;
    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(topNQuery.aggregatorSpecs) : topNQuery.aggregatorSpecs != null)
      return false;
    if (dimFilter != null ? !dimFilter.equals(topNQuery.dimFilter) : topNQuery.dimFilter != null) return false;
    if (dimensionSpec != null ? !dimensionSpec.equals(topNQuery.dimensionSpec) : topNQuery.dimensionSpec != null)
      return false;
    if (granularity != null ? !granularity.equals(topNQuery.granularity) : topNQuery.granularity != null) return false;
    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(topNQuery.postAggregatorSpecs) : topNQuery.postAggregatorSpecs != null)
      return false;
    if (topNMetricSpec != null ? !topNMetricSpec.equals(topNQuery.topNMetricSpec) : topNQuery.topNMetricSpec != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
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
