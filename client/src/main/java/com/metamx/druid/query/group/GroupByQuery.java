/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.group;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.Queries;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.group.limit.DefaultLimitSpec;
import com.metamx.druid.query.group.limit.LimitSpec;
import com.metamx.druid.query.group.limit.OrderByColumnSpec;
import com.metamx.druid.query.segment.LegacySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQuery extends BaseQuery<Row>
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final LimitSpec limitSpec;
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  private final Function<Sequence<Row>, Sequence<Row>> orderByLimitFn;

  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.limitSpec = (limitSpec == null) ? new DefaultLimitSpec() : limitSpec;
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);

    orderByLimitFn = this.limitSpec.build(this.dimensions, this.aggregatorSpecs, this.postAggregatorSpecs);
  }

  @JsonProperty
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
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

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return Query.GROUP_BY;
  }

  public Sequence<Row> applyLimit(Sequence<Row> results)
  {
    return orderByLimitFn.apply(results);
  }

  @Override
  public GroupByQuery withOverriddenContext(Map<String, String> contextOverride)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        limitSpec,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public GroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByQuery(
        getDataSource(),
        spec,
        limitSpec,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public static class Builder
  {
    private String dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private List<DimensionSpec> dimensions;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, String> context;

    private LimitSpec limitSpec = null;
    private List<OrderByColumnSpec> orderByColumnSpecs = Lists.newArrayList();
    private int limit = Integer.MAX_VALUE;

    private Builder() {}

    private Builder(Builder builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      limitSpec = builder.limitSpec;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      context = builder.context;
    }

    public Builder setDataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder setInterval(Object interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder limit(int limit)
    {
      ensureExplicitLimitNotSet();
      this.limit = limit;
      return this;
    }

    public Builder addOrderByColumn(String dimension)
    {
      return addOrderByColumn(dimension, (OrderByColumnSpec.Direction) null);
    }

    public Builder addOrderByColumn(String dimension, String direction)
    {
      return addOrderByColumn(dimension, OrderByColumnSpec.determineDirection(direction));
    }

    public Builder addOrderByColumn(String dimension, OrderByColumnSpec.Direction direction)
    {
      return addOrderByColumn(new OrderByColumnSpec(dimension, direction));
    }

    public Builder addOrderByColumn(OrderByColumnSpec columnSpec)
    {
      ensureExplicitLimitNotSet();
      this.orderByColumnSpecs.add(columnSpec);
      return this;
    }

    public Builder setLimitSpec(LimitSpec limitSpec)
    {
      ensureFluentLimitsNotSet();
      this.limitSpec = limitSpec;
      return this;
    }

    private void ensureExplicitLimitNotSet()
    {
      if (limitSpec != null) {
        throw new ISE("Ambiguous build, limitSpec[%s] already set", limitSpec);
      }
    }

    private void ensureFluentLimitsNotSet()
    {
      if (! (limit == Integer.MAX_VALUE && orderByColumnSpecs.isEmpty()) ) {
        throw new ISE("Ambiguous build, limit[%s] or columnSpecs[%s] already set.", limit, orderByColumnSpecs);
      }
    }

    public Builder setQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
    {
      this.querySegmentSpec = querySegmentSpec;
      return this;
    }

    public Builder setDimFilter(DimFilter dimFilter)
    {
      this.dimFilter = dimFilter;
      return this;
    }

    public Builder setGranularity(QueryGranularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public Builder addDimension(String column)
    {
      return addDimension(column, column);
    }

    public Builder addDimension(String column, String outputName)
    {
      return addDimension(new DefaultDimensionSpec(column, outputName));
    }

    public Builder addDimension(DimensionSpec dimension)
    {
      if (dimensions == null) {
        dimensions = Lists.newArrayList();
      }

      dimensions.add(dimension);
      return this;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder addAggregator(AggregatorFactory aggregator)
    {
      if (aggregatorSpecs == null) {
        aggregatorSpecs = Lists.newArrayList();
      }

      aggregatorSpecs.add(aggregator);
      return this;
    }

    public Builder setAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
    {
      this.aggregatorSpecs = Lists.newArrayList(aggregatorSpecs);
      return this;
    }

    public Builder addPostAggregator(PostAggregator postAgg)
    {
      if (postAggregatorSpecs == null) {
        postAggregatorSpecs = Lists.newArrayList();
      }

      postAggregatorSpecs.add(postAgg);
      return this;
    }

    public Builder setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = Lists.newArrayList(postAggregatorSpecs);
      return this;
    }

    public Builder setContext(Map<String, String> context)
    {
      this.context = context;
      return this;
    }

    public Builder copy()
    {
      return new Builder(this);
    }

    public GroupByQuery build()
    {
      final LimitSpec theLimitSpec;
      if (limitSpec == null) {
        theLimitSpec = new DefaultLimitSpec(orderByColumnSpecs, limit);
      }
      else {
        theLimitSpec = limitSpec;
      }

      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          theLimitSpec,
          dimFilter,
          granularity,
          dimensions,
          aggregatorSpecs,
          postAggregatorSpecs,
          context
      );
    }
  }
}
