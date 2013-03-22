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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.having.HavingSpec;
import com.metamx.druid.query.order.OrderBySpec;
import com.metamx.druid.query.segment.LegacySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQuery extends BaseQuery<Row>
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final HavingSpec havingSpec;
  private final OrderBySpec orderBySpec;
  private final Integer limit;
  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("orderBy") OrderBySpec orderBySpec,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.orderBySpec = orderBySpec;
    this.limit = limit;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
    Preconditions.checkArgument(this.limit == null || this.limit.intValue() >= 0, "A given limit can't be negative");

    validateOrderBySpecFields(aggregatorSpecs, postAggregatorSpecs, orderBySpec);
  }

  // Ensures that each aggregation field in the give orderBy spec is a valid name in either aggregator specs or in post aggregator specs.
  private void validateOrderBySpecFields(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs, OrderBySpec orderBySpec)
  {
    Set<String> aggNameSet = getAggregationNames(aggregatorSpecs);

    Set<String> postAggNames = getPostAggregationNames(postAggregatorSpecs);

    if(orderBySpec != null) {
      for(String name: orderBySpec.getAggregations()) {
        Preconditions.checkArgument(aggNameSet.contains(name) || postAggNames.contains(name), String.format("The field '%s' for orderBy must be either an aggregation or a post-aggregation", name));
      }
    }
  }

  private Set<String> getPostAggregationNames(List<PostAggregator> postAggregatorSpecs)
  {
    if(postAggregatorSpecs == null){
      return ImmutableSet.of();
    }

    return Sets.newHashSet(
      Iterables.transform(
        postAggregatorSpecs,
        new Function<PostAggregator, String>()
        {
          @Override
          public String apply(@Nullable PostAggregator postAgg)
          {
            return postAgg.getName();
          }
        }
      )
    );
  }

  private Set<String> getAggregationNames(List<AggregatorFactory> aggregatorSpecs)
  {
    if(aggregatorSpecs == null) {
      return ImmutableSet.of();
    }

    return Sets.newHashSet(
      Iterables.transform(
        aggregatorSpecs,
        new Function<AggregatorFactory, String>()
        {

          @Override
          public String apply(@Nullable AggregatorFactory agg)
          {
            return agg.getName();
          }
        }
      )
    );
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

  @JsonProperty("having")
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @JsonProperty("orderBy")
  public OrderBySpec getOrderBy()
  {
    return orderBySpec;
  }

  @JsonProperty("limit")
  public Integer getLimit()
  {
    return limit;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return "groupBy";
  }

  @Override
  public Query withOverriddenContext(Map<String, String> contextOverride)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        orderBySpec,
        limit,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByQuery(
        getDataSource(),
        spec,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        orderBySpec,
        limit,
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
    private HavingSpec havingSpec;
    private OrderBySpec orderBySpec;
    private Integer limit;

    private Map<String, String> context;

    private Builder() {}

    private Builder(Builder builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      havingSpec = builder.havingSpec;
      orderBySpec = builder.orderBySpec;
      limit = builder.limit;

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

    public Builder setHavingSpec(HavingSpec havingSpec)
    {
      this.havingSpec = havingSpec;

      return this;
    }

    public Builder setOrderBySpec(OrderBySpec orderBySpec)
    {
      this.orderBySpec = orderBySpec;

      return this;
    }

    public Builder setLimit(Integer limit)
    {
      this.limit = limit;

      return this;
    }

    public Builder copy()
    {
      return new Builder(this);
    }

    public GroupByQuery build()
    {
      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          orderBySpec,
          limit,
          context
      );
    }
  }
}
