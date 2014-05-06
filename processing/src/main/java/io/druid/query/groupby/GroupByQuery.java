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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;

import javax.annotation.Nullable;
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
  private final HavingSpec havingSpec;
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  private final Function<Sequence<Row>, Sequence<Row>> orderByLimitFn;

  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("orderBy") LimitSpec orderBySpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = (limitSpec == null) ? (orderBySpec == null ? new NoopLimitSpec() : orderBySpec) : limitSpec;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);

    Function<Sequence<Row>, Sequence<Row>> postProcFn =
        this.limitSpec.build(this.dimensions, this.aggregatorSpecs, this.postAggregatorSpecs);

    if (havingSpec != null) {
      postProcFn = Functions.compose(
          new Function<Sequence<Row>, Sequence<Row>>()
          {
            @Override
            public Sequence<Row> apply(@Nullable Sequence<Row> input)
            {
              return Sequences.filter(
                  input,
                  new Predicate<Row>()
                  {
                    @Override
                    public boolean apply(@Nullable Row input)
                    {
                      return GroupByQuery.this.havingSpec.eval(input);
                    }
                  }
              );
            }
          },
          postProcFn
      );
    }

    orderByLimitFn = postProcFn;
  }

  /**
   * A private constructor that avoids all of the various state checks.  Used by the with*() methods where the checks
   * have already passed in order for the object to exist.
   */
  private GroupByQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      DimFilter dimFilter,
      QueryGranularity granularity,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregatorSpecs,
      List<PostAggregator> postAggregatorSpecs,
      HavingSpec havingSpec,
      LimitSpec orderBySpec,
      Function<Sequence<Row>, Sequence<Row>> orderByLimitFn,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = orderBySpec;
    this.orderByLimitFn = orderByLimitFn;
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

  @JsonProperty
  public LimitSpec getOrderBy()
  {
    return limitSpec;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return GROUP_BY;
  }

  public Sequence<Row> applyLimit(Sequence<Row> results)
  {
    return orderByLimitFn.apply(results);
  }

  @Override
  public GroupByQuery withOverriddenContext(Map<String, Object> contextOverride)
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
        limitSpec,
        orderByLimitFn,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public GroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
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
        limitSpec,
        orderByLimitFn,
        getContext()
    );
  }

  public static class Builder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private List<DimensionSpec> dimensions;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private HavingSpec havingSpec;

    private Map<String, Object> context;

    private LimitSpec limitSpec = null;
    private List<OrderByColumnSpec> orderByColumnSpecs = Lists.newArrayList();
    private int limit = Integer.MAX_VALUE;

    private Builder()
    {
    }

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
      havingSpec = builder.havingSpec;
      limit = builder.limit;

      context = builder.context;
    }

    public Builder setDataSource(DataSource dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder setDataSource(String dataSource)
    {
      this.dataSource = new TableDataSource(dataSource);
      return this;
    }

    public Builder setDataSource(Query query)
    {
      this.dataSource = new QueryDataSource(query);
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
      if (!(limit == Integer.MAX_VALUE && orderByColumnSpecs.isEmpty())) {
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

    public Builder setContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder setHavingSpec(HavingSpec havingSpec)
    {
      this.havingSpec = havingSpec;

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
      final LimitSpec theLimitSpec;
      if (limitSpec == null) {
        theLimitSpec = new DefaultLimitSpec(orderByColumnSpecs, limit);
      } else {
        theLimitSpec = limitSpec;
      }

      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          null,
          theLimitSpec,
          context
      );
    }
  }

  @Override
  public String toString()
  {
    return "GroupByQuery{" +
        "limitSpec=" + limitSpec +
        ", dimFilter=" + dimFilter +
        ", granularity=" + granularity +
        ", dimensions=" + dimensions +
        ", aggregatorSpecs=" + aggregatorSpecs +
        ", postAggregatorSpecs=" + postAggregatorSpecs +
        ", orderByLimitFn=" + orderByLimitFn +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GroupByQuery that = (GroupByQuery) o;

    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(that.aggregatorSpecs) : that.aggregatorSpecs != null)
      return false;
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) return false;
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) return false;
    if (limitSpec != null ? !limitSpec.equals(that.limitSpec) : that.limitSpec != null) return false;
    if (orderByLimitFn != null ? !orderByLimitFn.equals(that.orderByLimitFn) : that.orderByLimitFn != null)
      return false;
    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(that.postAggregatorSpecs) : that.postAggregatorSpecs != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (havingSpec != null ? havingSpec.hashCode() : 0);
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (orderByLimitFn != null ? orderByLimitFn.hashCode() : 0);
    return result;
  }
}
