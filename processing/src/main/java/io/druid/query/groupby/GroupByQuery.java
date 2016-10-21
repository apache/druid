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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
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
import org.joda.time.Interval;

import java.util.Comparator;
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

  private final Function<Sequence<Row>, Sequence<Row>> limitFn;

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
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    for (DimensionSpec spec : this.dimensions) {
      Preconditions.checkArgument(spec != null, "dimensions has null DimensionSpec");
    }
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = (limitSpec == null) ? new NoopLimitSpec() : limitSpec;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);

    Function<Sequence<Row>, Sequence<Row>> postProcFn =
        this.limitSpec.build(this.dimensions, this.aggregatorSpecs, this.postAggregatorSpecs);

    if (havingSpec != null) {
      postProcFn = Functions.compose(
          postProcFn,
          new Function<Sequence<Row>, Sequence<Row>>()
          {
            @Override
            public Sequence<Row> apply(Sequence<Row> input)
            {
              return Sequences.filter(
                  input,
                  new Predicate<Row>()
                  {
                    @Override
                    public boolean apply(Row input)
                    {
                      return GroupByQuery.this.havingSpec.eval(input);
                    }
                  }
              );
            }
          }
      );
    }

    limitFn = postProcFn;
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
      Function<Sequence<Row>, Sequence<Row>> limitFn,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = orderBySpec;
    this.limitFn = limitFn;
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
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
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
    return GROUP_BY;
  }

  @Override
  public Ordering getResultOrdering()
  {
    final Comparator naturalNullsFirst = Ordering.natural().nullsFirst();
    final Ordering<Row> rowOrdering = getRowOrdering(false);

    return Ordering.from(
        new Comparator<Object>()
        {
          @Override
          public int compare(Object lhs, Object rhs)
          {
            if (lhs instanceof Row) {
              return rowOrdering.compare((Row) lhs, (Row) rhs);
            } else {
              // Probably bySegment queries
              return naturalNullsFirst.compare(lhs, rhs);
            }
          }
        }
    );
  }

  public Ordering<Row> getRowOrdering(final boolean granular)
  {
    final Comparator naturalNullsFirst = Ordering.natural().nullsFirst();

    return Ordering.from(
        new Comparator<Row>()
        {
          @Override
          public int compare(Row lhs, Row rhs)
          {
            final int timeCompare;

            if (granular) {
              timeCompare = Longs.compare(
                  granularity.truncate(lhs.getTimestampFromEpoch()),
                  granularity.truncate(rhs.getTimestampFromEpoch())
              );
            } else {
              timeCompare = Longs.compare(
                  lhs.getTimestampFromEpoch(),
                  rhs.getTimestampFromEpoch()
              );
            }

            if (timeCompare != 0) {
              return timeCompare;
            }

            for (DimensionSpec dimension : dimensions) {
              final int dimCompare = naturalNullsFirst.compare(
                  lhs.getRaw(dimension.getOutputName()),
                  rhs.getRaw(dimension.getOutputName())
              );
              if (dimCompare != 0) {
                return dimCompare;
              }
            }

            return 0;
          }
        }
    );
  }

  public Sequence<Row> applyLimit(Sequence<Row> results)
  {
    return limitFn.apply(results);
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
        limitFn,
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
        limitFn,
        getContext()
    );
  }

  public GroupByQuery withDimFilter(final DimFilter dimFilter)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        getGranularity(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        limitFn,
        getContext()
    );
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new GroupByQuery(
        dataSource,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        limitFn,
        getContext()
    );
  }

  public GroupByQuery withDimensionSpecs(final List<DimensionSpec> dimensionSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        dimensionSpecs,
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        limitFn,
        getContext()
    );
  }

  public GroupByQuery withLimitSpec(final LimitSpec limitSpec)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        limitSpec,
        getContext()
    );
  }

  public GroupByQuery withAggregatorSpecs(final List<AggregatorFactory> aggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        aggregatorSpecs,
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        limitFn,
        getContext()
    );
  }

  public GroupByQuery withPostAggregatorSpecs(final List<PostAggregator> postAggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getAggregatorSpecs(),
        postAggregatorSpecs,
        getHavingSpec(),
        getLimitSpec(),
        limitFn,
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

    public Builder()
    {
    }

    public Builder(GroupByQuery query)
    {
      dataSource = query.getDataSource();
      querySegmentSpec = query.getQuerySegmentSpec();
      limitSpec = query.getLimitSpec();
      dimFilter = query.getDimFilter();
      granularity = query.getGranularity();
      dimensions = query.getDimensions();
      aggregatorSpecs = query.getAggregatorSpecs();
      postAggregatorSpecs = query.getPostAggregatorSpecs();
      havingSpec = query.getHavingSpec();
      context = query.getContext();
    }

    public Builder(Builder builder)
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

    public Builder setInterval(QuerySegmentSpec interval)
    {
      return setQuerySegmentSpec(interval);
    }

    public Builder setInterval(List<Interval> intervals)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(intervals));
    }

    public Builder setInterval(Interval interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder setInterval(String interval)
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
        if (orderByColumnSpecs.isEmpty() && limit == Integer.MAX_VALUE) {
          theLimitSpec = new NoopLimitSpec();
        } else {
          theLimitSpec = new DefaultLimitSpec(orderByColumnSpecs, limit);
        }
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
          theLimitSpec,
          context
      );
    }
  }

  @Override
  public String toString()
  {
    return "GroupByQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limitSpec=" + limitSpec +
           ", dimFilter=" + dimFilter +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
           ", havingSpec=" + havingSpec +
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

    GroupByQuery that = (GroupByQuery) o;

    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(that.aggregatorSpecs) : that.aggregatorSpecs != null) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) {
      return false;
    }
    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }
    if (limitSpec != null ? !limitSpec.equals(that.limitSpec) : that.limitSpec != null) {
      return false;
    }
    if (postAggregatorSpecs != null
        ? !postAggregatorSpecs.equals(that.postAggregatorSpecs)
        : that.postAggregatorSpecs != null) {
      return false;
    }

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
    return result;
  }
}
