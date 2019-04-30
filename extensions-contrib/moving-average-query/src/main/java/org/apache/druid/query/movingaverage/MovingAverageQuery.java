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

package org.apache.druid.query.movingaverage;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.movingaverage.averagers.AveragerFactory;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that defines druid MovingAverage query fields
 */
@JsonTypeName("movingAverage")
public class MovingAverageQuery extends BaseQuery<Row>
{

  public static final String MOVING_AVG_QUERY_TYPE = "movingAverage";
  public static final String CTX_KEY_SORT_BY_DIMS_FIRST = "sortByDimsFirst";

  private final LimitSpec limitSpec;
  private final HavingSpec havingSpec;
  private final DimFilter dimFilter;
  private final Function<Sequence<Row>, Sequence<Row>> limitFn;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final List<AveragerFactory<?, ?>> averagerSpecs;
  private final List<PostAggregator> postAveragerSpecs;

  @JsonCreator
  public MovingAverageQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("averagers") List<AveragerFactory<?, ?>> averagerSpecs,
      @JsonProperty("postAveragers") List<PostAggregator> postAveragerSpecs,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);

    //TBD: Implement null awareness to respect the contract of this flag.
    Preconditions.checkArgument(NullHandling.replaceWithDefault(), "movingAverage does not support druid.generic.useDefaultValueForNull=false");

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.of() : dimensions;
    for (DimensionSpec spec : this.dimensions) {
      Preconditions.checkArgument(spec != null, "dimensions has null DimensionSpec");
    }
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.of() : postAggregatorSpecs;
    this.averagerSpecs = averagerSpecs == null ? ImmutableList.of() : averagerSpecs;
    this.postAveragerSpecs = postAveragerSpecs == null ? ImmutableList.of() : postAveragerSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = (limitSpec == null) ? NoopLimitSpec.INSTANCE : limitSpec;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");

    verifyOutputNames(this.dimensions, this.aggregatorSpecs, this.postAggregatorSpecs);

    // build combined list of aggregators and averagers so that limit spec building is happy
    List<AggregatorFactory> combinedAggregatorSpecs = new ArrayList<>();
    combinedAggregatorSpecs.addAll(this.aggregatorSpecs);
    for (AveragerFactory<?, ?> avg : this.averagerSpecs) {
      combinedAggregatorSpecs.add(new AveragerFactoryWrapper(avg, ""));
    }

    Function<Sequence<Row>, Sequence<Row>> postProcFn =
        this.limitSpec.build(
            this.dimensions,
            combinedAggregatorSpecs,
            this.postAggregatorSpecs,
            this.granularity,
            getContextSortByDimsFirst()
        );

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
                      return MovingAverageQuery.this.havingSpec.eval(input);
                    }
                  }
              );
            }
          }
      );
    }

    this.limitFn = postProcFn;

  }

  private static void verifyOutputNames(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregators,
      List<PostAggregator> postAggregators
  )
  {

    final Set<String> outputNames = new HashSet<>();
    for (DimensionSpec dimension : dimensions) {
      if (!outputNames.add(dimension.getOutputName())) {
        throw new IAE("Duplicate output name[%s]", dimension.getOutputName());
      }
    }

    for (AggregatorFactory aggregator : aggregators) {
      if (!outputNames.add(aggregator.getName())) {
        throw new IAE("Duplicate output name[%s]", aggregator.getName());
      }
    }

    for (PostAggregator postAggregator : postAggregators) {
      if (!outputNames.add(postAggregator.getName())) {
        throw new IAE("Duplicate output name[%s]", postAggregator.getName());
      }
    }
  }

  /**
   * A private constructor that avoids all of the various state checks.  Used by the with*() methods where the checks
   * have already passed in order for the object to exist.
   */
  private MovingAverageQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      DimFilter dimFilter,
      Granularity granularity,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggregatorSpecs,
      List<AveragerFactory<?, ?>> averagerSpecs,
      List<PostAggregator> postAggregatorSpecs,
      List<PostAggregator> postAveragerSpecs,
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
    this.averagerSpecs = averagerSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs;
    this.postAveragerSpecs = postAveragerSpecs;
    this.havingSpec = havingSpec;
    this.limitSpec = orderBySpec;
    this.limitFn = limitFn;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return MOVING_AVG_QUERY_TYPE;
  }

  @JsonIgnore
  public boolean getContextSortByDimsFirst()
  {
    return getContextBoolean(CTX_KEY_SORT_BY_DIMS_FIRST, false);
  }

  @Override
  @JsonProperty
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  @JsonProperty
  public Granularity getGranularity()
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

  @JsonProperty("averagers")
  public List<AveragerFactory<?, ?>> getAveragerSpecs()
  {
    return averagerSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty("postAveragers")
  public List<PostAggregator> getPostAveragerSpecs()
  {
    return postAveragerSpecs;
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
  public MovingAverageQuery withOverriddenContext(Map contextOverride)
  {
    return new MovingAverageQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        averagerSpecs,
        postAggregatorSpecs,
        postAveragerSpecs,
        havingSpec,
        limitSpec,
        limitFn,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public MovingAverageQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new MovingAverageQuery(
        getDataSource(),
        spec,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        averagerSpecs,
        postAggregatorSpecs,
        postAveragerSpecs,
        havingSpec,
        limitSpec,
        limitFn,
        getContext()
    );
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new MovingAverageQuery(
        dataSource,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        averagerSpecs,
        postAggregatorSpecs,
        postAveragerSpecs,
        havingSpec,
        limitSpec,
        limitFn,
        getContext()
    );
  }

  public Query<Row> withPostAveragers(List<PostAggregator> postAveragerSpecs)
  {
    return new MovingAverageQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        averagerSpecs,
        postAggregatorSpecs,
        postAveragerSpecs,
        havingSpec,
        limitSpec,
        limitFn,
        getContext()
    );
  }

  public Sequence<Row> applyLimit(Sequence<Row> results)
  {
    return limitFn.apply(results);
  }
}
