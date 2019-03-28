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

package org.apache.druid.query.lookbackquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class that defines the fields in a druid Lookback query
 */
@JsonTypeName("lookback")
public class LookbackQuery extends BaseQuery<Result<LookbackResultValue>>
{

  public static final String CTX_KEY_SORT_BY_DIMS_FIRST = "sortByDimsFirst";
  public static final String LOOKBACK_QUERY_TYPE = "lookback";
  public static final String DEFAULT_LOOKBACK_PREFIX = "lookback_";
  public static final QuerySegmentSpec DUMMY_INTERVAL = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.ETERNITY)
  );

  private final List<PostAggregator> postAggregatorSpecs;
  private final List<Period> lookbackOffsets;
  private final List<String> lookbackPrefixes;
  private final LimitSpec limitSpec;
  private final HavingSpec havingSpec;
  private final Function<Sequence<Row>, Sequence<Row>> postProcFn;

  @JsonCreator
  public LookbackQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("lookbackOffsets") List<Period> lookbackOffsets,
      @JsonProperty("lookbackPrefixes") List<String> lookbackPrefixes,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("limitSpec") LimitSpec limitSpec
  )
  {

    super(dataSource, DUMMY_INTERVAL, false, context);
    this.postAggregatorSpecs =
        postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.lookbackOffsets = lookbackOffsets;
    this.havingSpec = havingSpec;
    this.limitSpec = (limitSpec == null) ? NoopLimitSpec.instance() : limitSpec;
    this.lookbackPrefixes = (lookbackPrefixes != null)
                            ? lookbackPrefixes
                            : LookbackQueryHelper.buildLookbackPrefixList(lookbackOffsets);

    //validations
    LookbackQueryHelper.validateDatasource(dataSource);
    LookbackQueryHelper.validateLookbackOffsetsAndPeriods(lookbackOffsets, this.lookbackPrefixes);
    LookbackQueryHelper.validateQuerySegmentSpec(this);
    LookbackQueryHelper.validatePostaggNameUniqueness(this);

    Query<?> query = ((QueryDataSource) dataSource).getQuery();
    List<AggregatorFactory> combinedAggregatorSpecs = new ArrayList<>();
    List<PostAggregator> combinedPostAggSpecs = new ArrayList<>();
    // This is typed AggregatorFactory instead of AveragerFactory<?> because the common
    // groupBy code for sorting doesn't know about Averager's. So instead we wrap the averagers
    // in a wrapper that makes them look like aggregators for purposes of sorting, having, and limiting.

    combinedPostAggSpecs.addAll(this.postAggregatorSpecs);

    if (query instanceof TimeseriesQuery) {
      // create combined (inner and lookback) aggregator list
      List<AggregatorFactory> innerAggSpecs = ((TimeseriesQuery) query).getAggregatorSpecs();
      combinedAggregatorSpecs.addAll(innerAggSpecs);
      for (AggregatorFactory agg : innerAggSpecs) {
        // wrapper class clones inner agg and rename it to prefix + inner_agg_name
        for (String prefix : this.lookbackPrefixes) {
          combinedAggregatorSpecs.add(new AggregatorFactoryWrapper(agg, prefix));
        }
      }

      // create combined postAggregator list
      List<PostAggregator> innerPostAggSpecs = ((TimeseriesQuery) query).getPostAggregatorSpecs();
      combinedPostAggSpecs.addAll(innerPostAggSpecs);
      for (PostAggregator agg : innerPostAggSpecs) {
        for (String prefix : this.lookbackPrefixes) {
          combinedPostAggSpecs.add(new PostAggregatorWrapper(agg, prefix));
        }
      }
    } else if (query instanceof GroupByQuery) {
      List<AggregatorFactory> innerAggSpecs = ((GroupByQuery) query).getAggregatorSpecs();
      combinedAggregatorSpecs.addAll(innerAggSpecs);
      for (AggregatorFactory agg : innerAggSpecs) {
        for (String prefix : this.lookbackPrefixes) {
          combinedAggregatorSpecs.add(new AggregatorFactoryWrapper(agg, prefix));
        }
      }

      List<PostAggregator> innerPostAggSpecs = ((GroupByQuery) query).getPostAggregatorSpecs();
      combinedPostAggSpecs.addAll(innerPostAggSpecs);
      for (PostAggregator agg : innerPostAggSpecs) {
        for (String prefix : this.lookbackPrefixes) {
          combinedPostAggSpecs.add(new PostAggregatorWrapper(agg, prefix));
        }
      }
    }

    Function<Sequence<Row>, Sequence<Row>> postProcFunction =
        this.limitSpec.build(
            getDimensions(),
            combinedAggregatorSpecs,
            combinedPostAggSpecs,
            getGranularity(),
            getContextSortByDimsFirst()
        );
    if (havingSpec != null) {
      postProcFunction = Functions.compose(postProcFunction, new Function<Sequence<Row>, Sequence<Row>>()
      {
        @Override
        public Sequence<Row> apply(Sequence<Row> input)
        {
          return Sequences.filter(input, new Predicate<Row>()
          {
            @Override
            public boolean apply(Row input)
            {
              return LookbackQuery.this.havingSpec.eval(input);
            }
          });
        }
      });
    }
    postProcFn = postProcFunction;
  }

  public static LookbackQueryBuilder builder()
  {
    return new LookbackQueryBuilder();
  }

  public Sequence<Row> applyLimit(Sequence<Row> results)
  {
    return postProcFn.apply(results);
  }

  @JsonIgnore
  public boolean getContextSortByDimsFirst()
  {
    return getContextBoolean(CTX_KEY_SORT_BY_DIMS_FIRST, false);
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    Query<?> q = ((QueryDataSource) getDataSource()).getQuery();
    switch (q.getType()) {
      case Query.GROUP_BY:
        return ((GroupByQuery) q).getDimensions();
      default:
        return Collections.emptyList();
    }
  }

  @JsonProperty
  public List<Period> getLookbackOffsets()
  {
    return lookbackOffsets;
  }

  @JsonProperty
  public List<String> getLookbackPrefixes()
  {
    return lookbackPrefixes;
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
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return LOOKBACK_QUERY_TYPE;
  }

  /**
   * Creates a LookbackQuery using the provided context
   *
   * @param contextOverrides Map used to get the query context
   *
   * @return A new LookbackQuery with the provided context
   */
  @Override
  public Query<Result<LookbackResultValue>> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new LookbackQuery(getDataSource(), getPostAggregatorSpecs(),
                             computeOverridenContext(contextOverrides), getLookbackOffsets(), getLookbackPrefixes(),
                             getHavingSpec(), getLimitSpec()
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public Query<Result<LookbackResultValue>> withId(String id)
  {
    return super.withId(id);
  }

  /**
   * A lookback query uses the QuerySegmentSpec of its datasource and therefore a withQuerySegmentSpec method
   * returns a new query using the same dummy interval as the original lookback query
   *
   * @param querySegmentSpec QuerySegmentSpec used for defining the interval of the LookbackQuery
   *
   * @return A new Lookback query with the provided interval
   */
  @Override
  public Query<Result<LookbackResultValue>> withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new LookbackQuery(getDataSource(), getPostAggregatorSpecs(),
                             getContext(), getLookbackOffsets(), getLookbackPrefixes(),
                             getHavingSpec(), getLimitSpec()
    );
  }

  /**
   * Create a LookbackQuery with the provided dataSource
   *
   * @param dataSource Datasource to be used for creating the new LookbackQuery
   *
   * @return A new LookbackQuery using the datasource provided
   */
  @Override
  public Query<Result<LookbackResultValue>> withDataSource(DataSource dataSource)
  {
    return new LookbackQuery(dataSource, getPostAggregatorSpecs(),
                             getContext(), getLookbackOffsets(), getLookbackPrefixes(), getHavingSpec(), getLimitSpec()
    );
  }

  public static class LookbackQueryBuilder
  {

    private DataSource dataSource;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, Object> context;
    private List<Period> lookbackOffsets;
    private List<String> lookbackPrefixes;
    private LimitSpec limitSpec;
    private HavingSpec havingSpec;

    public LookbackQueryBuilder()
    {
    }

    public LookbackQueryBuilder(LookbackQuery query)
    {
      this.dataSource = query.getDataSource();
      this.postAggregatorSpecs = query.getPostAggregatorSpecs();
      this.context = query.getContext();
      this.lookbackOffsets.addAll(query.getLookbackOffsets());
      this.lookbackPrefixes.addAll(query.getLookbackPrefixes());
      this.limitSpec = query.getLimitSpec();
      this.havingSpec = query.getHavingSpec();
    }

    public LookbackQueryBuilder(LookbackQueryBuilder builder)
    {
      this.dataSource = builder.dataSource;
      this.postAggregatorSpecs = builder.postAggregatorSpecs;
      this.context = builder.context;
      this.lookbackOffsets.addAll(builder.lookbackOffsets);
      this.lookbackPrefixes.addAll(builder.lookbackPrefixes);
      this.limitSpec = builder.limitSpec;
      this.havingSpec = builder.havingSpec;
    }

    public LookbackQueryBuilder setDatasource(DataSource datasource)
    {
      this.dataSource = datasource;
      return this;
    }

    public LookbackQueryBuilder setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = postAggregatorSpecs;
      return this;
    }

    public LookbackQueryBuilder setContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public LookbackQueryBuilder setLookbackOffsets(List<Period> lookbackOffset)
    {
      this.lookbackOffsets = lookbackOffset;
      return this;
    }

    public LookbackQueryBuilder addLookbackOffset(Period period)
    {
      if (lookbackOffsets == null) {
        lookbackOffsets = new ArrayList<>();
      }
      this.lookbackOffsets.add(period);
      return this;
    }

    public LookbackQueryBuilder setLookbackPrefixes(List<String> lookbackPrefixes)
    {
      this.lookbackPrefixes = lookbackPrefixes;
      return this;
    }

    public LookbackQueryBuilder addLookbackPrefix(String lookbackPrefix)
    {
      if (lookbackPrefixes == null) {
        lookbackPrefixes = new ArrayList<>();
      }
      this.lookbackPrefixes.add(lookbackPrefix);
      return this;
    }

    public LookbackQueryBuilder setLimitSpec(LimitSpec limitSpec)
    {
      this.limitSpec = limitSpec;
      return this;
    }

    public LookbackQueryBuilder setHavingSpec(HavingSpec havingSpec)
    {
      this.havingSpec = havingSpec;
      return this;
    }

    public LookbackQueryBuilder copy()
    {
      return new LookbackQueryBuilder(this);
    }

    public LookbackQuery build()
    {
      return new LookbackQuery(
          dataSource,
          postAggregatorSpecs,
          context,
          lookbackOffsets,
          lookbackPrefixes,
          havingSpec,
          limitSpec
      );
    }

  }

  @Override
  public String toString()
  {
    return "LookbackQuery{" + "dataSource='" + getDataSource() + '\'' + ", querySegmentSpec="
           + getQuerySegmentSpec() + ", postAggregatorSpecs=" + postAggregatorSpecs + ", context="
           + getContext() + ", lookbackOffsets=" + lookbackOffsets + ", lookbackPrefixes=" + lookbackPrefixes
           + ", having=" + havingSpec + ", limitSpec=" + limitSpec + '}';
  }

  @Override
  public boolean equals(Object o)
  {

    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    LookbackQuery that = (LookbackQuery) o;

    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(that.postAggregatorSpecs)
                                    : that.postAggregatorSpecs != null) {
      return false;
    }
    if (lookbackOffsets != null ? !lookbackOffsets.equals(that.lookbackOffsets) : that.lookbackOffsets != null) {
      return false;
    }
    if (lookbackPrefixes != null ? !lookbackPrefixes.equals(that.lookbackPrefixes) : that.lookbackPrefixes != null) {
      return false;
    }
    if (limitSpec != null ? !limitSpec.equals(that.limitSpec) : that.limitSpec != null) {
      return false;
    }
    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (lookbackOffsets != null ? lookbackOffsets.hashCode() : 0);
    result = 31 * result + (lookbackPrefixes != null ? lookbackPrefixes.hashCode() : 0);
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (havingSpec != null ? havingSpec.hashCode() : 0);

    return result;
  }
}
