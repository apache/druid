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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.NoopDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.search.search.FragmentSearchQuerySpec;
import io.druid.query.search.search.InsensitiveContainsSearchQuerySpec;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.search.search.SearchSortSpec;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 */
public class Druids
{
  public static final Function<String, DimensionSpec> DIMENSION_IDENTITY = new Function<String, DimensionSpec>()
  {
    @Nullable
    @Override
    public DimensionSpec apply(String input)
    {
      return new DefaultDimensionSpec(input, input);
    }
  };

  private Druids()
  {
    throw new AssertionError();
  }

  /**
   * A Builder for AndDimFilter.
   * <p/>
   * Required: fields() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
   *                                        .fields(listOfDimFilterFields)
   *                                        .build();
   * </code></pre>
   *
   * @see AndDimFilter
   */
  public static class AndDimFilterBuilder
  {
    private List<DimFilter> fields;

    public AndDimFilterBuilder()
    {
      fields = Lists.newArrayList();
    }

    public AndDimFilter build()
    {
      return new AndDimFilter(fields);
    }

    public AndDimFilterBuilder copy(AndDimFilterBuilder builder)
    {
      return new AndDimFilterBuilder()
          .fields(builder.fields);
    }

    public AndDimFilterBuilder fields(List<DimFilter> f)
    {
      fields.addAll(f);
      return this;
    }
  }

  public static AndDimFilterBuilder newAndDimFilterBuilder()
  {
    return new AndDimFilterBuilder();
  }

  /**
   * A Builder for OrDimFilter.
   * <p/>
   * Required: fields() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   OrDimFilter orDimFilter = Druids.newOrDimFilterBuilder()
   *                                        .fields(listOfDimFilterFields)
   *                                        .build();
   * </code></pre>
   *
   * @see OrDimFilter
   */
  public static class OrDimFilterBuilder
  {
    private List<DimFilter> fields;

    public OrDimFilterBuilder()
    {
      fields = Lists.newArrayList();
    }

    public OrDimFilter build()
    {
      return new OrDimFilter(fields);
    }

    public OrDimFilterBuilder copy(OrDimFilterBuilder builder)
    {
      return new OrDimFilterBuilder()
          .fields(builder.fields);
    }

    public OrDimFilterBuilder fields(String dimensionName, String value, String... values)
    {
      fields = Lists.<DimFilter>newArrayList(new SelectorDimFilter(dimensionName, value, null));
      for (String val : values) {
        fields.add(new SelectorDimFilter(dimensionName, val, null));
      }
      return this;
    }

    public OrDimFilterBuilder fields(List<DimFilter> f)
    {
      fields.addAll(f);
      return this;
    }
  }

  public static OrDimFilterBuilder newOrDimFilterBuilder()
  {
    return new OrDimFilterBuilder();
  }

  /**
   * A Builder for NotDimFilter.
   * <p/>
   * Required: field() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   NotDimFilter notDimFilter = Druids.newNotDimFilterBuilder()
   *                                        .field(dimFilterField)
   *                                        .build();
   * </code></pre>
   *
   * @see NotDimFilter
   */
  public static class NotDimFilterBuilder
  {
    private DimFilter field;

    public NotDimFilterBuilder()
    {
      field = null;
    }

    public NotDimFilter build()
    {
      return new NotDimFilter(field);
    }

    public NotDimFilterBuilder copy(NotDimFilterBuilder builder)
    {
      return new NotDimFilterBuilder()
          .field(builder.field);
    }

    public NotDimFilterBuilder field(DimFilter f)
    {
      field = f;
      return this;
    }
  }

  public static NotDimFilterBuilder newNotDimFilterBuilder()
  {
    return new NotDimFilterBuilder();
  }

  /**
   * A Builder for SelectorDimFilter.
   * <p/>
   * Required: dimension() and value() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   Selector selDimFilter = Druids.newSelectorDimFilterBuilder()
   *                                        .dimension("test")
   *                                        .value("sample")
   *                                        .build();
   * </code></pre>
   *
   * @see SelectorDimFilter
   */
  public static class SelectorDimFilterBuilder
  {
    private String dimension;
    private String value;

    public SelectorDimFilterBuilder()
    {
      dimension = "";
      value = "";
    }

    public SelectorDimFilter build()
    {
      return new SelectorDimFilter(dimension, value, null);
    }

    public SelectorDimFilterBuilder copy(SelectorDimFilterBuilder builder)
    {
      return new SelectorDimFilterBuilder()
          .dimension(builder.dimension)
          .value(builder.value);
    }

    public SelectorDimFilterBuilder dimension(String d)
    {
      dimension = d;
      return this;
    }

    public SelectorDimFilterBuilder value(String v)
    {
      value = v;
      return this;
    }
  }

  public static SelectorDimFilterBuilder newSelectorDimFilterBuilder()
  {
    return new SelectorDimFilterBuilder();
  }

  /**
   * A Builder for NoopDimFilter.
   * Usage example:
   * <pre><code>
   *   NoopDimFilter noopDimFilter = Druids.newNoopDimFilterBuilder()
   *                                       .build();
   * </code></pre>
   *
   * @see NotDimFilter
   */
  public static class NoopDimFilterBuilder
  {
    public NoopDimFilter build()
    {
      return new NoopDimFilter();
    }
  }

  public static NoopDimFilterBuilder newNoopDimFilterBuilder()
  {
    return new NoopDimFilterBuilder();
  }

  /**
   * A Builder for TimeseriesQuery.
   * <p/>
   * Required: dataSource(), intervals(), and aggregators() must be called before build()
   * Optional: filters(), granularity(), postAggregators(), and context() can be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
   *                                        .dataSource("Example")
   *                                        .intervals("2012-01-01/2012-01-02")
   *                                        .aggregators(listofAggregators)
   *                                        .build();
   * </code></pre>
   *
   * @see io.druid.query.timeseries.TimeseriesQuery
   */
  public static class TimeseriesQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, Object> context;

    private boolean descending;

    private TimeseriesQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      dimFilter = null;
      granularity = QueryGranularities.ALL;
      aggregatorSpecs = Lists.newArrayList();
      postAggregatorSpecs = Lists.newArrayList();
      context = null;
    }

    public TimeseriesQuery build()
    {
      return new TimeseriesQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          granularity,
          aggregatorSpecs,
          postAggregatorSpecs,
          context
      );
    }

    public TimeseriesQueryBuilder copy(TimeseriesQuery query)
    {
      return new TimeseriesQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getIntervals())
          .filters(query.getDimensionsFilter())
          .descending(query.isDescending())
          .granularity(query.getGranularity())
          .aggregators(query.getAggregatorSpecs())
          .postAggregators(query.getPostAggregatorSpecs())
          .context(query.getContext());
    }

    public TimeseriesQueryBuilder copy(TimeseriesQueryBuilder builder)
    {
      return new TimeseriesQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .filters(builder.dimFilter)
          .descending(builder.descending)
          .granularity(builder.granularity)
          .aggregators(builder.aggregatorSpecs)
          .postAggregators(builder.postAggregatorSpecs)
          .context(builder.context);
    }

    public DataSource getDataSource()
    {
      return dataSource;
    }

    public QuerySegmentSpec getQuerySegmentSpec()
    {
      return querySegmentSpec;
    }

    public DimFilter getDimFilter()
    {
      return dimFilter;
    }

    public boolean isDescending()
    {
      return descending;
    }

    public QueryGranularity getGranularity()
    {
      return granularity;
    }

    public List<AggregatorFactory> getAggregatorSpecs()
    {
      return aggregatorSpecs;
    }

    public List<PostAggregator> getPostAggregatorSpecs()
    {
      return postAggregatorSpecs;
    }

    public Map<String, Object> getContext()
    {
      return context;
    }

    public TimeseriesQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public TimeseriesQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public TimeseriesQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public TimeseriesQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public TimeseriesQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public TimeseriesQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public TimeseriesQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public TimeseriesQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public TimeseriesQueryBuilder descending(boolean d)
    {
      descending = d;
      return this;
    }

    public TimeseriesQueryBuilder granularity(String g)
    {
      granularity = QueryGranularity.fromString(g);
      return this;
    }

    public TimeseriesQueryBuilder granularity(QueryGranularity g)
    {
      granularity = g;
      return this;
    }

    public TimeseriesQueryBuilder aggregators(List<AggregatorFactory> a)
    {
      aggregatorSpecs = a;
      return this;
    }

    public TimeseriesQueryBuilder postAggregators(List<PostAggregator> p)
    {
      postAggregatorSpecs = p;
      return this;
    }

    public TimeseriesQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static TimeseriesQueryBuilder newTimeseriesQueryBuilder()
  {
    return new TimeseriesQueryBuilder();
  }

  /**
   * A Builder for SearchQuery.
   * <p/>
   * Required: dataSource(), intervals(), dimensions() and query() must be called before build()
   * <p/>
   * Optional: filters(), granularity(), and context() can be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SearchQuery query = Druids.newSearchQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .dimensions(listofEgDims)
   *                                  .query(exampleQuery)
   *                                  .intervals("2012-01-01/2012-01-02")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.search.search.SearchQuery
   */
  public static class SearchQueryBuilder
  {
    private DataSource dataSource;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private int limit;
    private QuerySegmentSpec querySegmentSpec;
    private List<DimensionSpec> dimensions;
    private SearchQuerySpec querySpec;
    private SearchSortSpec sortSpec;
    private Map<String, Object> context;

    public SearchQueryBuilder()
    {
      dataSource = null;
      dimFilter = null;
      granularity = QueryGranularities.ALL;
      limit = 0;
      querySegmentSpec = null;
      dimensions = null;
      querySpec = null;
      context = null;
    }

    public SearchQuery build()
    {
      return new SearchQuery(
          dataSource,
          dimFilter,
          granularity,
          limit,
          querySegmentSpec,
          dimensions,
          querySpec,
          sortSpec,
          context
      );
    }

    public SearchQueryBuilder copy(SearchQuery query)
    {
      return new SearchQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .filters(query.getDimensionsFilter())
          .granularity(query.getGranularity())
          .limit(query.getLimit())
          .dimensions(query.getDimensions())
          .query(query.getQuery())
          .context(query.getContext());
    }

    public SearchQueryBuilder copy(SearchQueryBuilder builder)
    {
      return new SearchQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .filters(builder.dimFilter)
          .granularity(builder.granularity)
          .limit(builder.limit)
          .dimensions(builder.dimensions)
          .query(builder.querySpec)
          .context(builder.context);
    }

    public SearchQueryBuilder dataSource(String d)
    {
      dataSource = new TableDataSource(d);
      return this;
    }

    public SearchQueryBuilder dataSource(DataSource d)
    {
      dataSource = d;
      return this;
    }

    public SearchQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public SearchQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public SearchQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SearchQueryBuilder granularity(String g)
    {
      granularity = QueryGranularity.fromString(g);
      return this;
    }

    public SearchQueryBuilder granularity(QueryGranularity g)
    {
      granularity = g;
      return this;
    }

    public SearchQueryBuilder limit(int l)
    {
      limit = l;
      return this;
    }

    public SearchQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SearchQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SearchQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public SearchQueryBuilder dimensions(String d)
    {
      dimensions = ImmutableList.of(DIMENSION_IDENTITY.apply(d));
      return this;
    }

    public SearchQueryBuilder dimensions(Iterable<String> d)
    {
      dimensions = ImmutableList.copyOf(Iterables.transform(d, DIMENSION_IDENTITY));
      return this;
    }

    public SearchQueryBuilder dimensions(DimensionSpec d)
    {
      dimensions = Lists.newArrayList(d);
      return this;
    }

    public SearchQueryBuilder dimensions(List<DimensionSpec> d)
    {
      dimensions = d;
      return this;
    }

    public SearchQueryBuilder query(SearchQuerySpec s)
    {
      querySpec = s;
      return this;
    }

    public SearchQueryBuilder query(String q)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new InsensitiveContainsSearchQuerySpec(q);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q)
    {
      String value = Preconditions.checkNotNull(q.get("value"), "no value").toString();
      querySpec = new InsensitiveContainsSearchQuerySpec(value);
      return this;
    }

    public SearchQueryBuilder query(String q, boolean caseSensitive)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new ContainsSearchQuerySpec(q, caseSensitive);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q, boolean caseSensitive)
    {
      String value = Preconditions.checkNotNull(q.get("value"), "no value").toString();
      querySpec = new ContainsSearchQuerySpec(value, caseSensitive);
      return this;
    }

    public SearchQueryBuilder fragments(List<String> q)
    {
      return fragments(q, false);
    }

    public SearchQueryBuilder sortSpec(SearchSortSpec sortSpec)
    {
      this.sortSpec = sortSpec;
      return this;
    }

    public SearchQueryBuilder fragments(List<String> q, boolean caseSensitive)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new FragmentSearchQuerySpec(q, caseSensitive);
      return this;
    }

    public SearchQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static SearchQueryBuilder newSearchQueryBuilder()
  {
    return new SearchQueryBuilder();
  }

  /**
   * A Builder for TimeBoundaryQuery.
   * <p/>
   * Required: dataSource() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   TimeBoundaryQuery query = new MaxTimeQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.timeboundary.TimeBoundaryQuery
   */
  public static class TimeBoundaryQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private String bound;
    private DimFilter dimFilter;
    private Map<String, Object> context;

    public TimeBoundaryQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      bound = null;
      dimFilter = null;
      context = null;
    }

    public TimeBoundaryQuery build()
    {
      return new TimeBoundaryQuery(
          dataSource,
          querySegmentSpec,
          bound,
          dimFilter,
          context
      );
    }

    public TimeBoundaryQueryBuilder copy(TimeBoundaryQueryBuilder builder)
    {
      return new TimeBoundaryQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .bound(builder.bound)
          .filters(builder.dimFilter)
          .context(builder.context);
    }

    public TimeBoundaryQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public TimeBoundaryQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public TimeBoundaryQueryBuilder bound(String b)
    {
      bound = b;
      return this;
    }

    public TimeBoundaryQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public TimeBoundaryQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public TimeBoundaryQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public TimeBoundaryQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static TimeBoundaryQueryBuilder newTimeBoundaryQueryBuilder()
  {
    return new TimeBoundaryQueryBuilder();
  }

  /**
   * A Builder for Result.
   * <p/>
   * Required: timestamp() and value() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   Result&lt;T&gt; result = Druids.newResultBuilder()
   *                            .timestamp(egDateTime)
   *                            .value(egValue)
   *                            .build();
   * </code></pre>
   *
   * @see Result
   */
  public static class ResultBuilder<T>
  {
    private DateTime timestamp;
    private Object value;

    public ResultBuilder()
    {
      timestamp = new DateTime(0);
      value = null;
    }

    public Result<T> build()
    {
      return new Result<T>(timestamp, (T) value);
    }

    public ResultBuilder copy(ResultBuilder builder)
    {
      return new ResultBuilder()
          .timestamp(builder.timestamp)
          .value(builder.value);
    }

    public ResultBuilder<T> timestamp(DateTime t)
    {
      timestamp = t;
      return this;
    }

    public ResultBuilder<T> value(Object v)
    {
      value = v;
      return this;
    }
  }

  public static ResultBuilder newResultBuilder()
  {
    return new ResultBuilder();
  }

  public static ResultBuilder<SearchResultValue> newSearchResultBuilder()
  {
    return new ResultBuilder<SearchResultValue>();
  }

  public static ResultBuilder<TimeBoundaryResultValue> newTimeBoundaryResultBuilder()
  {
    return new ResultBuilder<TimeBoundaryResultValue>();
  }

  /**
   * A Builder for SegmentMetadataQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SegmentMetadataQuery query = new SegmentMetadataQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.metadata.metadata.SegmentMetadataQuery
   */
  public static class SegmentMetadataQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private ColumnIncluderator toInclude;
    private EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes;
    private Boolean merge;
    private Boolean lenientAggregatorMerge;
    private Map<String, Object> context;

    public SegmentMetadataQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      toInclude = null;
      analysisTypes = null;
      merge = null;
      context = null;
      lenientAggregatorMerge = null;
    }

    public SegmentMetadataQuery build()
    {
      return new SegmentMetadataQuery(
          dataSource,
          querySegmentSpec,
          toInclude,
          merge,
          context,
          analysisTypes,
          false,
          lenientAggregatorMerge
      );
    }

    public SegmentMetadataQueryBuilder copy(SegmentMetadataQueryBuilder builder)
    {
      final SegmentMetadataQuery.AnalysisType[] analysisTypesArray =
          analysisTypes != null
          ? analysisTypes.toArray(new SegmentMetadataQuery.AnalysisType[analysisTypes.size()])
          : null;
      return new SegmentMetadataQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .toInclude(toInclude)
          .analysisTypes(analysisTypesArray)
          .merge(merge)
          .lenientAggregatorMerge(lenientAggregatorMerge)
          .context(builder.context);
    }

    public SegmentMetadataQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public SegmentMetadataQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public SegmentMetadataQueryBuilder toInclude(ColumnIncluderator toInclude)
    {
      this.toInclude = toInclude;
      return this;
    }

    public SegmentMetadataQueryBuilder analysisTypes(SegmentMetadataQuery.AnalysisType... analysisTypes)
    {
      if (analysisTypes == null) {
        this.analysisTypes = null;
      } else {
        this.analysisTypes = analysisTypes.length == 0
                             ? EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class)
                             : EnumSet.copyOf(Arrays.asList(analysisTypes));
      }
      return this;
    }

    public SegmentMetadataQueryBuilder merge(boolean merge)
    {
      this.merge = merge;
      return this;
    }

    public SegmentMetadataQueryBuilder lenientAggregatorMerge(boolean lenientAggregatorMerge)
    {
      this.lenientAggregatorMerge = lenientAggregatorMerge;
      return this;
    }

    public SegmentMetadataQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static SegmentMetadataQueryBuilder newSegmentMetadataQueryBuilder()
  {
    return new SegmentMetadataQueryBuilder();
  }

  /**
   * A Builder for SelectQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SelectQuery query = new SelectQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.select.SelectQuery
   */
  public static class SelectQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private boolean descending;
    private Map<String, Object> context;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private List<DimensionSpec> dimensions;
    private List<String> metrics;
    private PagingSpec pagingSpec;

    public SelectQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      context = null;
      dimFilter = null;
      granularity = QueryGranularities.ALL;
      dimensions = Lists.newArrayList();
      metrics = Lists.newArrayList();
      pagingSpec = null;
    }

    public SelectQuery build()
    {
      return new SelectQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          granularity,
          dimensions, metrics, pagingSpec,
          context
      );
    }

    public SelectQueryBuilder copy(SelectQueryBuilder builder)
    {
      return new SelectQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .context(builder.context);
    }

    public SelectQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public SelectQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public SelectQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SelectQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SelectQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public SelectQueryBuilder descending(boolean descending)
    {
      this.descending = descending;
      return this;
    }

    public SelectQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public SelectQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public SelectQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public SelectQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SelectQueryBuilder granularity(String g)
    {
      granularity = QueryGranularity.fromString(g);
      return this;
    }

    public SelectQueryBuilder granularity(QueryGranularity g)
    {
      granularity = g;
      return this;
    }

    public SelectQueryBuilder dimensionSpecs(List<DimensionSpec> d)
    {
      dimensions = d;
      return this;
    }

    public SelectQueryBuilder dimensions(List<String> d)
    {
      dimensions = DefaultDimensionSpec.toSpec(d);
      return this;
    }

    public SelectQueryBuilder metrics(List<String> m)
    {
      metrics = m;
      return this;
    }

    public SelectQueryBuilder pagingSpec(PagingSpec p)
    {
      pagingSpec = p;
      return this;
    }
  }

  public static SelectQueryBuilder newSelectQueryBuilder()
  {
    return new SelectQueryBuilder();
  }

  /**
   * A Builder for DataSourceMetadataQuery.
   * <p/>
   * Required: dataSource() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   DataSourceMetadataQueryBuilder query = new DataSourceMetadataQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.datasourcemetadata.DataSourceMetadataQuery
   */
  public static class DataSourceMetadataQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private Map<String, Object> context;

    public DataSourceMetadataQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      context = null;
    }

    public DataSourceMetadataQuery build()
    {
      return new DataSourceMetadataQuery(
          dataSource,
          querySegmentSpec,
          context
      );
    }

    public DataSourceMetadataQueryBuilder copy(DataSourceMetadataQueryBuilder builder)
    {
      return new DataSourceMetadataQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .context(builder.context);
    }

    public DataSourceMetadataQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public DataSourceMetadataQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public DataSourceMetadataQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static DataSourceMetadataQueryBuilder newDataSourceMetadataQueryBuilder()
  {
    return new DataSourceMetadataQueryBuilder();
  }
}
