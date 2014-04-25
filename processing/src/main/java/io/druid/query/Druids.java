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

package io.druid.query;

import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NoopDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.InsensitiveContainsSearchQuerySpec;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class Druids
{
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
      fields = Lists.<DimFilter>newArrayList(new SelectorDimFilter(dimensionName, value));
      for (String val : values) {
        fields.add(new SelectorDimFilter(dimensionName, val));
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
      return new SelectorDimFilter(dimension, value);
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

    private TimeseriesQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      dimFilter = null;
      granularity = QueryGranularity.ALL;
      aggregatorSpecs = Lists.newArrayList();
      postAggregatorSpecs = Lists.newArrayList();
      context = null;
    }

    public TimeseriesQuery build()
    {
      return new TimeseriesQuery(
          dataSource,
          querySegmentSpec,
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
      dimFilter = new SelectorDimFilter(dimensionName, value);
      return this;
    }

    public TimeseriesQueryBuilder filters(String dimensionName, String value, String... values)
    {
      List<DimFilter> fields = Lists.<DimFilter>newArrayList(new SelectorDimFilter(dimensionName, value));
      for (String val : values) {
        fields.add(new SelectorDimFilter(dimensionName, val));
      }
      dimFilter = new OrDimFilter(fields);
      return this;
    }

    public TimeseriesQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
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
    private List<String> dimensions;
    private SearchQuerySpec querySpec;
    private Map<String, Object> context;

    public SearchQueryBuilder()
    {
      dataSource = null;
      dimFilter = null;
      granularity = QueryGranularity.ALL;
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
          null,
          context
      );
    }

    public SearchQueryBuilder copy(SearchQuery query)
    {
      return new SearchQueryBuilder()
          .dataSource(((TableDataSource)query.getDataSource()).getName())
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
      dimFilter = new SelectorDimFilter(dimensionName, value);
      return this;
    }

    public SearchQueryBuilder filters(String dimensionName, String value, String... values)
    {
      List<DimFilter> fields = Lists.<DimFilter>newArrayList(new SelectorDimFilter(dimensionName, value));
      for (String val : values) {
        fields.add(new SelectorDimFilter(dimensionName, val));
      }
      dimFilter = new OrDimFilter(fields);
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
      dimensions = Lists.newArrayList(d);
      return this;
    }

    public SearchQueryBuilder dimensions(List<String> d)
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
      querySpec = new InsensitiveContainsSearchQuerySpec(q);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q)
    {
      querySpec = new InsensitiveContainsSearchQuerySpec((String) q.get("value"));
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
    private Map<String, Object> context;

    public TimeBoundaryQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      context = null;
    }

    public TimeBoundaryQuery build()
    {
      return new TimeBoundaryQuery(
          dataSource,
          querySegmentSpec,
          context
      );
    }

    public TimeBoundaryQueryBuilder copy(TimeBoundaryQueryBuilder builder)
    {
      return new TimeBoundaryQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
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
   *   Result<T> result = Druids.newResultBuilder()
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
}
