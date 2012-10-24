package com.metamx.druid;

import com.google.common.collect.Lists;
import com.metamx.druid.query.filter.AndDimFilter;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.filter.NoopDimFilter;
import com.metamx.druid.query.filter.NotDimFilter;
import com.metamx.druid.query.filter.OrDimFilter;
import com.metamx.druid.query.filter.SelectorDimFilter;
import com.metamx.druid.query.search.InsensitiveContainsSearchQuerySpec;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQuerySpec;
import com.metamx.druid.query.segment.LegacySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import com.metamx.druid.result.TimeBoundaryResultValue;
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
   * @see com.metamx.druid.query.search.SearchQuery
   */
  public static class SearchQueryBuilder
  {
    private String dataSource;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private int limit;
    private QuerySegmentSpec querySegmentSpec;
    private List<String> dimensions;
    private SearchQuerySpec querySpec;
    private Map<String, String> context;

    public SearchQueryBuilder()
    {
      dataSource = "";
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
      querySpec = new InsensitiveContainsSearchQuerySpec(q, null);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q)
    {
      querySpec = new InsensitiveContainsSearchQuerySpec((String) q.get("value"), null);
      return this;
    }

    public SearchQueryBuilder context(Map<String, String> c)
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
   * @see com.metamx.druid.query.timeboundary.TimeBoundaryQuery
   */
  public static class TimeBoundaryQueryBuilder
  {
    private String dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private Map<String, String> context;

    public TimeBoundaryQueryBuilder()
    {
      dataSource = "";
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

    public TimeBoundaryQueryBuilder dataSource(String d)
    {
      dataSource = d;
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

    public TimeBoundaryQueryBuilder context(Map<String, String> c)
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
   * @see com.metamx.druid.result.Result
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
