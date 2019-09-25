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

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.metadata.metadata.ColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.query.search.FragmentSearchQuerySpec;
import org.apache.druid.query.search.InsensitiveContainsSearchQuerySpec;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQuerySpec;
import org.apache.druid.query.search.SearchSortSpec;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
   * @see TimeseriesQuery
   */
  public static class TimeseriesQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private boolean descending;
    private VirtualColumns virtualColumns;
    private DimFilter dimFilter;
    private Granularity granularity;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, Object> context;
    private int limit;

    private TimeseriesQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      descending = false;
      virtualColumns = null;
      dimFilter = null;
      granularity = Granularities.ALL;
      aggregatorSpecs = new ArrayList<>();
      postAggregatorSpecs = new ArrayList<>();
      limit = 0;
      context = null;
    }

    public TimeseriesQuery build()
    {
      return new TimeseriesQuery(
          dataSource,
          querySegmentSpec,
          descending,
          virtualColumns,
          dimFilter,
          granularity,
          aggregatorSpecs,
          postAggregatorSpecs,
          limit,
          context
      );
    }

    public static TimeseriesQueryBuilder copy(TimeseriesQuery query)
    {
      return new TimeseriesQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .descending(query.isDescending())
          .virtualColumns(query.getVirtualColumns())
          .filters(query.getDimensionsFilter())
          .granularity(query.getGranularity())
          .aggregators(query.getAggregatorSpecs())
          .postAggregators(query.getPostAggregatorSpecs())
          .limit(query.getLimit())
          .context(query.getContext());
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

    public TimeseriesQueryBuilder virtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public TimeseriesQueryBuilder virtualColumns(VirtualColumn... virtualColumns)
    {
      return virtualColumns(VirtualColumns.create(Arrays.asList(virtualColumns)));
    }

    public TimeseriesQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public TimeseriesQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null, null);
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
      granularity = Granularity.fromString(g);
      return this;
    }

    public TimeseriesQueryBuilder granularity(Granularity g)
    {
      granularity = g;
      return this;
    }

    public TimeseriesQueryBuilder aggregators(List<AggregatorFactory> a)
    {
      aggregatorSpecs = a;
      return this;
    }

    public TimeseriesQueryBuilder aggregators(AggregatorFactory... aggregators)
    {
      aggregatorSpecs = Arrays.asList(aggregators);
      return this;
    }

    public TimeseriesQueryBuilder postAggregators(List<PostAggregator> p)
    {
      postAggregatorSpecs = p;
      return this;
    }

    public TimeseriesQueryBuilder postAggregators(PostAggregator... postAggregators)
    {
      postAggregatorSpecs = Arrays.asList(postAggregators);
      return this;
    }

    public TimeseriesQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public TimeseriesQueryBuilder limit(int lim)
    {
      limit = lim;
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
   * @see SearchQuery
   */
  public static class SearchQueryBuilder
  {
    private DataSource dataSource;
    private DimFilter dimFilter;
    private Granularity granularity;
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
      granularity = Granularities.ALL;
      limit = 0;
      querySegmentSpec = null;
      dimensions = null;
      querySpec = null;
      sortSpec = null;
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

    public static SearchQueryBuilder copy(SearchQuery query)
    {
      return new SearchQueryBuilder()
          .dataSource(query.getDataSource())
          .filters(query.getDimensionsFilter())
          .granularity(query.getGranularity())
          .limit(query.getLimit())
          .intervals(query.getQuerySegmentSpec())
          .dimensions(query.getDimensions())
          .query(query.getQuery())
          .sortSpec(query.getSort())
          .context(query.getContext());
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
      dimFilter = new SelectorDimFilter(dimensionName, value, null, null);
      return this;
    }

    public SearchQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SearchQueryBuilder granularity(Granularity g)
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
      dimensions = Collections.singletonList(d);
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

    public SearchQueryBuilder query(String q, boolean caseSensitive)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new ContainsSearchQuerySpec(q, caseSensitive);
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
   * @see TimeBoundaryQuery
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

    public static TimeBoundaryQueryBuilder copy(TimeBoundaryQuery query)
    {
      return new TimeBoundaryQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .bound(query.getBound())
          .filters(query.getFilter())
          .context(query.getContext());
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
   * @see SegmentMetadataQuery
   */
  public static class SegmentMetadataQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private ColumnIncluderator toInclude;
    private EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes;
    private Boolean merge;
    private Boolean lenientAggregatorMerge;
    private Boolean usingDefaultInterval;
    private Map<String, Object> context;

    public SegmentMetadataQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      toInclude = null;
      analysisTypes = null;
      merge = null;
      lenientAggregatorMerge = null;
      usingDefaultInterval = null;
      context = null;
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
          usingDefaultInterval,
          lenientAggregatorMerge
      );
    }

    public static SegmentMetadataQueryBuilder copy(SegmentMetadataQuery query)
    {
      return new SegmentMetadataQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .toInclude(query.getToInclude())
          .analysisTypes(query.getAnalysisTypes())
          .merge(query.isMerge())
          .lenientAggregatorMerge(query.isLenientAggregatorMerge())
          .usingDefaultInterval(query.isUsingDefaultInterval())
          .context(query.getContext());
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

    public SegmentMetadataQueryBuilder analysisTypes(EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes)
    {
      this.analysisTypes = analysisTypes;
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

    public SegmentMetadataQueryBuilder usingDefaultInterval(boolean usingDefaultInterval)
    {
      this.usingDefaultInterval = usingDefaultInterval;
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
   * @see SelectQuery
   */
  public static class SelectQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private boolean descending;
    private Map<String, Object> context;
    private DimFilter dimFilter;
    private Granularity granularity;
    private List<DimensionSpec> dimensions;
    private List<String> metrics;
    private VirtualColumns virtualColumns;
    private PagingSpec pagingSpec;

    public SelectQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      descending = false;
      context = null;
      dimFilter = null;
      granularity = Granularities.ALL;
      dimensions = new ArrayList<>();
      metrics = new ArrayList<>();
      virtualColumns = null;
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
          dimensions,
          metrics,
          virtualColumns,
          pagingSpec,
          context
      );
    }

    public static SelectQueryBuilder copy(SelectQuery query)
    {
      return new SelectQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .descending(query.isDescending())
          .filters(query.getFilter())
          .granularity(query.getGranularity())
          .dimensionSpecs(query.getDimensions())
          .metrics(query.getMetrics())
          .virtualColumns(query.getVirtualColumns())
          .pagingSpec(query.getPagingSpec())
          .context(query.getContext());
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

    public SelectQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SelectQueryBuilder granularity(Granularity g)
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

    public SelectQueryBuilder virtualColumns(VirtualColumns vcs)
    {
      virtualColumns = vcs;
      return this;
    }

    public SelectQueryBuilder virtualColumns(List<VirtualColumn> vcs)
    {
      return virtualColumns(VirtualColumns.create(vcs));
    }

    public SelectQueryBuilder virtualColumns(VirtualColumn... vcs)
    {
      return virtualColumns(VirtualColumns.create(Arrays.asList(vcs)));
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
   * A Builder for ScanQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   ScanQuery query = new ScanQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see ScanQuery
   */
  public static class ScanQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private VirtualColumns virtualColumns;
    private Map<String, Object> context;
    private ScanQuery.ResultFormat resultFormat;
    private int batchSize;
    private long limit;
    private DimFilter dimFilter;
    private List<String> columns;
    private Boolean legacy;
    private ScanQuery.Order order;

    public ScanQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      virtualColumns = null;
      context = null;
      resultFormat = null;
      batchSize = 0;
      limit = 0;
      dimFilter = null;
      columns = new ArrayList<>();
      legacy = null;
      order = null;
    }

    public ScanQuery build()
    {
      return new ScanQuery(
          dataSource,
          querySegmentSpec,
          virtualColumns,
          resultFormat,
          batchSize,
          limit,
          order,
          dimFilter,
          columns,
          legacy,
          context
      );
    }

    public static ScanQueryBuilder copy(ScanQuery query)
    {
      return new ScanQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .virtualColumns(query.getVirtualColumns())
          .resultFormat(query.getResultFormat())
          .batchSize(query.getBatchSize())
          .limit(query.getScanRowsLimit())
          .filters(query.getFilter())
          .columns(query.getColumns())
          .legacy(query.isLegacy())
          .context(query.getContext())
          .order(query.getOrder());
    }

    public ScanQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public ScanQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public ScanQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public ScanQueryBuilder virtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public ScanQueryBuilder virtualColumns(VirtualColumn... virtualColumns)
    {
      return virtualColumns(VirtualColumns.create(Arrays.asList(virtualColumns)));
    }

    public ScanQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public ScanQueryBuilder resultFormat(ScanQuery.ResultFormat r)
    {
      resultFormat = r;
      return this;
    }

    public ScanQueryBuilder batchSize(int b)
    {
      batchSize = b;
      return this;
    }

    public ScanQueryBuilder limit(long l)
    {
      limit = l;
      return this;
    }

    public ScanQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public ScanQueryBuilder columns(List<String> c)
    {
      columns = c;
      return this;
    }

    public ScanQueryBuilder columns(String... c)
    {
      columns = Arrays.asList(c);
      return this;
    }

    public ScanQueryBuilder legacy(Boolean legacy)
    {
      this.legacy = legacy;
      return this;
    }

    public ScanQueryBuilder order(ScanQuery.Order order)
    {
      this.order = order;
      return this;
    }
  }

  public static ScanQueryBuilder newScanQueryBuilder()
  {
    return new ScanQueryBuilder();
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
   * @see DataSourceMetadataQuery
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

    public static DataSourceMetadataQueryBuilder copy(DataSourceMetadataQuery query)
    {
      return new DataSourceMetadataQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .context(query.getContext());
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
