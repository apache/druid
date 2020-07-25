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

package org.apache.druid.query.topn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A Builder for TopNQuery.
 *
 * Required: dataSource(), intervals(), metric() and threshold() must be called before build()
 * Additional requirement for numeric metric sorts: aggregators() must be called before build()
 *
 * Optional: filters(), granularity(), postAggregators() and context() can be called before build()
 *
 * Usage example:
 * <pre><code>
 *   TopNQuery query = new TopNQueryBuilder()
 *                                  .dataSource("Example")
 *                                  .dimension("example_dim")
 *                                  .metric("example_metric")
 *                                  .threshold(100)
 *                                  .intervals("2012-01-01/2012-01-02")
 *                                  .build();
 * </code></pre>
 *
 * @see TopNQuery
 */
public class TopNQueryBuilder
{
  private DataSource dataSource;
  private VirtualColumns virtualColumns;
  private DimensionSpec dimensionSpec;
  private TopNMetricSpec topNMetricSpec;
  private int threshold;
  private QuerySegmentSpec querySegmentSpec;
  private DimFilter dimFilter;
  private Granularity granularity;
  private List<AggregatorFactory> aggregatorSpecs;
  private List<PostAggregator> postAggregatorSpecs;
  private Map<String, Object> context;

  public TopNQueryBuilder()
  {
    dataSource = null;
    virtualColumns = null;
    dimensionSpec = null;
    topNMetricSpec = null;
    threshold = 0;
    querySegmentSpec = null;
    dimFilter = null;
    granularity = Granularities.ALL;
    aggregatorSpecs = new ArrayList<>();
    postAggregatorSpecs = new ArrayList<>();
    context = null;
  }

  public TopNQueryBuilder(final TopNQuery query)
  {
    this.dataSource = query.getDataSource();
    this.virtualColumns = query.getVirtualColumns();
    this.dimensionSpec = query.getDimensionSpec();
    this.topNMetricSpec = query.getTopNMetricSpec();
    this.threshold = query.getThreshold();
    this.querySegmentSpec = query.getQuerySegmentSpec();
    this.dimFilter = query.getDimensionsFilter();
    this.granularity = query.getGranularity();
    this.aggregatorSpecs = query.getAggregatorSpecs();
    this.postAggregatorSpecs = query.getPostAggregatorSpecs();
    this.context = query.getContext();
  }

  public TopNQuery build()
  {
    return new TopNQuery(
        dataSource,
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        querySegmentSpec,
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        context
    );
  }

  @Deprecated
  public TopNQueryBuilder copy(TopNQuery query)
  {
    return new TopNQueryBuilder(query);
  }

  @Deprecated
  public TopNQueryBuilder copy(TopNQueryBuilder builder)
  {
    return new TopNQueryBuilder()
        .dataSource(builder.dataSource)
        .virtualColumns(builder.virtualColumns)
        .dimension(builder.dimensionSpec)
        .metric(builder.topNMetricSpec)
        .threshold(builder.threshold)
        .intervals(builder.querySegmentSpec)
        .filters(builder.dimFilter)
        .granularity(builder.granularity)
        .aggregators(builder.aggregatorSpecs)
        .postAggregators(builder.postAggregatorSpecs)
        .context(builder.context);
  }

  public TopNQueryBuilder dataSource(String d)
  {
    dataSource = new TableDataSource(d);
    return this;
  }

  public TopNQueryBuilder virtualColumns(VirtualColumns virtualColumns)
  {
    this.virtualColumns = virtualColumns;
    return this;
  }

  public TopNQueryBuilder virtualColumns(VirtualColumn... virtualColumns)
  {
    return virtualColumns(VirtualColumns.create(Arrays.asList(virtualColumns)));
  }

  public TopNQueryBuilder dataSource(DataSource d)
  {
    dataSource = d;
    return this;
  }

  public TopNQueryBuilder dimension(String d)
  {
    return dimension(d, null);
  }

  public TopNQueryBuilder dimension(String d, String outputName)
  {
    return dimension(new DefaultDimensionSpec(d, outputName));
  }

  public TopNQueryBuilder dimension(DimensionSpec d)
  {
    dimensionSpec = d;
    return this;
  }

  public TopNQueryBuilder metric(String s)
  {
    return metric(new NumericTopNMetricSpec(s));
  }

  public TopNQueryBuilder metric(TopNMetricSpec t)
  {
    topNMetricSpec = t;
    return this;
  }

  public TopNQueryBuilder threshold(int i)
  {
    threshold = i;
    return this;
  }

  public TopNQueryBuilder intervals(QuerySegmentSpec q)
  {
    querySegmentSpec = q;
    return this;
  }

  public TopNQueryBuilder intervals(String s)
  {
    querySegmentSpec = new LegacySegmentSpec(s);
    return this;
  }

  public TopNQueryBuilder intervals(List<Interval> l)
  {
    querySegmentSpec = new LegacySegmentSpec(l);
    return this;
  }

  public TopNQueryBuilder filters(String dimensionName, String value)
  {
    dimFilter = new SelectorDimFilter(dimensionName, value, null);
    return this;
  }

  public TopNQueryBuilder filters(String dimensionName, String value, String... values)
  {
    final Set<String> filterValues = Sets.newHashSet(values);
    filterValues.add(value);
    dimFilter = new InDimFilter(dimensionName, filterValues, null, null);
    return this;
  }

  public TopNQueryBuilder filters(DimFilter f)
  {
    dimFilter = f;
    return this;
  }

  public TopNQueryBuilder granularity(Granularity g)
  {
    granularity = g;
    return this;
  }

  @SuppressWarnings("unchecked")
  public TopNQueryBuilder aggregators(List<? extends AggregatorFactory> a)
  {
    aggregatorSpecs = new ArrayList<>(a); // defensive copy
    return this;
  }

  public TopNQueryBuilder aggregators(AggregatorFactory... aggs)
  {
    aggregatorSpecs = Arrays.asList(aggs);
    return this;
  }

  public TopNQueryBuilder postAggregators(Collection<PostAggregator> p)
  {
    postAggregatorSpecs = new ArrayList<>(p); // defensive copy
    return this;
  }

  public TopNQueryBuilder postAggregators(PostAggregator... postAggs)
  {
    postAggregatorSpecs = Arrays.asList(postAggs);
    return this;
  }

  public TopNQueryBuilder context(Map<String, Object> c)
  {
    this.context = c;
    return this;
  }

  public TopNQueryBuilder randomQueryId()
  {
    return queryId(UUID.randomUUID().toString());
  }

  public TopNQueryBuilder queryId(String queryId)
  {
    context = BaseQuery.computeOverriddenContext(context, ImmutableMap.of(BaseQuery.QUERY_ID, queryId));
    return this;
  }
}
