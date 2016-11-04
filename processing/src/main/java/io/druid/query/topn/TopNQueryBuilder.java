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

package io.druid.query.topn;

import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.query.DataSource;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

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
 * @see io.druid.query.topn.TopNQuery
 */
public class TopNQueryBuilder
{
  private DataSource dataSource;
  private DimensionSpec dimensionSpec;
  private TopNMetricSpec topNMetricSpec;
  private int threshold;
  private QuerySegmentSpec querySegmentSpec;
  private DimFilter dimFilter;
  private QueryGranularity granularity;
  private List<AggregatorFactory> aggregatorSpecs;
  private List<PostAggregator> postAggregatorSpecs;
  private Map<String, Object> context;

  public TopNQueryBuilder()
  {
    dataSource = null;
    dimensionSpec = null;
    topNMetricSpec = null;
    threshold = 0;
    querySegmentSpec = null;
    dimFilter = null;
    granularity = QueryGranularities.ALL;
    aggregatorSpecs = Lists.newArrayList();
    postAggregatorSpecs = Lists.newArrayList();
    context = null;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public DimensionSpec getDimensionSpec()
  {
    return dimensionSpec;
  }

  public TopNMetricSpec getTopNMetricSpec()
  {
    return topNMetricSpec;
  }

  public int getThreshold()
  {
    return threshold;
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

  public TopNQuery build()
  {
    return new TopNQuery(
        dataSource,
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

  public TopNQueryBuilder copy(TopNQuery query)
  {
    return new TopNQueryBuilder()
        .dataSource(query.getDataSource().toString())
        .dimension(query.getDimensionSpec())
        .metric(query.getTopNMetricSpec())
        .threshold(query.getThreshold())
        .intervals(query.getIntervals())
        .filters(query.getDimensionsFilter())
        .granularity(query.getGranularity())
        .aggregators(query.getAggregatorSpecs())
        .postAggregators(query.getPostAggregatorSpecs())
        .context(query.getContext());
  }

  public TopNQueryBuilder copy(TopNQueryBuilder builder)
  {
    return new TopNQueryBuilder()
        .dataSource(builder.dataSource)
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
    dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
    return this;
  }

  public TopNQueryBuilder filters(DimFilter f)
  {
    dimFilter = f;
    return this;
  }

  public TopNQueryBuilder granularity(String g)
  {
    granularity = QueryGranularity.fromString(g);
    return this;
  }

  public TopNQueryBuilder granularity(QueryGranularity g)
  {
    granularity = g;
    return this;
  }

  public TopNQueryBuilder aggregators(List<AggregatorFactory> a)
  {
    aggregatorSpecs = a;
    return this;
  }

  public TopNQueryBuilder postAggregators(List<PostAggregator> p)
  {
    postAggregatorSpecs = p;
    return this;
  }

  public TopNQueryBuilder context(Map<String, Object> c)
  {
    context = c;
    return this;
  }
}
