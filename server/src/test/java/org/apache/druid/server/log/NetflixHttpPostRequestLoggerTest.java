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

package org.apache.druid.server.log;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NetflixHttpPostRequestLoggerTest
{
  private static final String QUERY_ID = "fakequeryid";
  private static final String REMOTE_ADDRESS = "some.host";
  private static final String DATASOURCE = "fakeds";
  private static final boolean HAS_FILTERS = true;
  private static final String ERROR_STACK_TRACE = Throwables.getStackTraceAsString(new Exception());
  private static final Long QUERY_TIME = 100L;
  private static final Long QUERY_BYTES = 200L;
  private static String INTERVAL_STRING;
  private static String METRICS_USED;
  private static String POST_AGGREGATOR_STRING;
  private static String DIMENSIONS_USED;
  private static String AGGREGATOR_STRING;

  private static final List<Interval> INTERVALS = ImmutableList.of(
      Intervals.of("2016-01-01T00Z/2016-01-02T00Z"),
      Intervals.of("2016-01-01T00Z/2016-01-04T00Z"),
      Intervals.of("2016-01-06T00Z/2016-01-08T00Z")
  );

  private static final List<AggregatorFactory> AGGREGATOR_FACTORIES = ImmutableList.of(new LongSumAggregatorFactory(
      "n1",
      "f1"
  ), new DoubleSumAggregatorFactory("n2", "f2"));


  private static final List<PostAggregator> POST_AGGREGATOR = Collections.singletonList(new ConstantPostAggregator(
      "post",
      1
  ));

  private static final List<DimensionSpec> DIMENSION_SPECS = Collections.singletonList(new DefaultDimensionSpec(
      "dimName",
      "dimOutput"
  ));

  private static final DimFilter IN_DIM_FILTER =
      DimFilters.or(
          new InDimFilter(
              "foo",
              ImmutableList.of("a", "b"),
              null
          )
      );

  private static final Map<String, Object> QUERY_CONTEXT = ImmutableMap.of(
      "foo",
      "bar",
      BaseQuery.QUERY_ID,
      QUERY_ID
  );

  private static GroupByQuery GROUP_BY_QUERY;

  static {
    GroupByQuery.Builder builder = GroupByQuery.builder();
    builder.setDataSource(DATASOURCE)
           .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
           .setAggregatorSpecs(AGGREGATOR_FACTORIES)
           .setPostAggregatorSpecs(POST_AGGREGATOR)
           .setDimFilter(IN_DIM_FILTER)
           .setDimensions(DIMENSION_SPECS)
           .setInterval(INTERVALS)
           .setContext(QUERY_CONTEXT);
    GROUP_BY_QUERY = builder.build();

    INTERVAL_STRING = NetflixHttpPostRequestLogger.intervalsToString(INTERVALS);
    DIMENSIONS_USED = NetflixHttpPostRequestLogger.dimensionsProjectedAndFiltered(GROUP_BY_QUERY);
    METRICS_USED = NetflixHttpPostRequestLogger.metricsFromAggregators(GROUP_BY_QUERY);
    AGGREGATOR_STRING = NetflixHttpPostRequestLogger.aggregators(GROUP_BY_QUERY);
    POST_AGGREGATOR_STRING = NetflixHttpPostRequestLogger.postAggregators(GROUP_BY_QUERY);
  }

  @Test
  public void testSuccessfulGroupByQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");
    final QueryStats successfulQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStats.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStats.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStats.SUCCESS.toString(),
        true
    ));
    final RequestLogLine logLine = RequestLogLine.forNative(
        GROUP_BY_QUERY,
        timestamp,
        REMOTE_ADDRESS,
        successfulQueryStats
    );
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(Query.GROUP_BY, payload.getQueryType());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(INTERVAL_STRING, payload.getIntervals());
    Assert.assertEquals(IN_DIM_FILTER.toString(), payload.getFilter());
    Assert.assertEquals(AGGREGATOR_STRING, payload.getAggregators());
    Assert.assertEquals(POST_AGGREGATOR_STRING, payload.getPostAggregators());
    Assert.assertEquals(DIMENSIONS_USED, payload.getDimensionsUsed());
    Assert.assertEquals(METRICS_USED, payload.getMetricsUsed());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertEquals(true, payload.isQuerySuccessful());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertNull(payload.getErrorStackTrace());
    Assert.assertFalse(payload.isWasInterrupted());
    Assert.assertNull(payload.getInterruptionReason());
  }

  @Test
  public void testFailedQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");

    final QueryStats failedQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStats.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStats.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStats.ERROR_STACKTRACE.toString(),
        ERROR_STACK_TRACE,
        NetflixHttpPostRequestLogger.QueryStats.SUCCESS.toString(),
        false
    ));
    final RequestLogLine logLine = RequestLogLine.forNative(
        GROUP_BY_QUERY,
        timestamp,
        REMOTE_ADDRESS,
        failedQueryStats
    );
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(Query.GROUP_BY, payload.getQueryType());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(INTERVAL_STRING, payload.getIntervals());
    Assert.assertEquals(IN_DIM_FILTER.toString(), payload.getFilter());
    Assert.assertEquals(AGGREGATOR_STRING, payload.getAggregators());
    Assert.assertEquals(POST_AGGREGATOR_STRING, payload.getPostAggregators());
    Assert.assertEquals(DIMENSIONS_USED, payload.getDimensionsUsed());
    Assert.assertEquals(METRICS_USED, payload.getMetricsUsed());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertFalse(payload.isQuerySuccessful());
    Assert.assertEquals(ERROR_STACK_TRACE, payload.getErrorStackTrace());
    Assert.assertFalse(payload.isWasInterrupted());
    Assert.assertNull(payload.getInterruptionReason());
  }

  @Test
  public void testInterruptedQueryPayload()
  {
    final DateTime timestamp = DateTimes.of("2016-01-01T00:00:00Z");
    final String interruptionReason = "query timed out";
    final QueryStats interruptedQueryStats = new QueryStats(ImmutableMap.of(
        NetflixHttpPostRequestLogger.QueryStats.QUERY_TIME.toString(),
        QUERY_TIME,
        NetflixHttpPostRequestLogger.QueryStats.QUERY_BYTES.toString(),
        QUERY_BYTES,
        NetflixHttpPostRequestLogger.QueryStats.INTERRUPTED.toString(),
        true,
        NetflixHttpPostRequestLogger.QueryStats.INTERRUPTION_REASON.toString(),
        interruptionReason,
        NetflixHttpPostRequestLogger.QueryStats.SUCCESS.toString(),
        false

    ));
    final RequestLogLine logLine = RequestLogLine.forNative(
        GROUP_BY_QUERY,
        timestamp,
        REMOTE_ADDRESS,
        interruptedQueryStats
    );
    NetflixHttpPostRequestLogger.KeyStoneGatewayRequest request = new NetflixHttpPostRequestLogger.KeyStoneGatewayRequest(
        "druid",
        "localhost",
        false,
        logLine
    );
    NetflixHttpPostRequestLogger.Payload payload = request.getPayload();
    Assert.assertEquals(QUERY_ID, payload.getQueryId());
    Assert.assertEquals(DATASOURCE, payload.getDatasource());
    Assert.assertEquals(Query.GROUP_BY, payload.getQueryType());
    Assert.assertEquals(HAS_FILTERS, payload.isHasFilters());
    Assert.assertEquals(INTERVAL_STRING, payload.getIntervals());
    Assert.assertEquals(IN_DIM_FILTER.toString(), payload.getFilter());
    Assert.assertEquals(AGGREGATOR_STRING, payload.getAggregators());
    Assert.assertEquals(POST_AGGREGATOR_STRING, payload.getPostAggregators());
    Assert.assertEquals(DIMENSIONS_USED, payload.getDimensionsUsed());
    Assert.assertEquals(METRICS_USED, payload.getMetricsUsed());
    Assert.assertEquals(QUERY_TIME, payload.getQueryTime());
    Assert.assertEquals(QUERY_BYTES, payload.getQueryBytes());
    Assert.assertEquals(REMOTE_ADDRESS, payload.getRemoteAddress());
    Assert.assertNull(payload.getErrorStackTrace());
    Assert.assertTrue(payload.isWasInterrupted());
    Assert.assertEquals(interruptionReason, payload.getInterruptionReason());
  }
}
