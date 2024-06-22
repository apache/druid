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

package org.apache.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DefaultQueryMetricsTest;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultTimeseriesQueryMetricsTest extends InitializedNullHandlingTest
{

  /**
   * Tests that passed a query {@link DefaultTimeseriesQueryMetrics} produces events with a certain set of dimensions,
   * no more, no less.
   */
  @Test
  public void testDefaultTimeseriesQueryMetricsQuery()
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    DefaultTimeseriesQueryMetrics queryMetrics = new DefaultTimeseriesQueryMetrics();
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.INDEX_DOUBLE_SUM)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(true)
        .context(ImmutableMap.of("testKey", "testValue"))
        .build();
    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assert.assertEquals(16, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals(QueryRunnerTestHelper.DATA_SOURCE, actualEvent.get(DruidMetrics.DATASOURCE));
    Assert.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    List<Interval> expectedIntervals = QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals();
    List<String> expectedStringIntervals =
        expectedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
    Assert.assertEquals(expectedStringIntervals, actualEvent.get(DruidMetrics.INTERVAL));
    Assert.assertEquals("false", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedIntervals.get(0).toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("", actualEvent.get(DruidMetrics.ID));
    Assert.assertEquals(ImmutableMap.of("testKey", "testValue"), actualEvent.get("context"));

    // Timeseries-specific dimensions
    Assert.assertEquals("2", actualEvent.get("numMetrics"));
    Assert.assertEquals("0", actualEvent.get("numComplexMetrics"));

    // Metric
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
  }

  @Test
  public void testDefaultTimeseriesQueryMetricsMetricNamesAndUnits()
  {
    DefaultTimeseriesQueryMetrics queryMetrics = new DefaultTimeseriesQueryMetrics();
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(queryMetrics);
  }
}
