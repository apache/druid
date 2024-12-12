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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultQueryMetricsTest extends InitializedNullHandlingTest
{

  /**
   * Tests that passed a query {@link DefaultQueryMetrics} produces events with a certain set of dimensions, no more,
   * no less.
   */
  @Test
  public void testDefaultQueryMetricsQuery()
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new ListFilteredDimensionSpec(
            new DefaultDimensionSpec("tags", "tags"),
            ImmutableSet.of("t3"),
            null
        ))
        .metric("count")
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("count"))
        .threshold(5)
        .filters(new SelectorDimFilter("tags", "t3", null))
        .context(ImmutableMap.of("testKey", "testValue"))
        .build();
    queryMetrics.query(query);
    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    // No way to verify this right now since DefaultQueryMetrics implements a no-op for sqlQueryId(String) and queryId(String)
    // This change is done to keep the code coverage tool happy by exercising the implementation
    queryMetrics.sqlQueryId("dummy");
    queryMetrics.queryId("dummy");
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assert.assertEquals(13, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals("xx", actualEvent.get(DruidMetrics.DATASOURCE));
    Assert.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    List<Interval> expectedIntervals = QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals();
    List<String> expectedStringIntervals =
        expectedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
    Assert.assertEquals(expectedStringIntervals, actualEvent.get(DruidMetrics.INTERVAL));
    Assert.assertEquals("true", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedIntervals.get(0).toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("dummy", actualEvent.get(DruidMetrics.ID));
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
    Assert.assertEquals(ImmutableMap.of("testKey", "testValue"), actualEvent.get("context"));
  }

  @Test
  public void testDefaultQueryMetricsMetricNamesAndUnits()
  {
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();
    testQueryMetricsDefaultMetricNamesAndUnits(queryMetrics);
  }

  public static void testQueryMetricsDefaultMetricNamesAndUnits(
      QueryMetrics<? extends Query<?>> queryMetrics
  )
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter();
    queryMetrics.reportQueryTime(1000001).emit(serviceEmitter);
    // query/time and most metrics below are measured in milliseconds by default
    serviceEmitter.verifyValue("query/time", 1L);

    queryMetrics.reportWaitTime(2000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/wait/time", 2L);

    queryMetrics.reportSegmentTime(3000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/segment/time", 3L);

    queryMetrics.reportSegmentAndCacheTime(4000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/segmentAndCache/time", 4L);

    // CPU time is measured in microseconds by default
    queryMetrics.reportCpuTime(6000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/cpu/time", 6000L);

    queryMetrics.reportNodeTimeToFirstByte(7000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/node/ttfb", 7L);

    queryMetrics.reportNodeTime(8000001).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/node/time", 8L);

    queryMetrics.reportQueryBytes(9).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/bytes", 9L);

    queryMetrics.reportNodeBytes(10).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/node/bytes", 10L);
    Assert.assertEquals(9, serviceEmitter.getEvents().size());

    // Verify that Queried Segment Count does not get emitted by the DefaultQueryMetrics
    // and the total number of emitted metrics remains unchanged
    queryMetrics.reportQueriedSegmentCount(25).emit(serviceEmitter);
    Assert.assertEquals(9, serviceEmitter.getEvents().size());
  }

  @Test
  public void testVectorizedDimensionInMetrics()
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();
    queryMetrics.vectorized(true);
    queryMetrics.reportSegmentTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assert.assertEquals(7, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals("query/segment/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
    Assert.assertEquals(true, actualEvent.get("vectorized"));
  }
}
