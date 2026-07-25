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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
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
    // No way to verify this right now since DefaultQueryMetrics implements a no-op for sqlQueryId(String) and queryId(String)
    // This change is done to keep the code coverage tool happy by exercising the implementation
    queryMetrics.sqlQueryId("dummy");
    queryMetrics.queryId("dummy");
    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertEquals(14, actualEvent.size());
    Assertions.assertTrue(actualEvent.containsKey("feed"));
    Assertions.assertTrue(actualEvent.containsKey("timestamp"));
    Assertions.assertEquals("localhost", actualEvent.get("host"));
    Assertions.assertEquals("testing", actualEvent.get("service"));
    Assertions.assertEquals("xx", actualEvent.get(DruidMetrics.DATASOURCE));
    Assertions.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    List<Interval> expectedIntervals = QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals();
    List<String> expectedStringIntervals =
        expectedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
    Assertions.assertEquals(expectedStringIntervals, actualEvent.get(DruidMetrics.INTERVAL));
    Assertions.assertEquals("true", actualEvent.get("hasFilters"));
    Assertions.assertEquals(expectedIntervals.get(0).toDuration().toString(), actualEvent.get("duration"));
    Assertions.assertEquals("dummy", actualEvent.get(DruidMetrics.ID));
    Assertions.assertEquals("query/time", actualEvent.get("metric"));
    Assertions.assertEquals(0L, actualEvent.get("value"));
    Assertions.assertEquals(ImmutableMap.of("testKey", "testValue"), actualEvent.get("context"));
    Assertions.assertFalse(actualEvent.containsKey(DruidMetrics.LANE));
    Assertions.assertEquals(QueryContexts.DEFAULT_PRIORITY, actualEvent.get(DruidMetrics.PRIORITY));
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

    queryMetrics.reportResultCachePoll(true).emit(serviceEmitter);
    serviceEmitter.verifyValue("query/resultCache/hit", 1);

    // Verify that Queried Segment Count does not get emitted by the DefaultQueryMetrics
    // and the total number of emitted metrics remains unchanged
    queryMetrics.reportQueriedSegmentCount(25).emit(serviceEmitter);
    Assertions.assertEquals(10, serviceEmitter.getNumEmittedEvents());
  }

  @Test
  public void testLaneAndPriorityReadFromQueryContext()
  {
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
    final DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();

    queryMetrics.query(
        makeQueryWithContext(ImmutableMap.of(QueryContexts.LANE_KEY, "low", QueryContexts.PRIORITY_KEY, -5))
    );
    queryMetrics.reportQueryTime(0).emit(serviceEmitter);

    final Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertEquals("low", actualEvent.get(DruidMetrics.LANE));
    Assertions.assertEquals(-5, actualEvent.get(DruidMetrics.PRIORITY));
  }

  /**
   * An unlaned query is in no lane at all -- the scheduler gives it no lane bulkhead -- so no lane dimension is
   * emitted rather than a fabricated one. Priority always has a value, so it is always emitted.
   */
  @Test
  public void testUnlanedQueryOmitsLaneAndReportsDefaultPriority()
  {
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
    final DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();

    queryMetrics.query(makeQueryWithContext(ImmutableMap.of()));
    queryMetrics.reportQueryTime(0).emit(serviceEmitter);

    final Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertFalse(actualEvent.containsKey(DruidMetrics.LANE));
    Assertions.assertEquals(QueryContexts.DEFAULT_PRIORITY, actualEvent.get(DruidMetrics.PRIORITY));
  }

  /**
   * The direct setters are what the Broker uses to report the lane/priority the scheduler actually assigned, which can
   * differ from whatever the caller put in the context.
   */
  @Test
  public void testDirectLaneAndPriorityOverrideQueryContext()
  {
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
    final DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();

    queryMetrics.query(
        makeQueryWithContext(ImmutableMap.of(QueryContexts.LANE_KEY, "low", QueryContexts.PRIORITY_KEY, -5))
    );
    queryMetrics.lane("high");
    queryMetrics.priority(10);
    queryMetrics.reportQueryTime(0).emit(serviceEmitter);

    final Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertEquals("high", actualEvent.get(DruidMetrics.LANE));
    Assertions.assertEquals(10, actualEvent.get(DruidMetrics.PRIORITY));
  }

  /**
   * Dimensions live on a shared builder, so lane/priority ride along on every metric emitted by the same instance.
   */
  @Test
  public void testLaneAndPriorityPresentOnOtherQueryMetrics()
  {
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
    final DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();

    queryMetrics.query(makeQueryWithContext(ImmutableMap.of(QueryContexts.LANE_KEY, "low")));
    queryMetrics.reportQueryBytes(42).emit(serviceEmitter);

    final Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertEquals("query/bytes", actualEvent.get("metric"));
    Assertions.assertEquals("low", actualEvent.get(DruidMetrics.LANE));
    Assertions.assertEquals(QueryContexts.DEFAULT_PRIORITY, actualEvent.get(DruidMetrics.PRIORITY));
  }

  private static TopNQuery makeQueryWithContext(Map<String, Object> context)
  {
    return new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("tags", "tags"))
        .metric("count")
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("count"))
        .threshold(5)
        .context(context)
        .build();
  }

  @Test
  public void testVectorizedDimensionInMetrics()
  {
    final StubServiceEmitter serviceEmitter = StubServiceEmitter.createStarted();
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();
    queryMetrics.vectorized(true);
    queryMetrics.reportSegmentTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assertions.assertEquals(7, actualEvent.size());
    Assertions.assertTrue(actualEvent.containsKey("feed"));
    Assertions.assertTrue(actualEvent.containsKey("timestamp"));
    Assertions.assertEquals("localhost", actualEvent.get("host"));
    Assertions.assertEquals("testing", actualEvent.get("service"));
    Assertions.assertEquals("query/segment/time", actualEvent.get("metric"));
    Assertions.assertEquals(0L, actualEvent.get("value"));
    Assertions.assertEquals(true, actualEvent.get("vectorized"));
  }
}
