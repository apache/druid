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

import com.google.common.collect.ImmutableSet;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ListFilteredDimensionSpec;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultQueryMetricsTest
{

  /**
   * Tests that passed a query {@link DefaultQueryMetrics} produces events with a certain set of dimensions, no more,
   * no less.
   */
  @Test
  public void testDefaultQueryMetricsQuery()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>(TestHelper.getJsonMapper());
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("xx")
        .granularity(Granularities.ALL)
        .dimension(new ListFilteredDimensionSpec(
            new DefaultDimensionSpec("tags", "tags"),
            ImmutableSet.of("t3"),
            null
        ))
        .metric("count")
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
        .threshold(5)
        .filters(new SelectorDimFilter("tags", "t3", null))
        .build();
    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals(12, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals("xx", actualEvent.get(DruidMetrics.DATASOURCE));
    Assert.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    List<Interval> expectedIntervals = QueryRunnerTestHelper.fullOnInterval.getIntervals();
    List<String> expectedStringIntervals =
        expectedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
    Assert.assertEquals(expectedStringIntervals, actualEvent.get(DruidMetrics.INTERVAL));
    Assert.assertEquals("true", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedIntervals.get(0).toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("", actualEvent.get(DruidMetrics.ID));
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
  }

  @Test
  public void testDefaultQueryMetricsMetricNamesAndUnits()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>(TestHelper.getJsonMapper());
    testQueryMetricsDefaultMetricNamesAndUnits(cachingEmitter, serviceEmitter, queryMetrics);
  }

  public static void testQueryMetricsDefaultMetricNamesAndUnits(
      CachingEmitter cachingEmitter,
      ServiceEmitter serviceEmitter,
      QueryMetrics<? extends Query<?>> queryMetrics
  )
  {
    queryMetrics.reportQueryTime(1000001).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    // query/time and most metrics below are measured in milliseconds by default
    Assert.assertEquals(1L, actualEvent.get("value"));

    queryMetrics.reportWaitTime(2000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/wait/time", actualEvent.get("metric"));
    Assert.assertEquals(2L, actualEvent.get("value"));

    queryMetrics.reportSegmentTime(3000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/segment/time", actualEvent.get("metric"));
    Assert.assertEquals(3L, actualEvent.get("value"));

    queryMetrics.reportSegmentAndCacheTime(4000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/segmentAndCache/time", actualEvent.get("metric"));
    Assert.assertEquals(4L, actualEvent.get("value"));

    queryMetrics.reportIntervalChunkTime(5000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/intervalChunk/time", actualEvent.get("metric"));
    Assert.assertEquals(5L, actualEvent.get("value"));

    queryMetrics.reportCpuTime(6000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/cpu/time", actualEvent.get("metric"));
    // CPU time is measured in microseconds by default
    Assert.assertEquals(6000L, actualEvent.get("value"));

    queryMetrics.reportNodeTimeToFirstByte(7000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/node/ttfb", actualEvent.get("metric"));
    Assert.assertEquals(7L, actualEvent.get("value"));

    queryMetrics.reportNodeTime(8000001).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/node/time", actualEvent.get("metric"));
    Assert.assertEquals(8L, actualEvent.get("value"));

    queryMetrics.reportQueryBytes(9).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/bytes", actualEvent.get("metric"));
    Assert.assertEquals(9L, actualEvent.get("value"));

    queryMetrics.reportNodeBytes(10).emit(serviceEmitter);
    actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals("query/node/bytes", actualEvent.get("metric"));
    Assert.assertEquals(10L, actualEvent.get("value"));
  }
}
