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

package org.apache.druid.query.search;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.CachingEmitter;
import org.apache.druid.query.DefaultQueryMetricsTest;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultSearchQueryMetricsTest
{

  /**
   * Tests that passed a query {@link DefaultSearchQueryMetrics} produces events with a certain set of dimensions.
   */
  @Test
  public void testDefaultSearchQueryMetricsQuery()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    SearchQuery query = Druids
        .newSearchQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimensions(new ListFilteredDimensionSpec(
            new DefaultDimensionSpec("tags", "tags"),
            ImmutableSet.of("t3"),
            null
        ))
        .build();

    SearchQueryMetrics queryMetrics = DefaultSearchQueryMetricsFactory.instance().makeMetrics(query);

    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals(12, actualEvent.size());
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

    // Metric
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
  }

  @Test
  public void testDefaultSearchQueryMetricsMetricNamesAndUnits()
  {
    SearchQuery query = Druids
        .newSearchQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    SearchQueryMetrics queryMetrics = DefaultSearchQueryMetricsFactory.instance().makeMetrics(query);
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(cachingEmitter, serviceEmitter, queryMetrics);
  }
}
