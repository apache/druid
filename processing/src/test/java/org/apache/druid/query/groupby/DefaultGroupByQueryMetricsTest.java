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

package org.apache.druid.query.groupby;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DefaultQueryMetricsTest;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DefaultGroupByQueryMetricsTest extends InitializedNullHandlingTest
{

  /**
   * Tests that passed a query {@link DefaultGroupByQueryMetrics} produces events with a certain set of dimensions,
   * no more, no less.
   */
  @Test
  public void testDefaultGroupByQueryMetricsQuery()
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    DefaultGroupByQueryMetrics queryMetrics = new DefaultGroupByQueryMetrics();
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04").setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(
                new MapLookupExtractor(ImmutableMap.of("mezzanine", "mezzanine0"), false),
                false,
                null,
                true,
                false
            )
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
    GroupByQuery query = builder.build();
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
    Interval expectedInterval = Intervals.of("2011-04-02/2011-04-04");
    Assert.assertEquals(Collections.singletonList(expectedInterval.toString()), actualEvent.get(DruidMetrics.INTERVAL));
    Assert.assertEquals("true", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedInterval.toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("", actualEvent.get(DruidMetrics.ID));
    Assert.assertEquals(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true), actualEvent.get("context"));

    // GroupBy-specific dimensions
    Assert.assertEquals("1", actualEvent.get("numDimensions"));
    Assert.assertEquals("2", actualEvent.get("numMetrics"));
    Assert.assertEquals("0", actualEvent.get("numComplexMetrics"));

    // Metric
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));
  }

  @Test
  public void testDefaultGroupByQueryMetricsMetricNamesAndUnits()
  {
    DefaultGroupByQueryMetrics queryMetrics = new DefaultGroupByQueryMetrics();
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(queryMetrics);
  }
}
