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

package io.druid.query.groupby;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.query.CachingEmitter;
import io.druid.query.DefaultQueryMetricsTest;
import io.druid.query.DruidMetrics;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class DefaultGroupByQueryMetricsTest
{

  /**
   * Tests that passed a query {@link DefaultGroupByQueryMetrics} produces events with a certain set of dimensions,
   * no more, no less.
   */
  @Test
  public void testDefaultGroupByQueryMetricsQuery()
  {
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultGroupByQueryMetrics queryMetrics = new DefaultGroupByQueryMetrics(TestHelper.getJsonMapper());
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(
            Lists.<DimensionSpec>newArrayList(
                new ExtractionDimensionSpec(
                    "quality",
                    "alias",
                    new LookupExtractionFn(
                        new MapLookupExtractor(
                            ImmutableMap.of(
                                "mezzanine",
                                "mezzanine0"
                            ),
                            false
                        ), false, null, true,
                        false
                    )
                )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.<String, Object>of("bySegment", true));
    GroupByQuery query = builder.build();
    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = cachingEmitter.getLastEmittedEvent().toMap();
    Assert.assertEquals(15, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals(QueryRunnerTestHelper.dataSource, actualEvent.get(DruidMetrics.DATASOURCE));
    Assert.assertEquals(query.getType(), actualEvent.get(DruidMetrics.TYPE));
    Interval expectedInterval = new Interval("2011-04-02/2011-04-04");
    Assert.assertEquals(Collections.singletonList(expectedInterval.toString()), actualEvent.get(DruidMetrics.INTERVAL));
    Assert.assertEquals("true", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedInterval.toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("", actualEvent.get(DruidMetrics.ID));

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
    CachingEmitter cachingEmitter = new CachingEmitter();
    ServiceEmitter serviceEmitter = new ServiceEmitter("", "", cachingEmitter);
    DefaultGroupByQueryMetrics queryMetrics = new DefaultGroupByQueryMetrics(TestHelper.getJsonMapper());
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(cachingEmitter, serviceEmitter, queryMetrics);
  }
}
