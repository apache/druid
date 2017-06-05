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

package io.druid.query.timeseries;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.CacheStrategy;
import io.druid.query.Druids;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.VirtualColumns;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TimeseriesQueryQueryToolChestTest
{
  private static final TimeseriesQueryQueryToolChest TOOL_CHEST = new TimeseriesQueryQueryToolChest(null);

  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private final boolean descending;

  public TimeseriesQueryQueryToolChestTest(boolean descending)
  {
    this.descending = descending;
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> strategy =
        TOOL_CHEST.getCacheStrategy(
            new TimeseriesQuery(
                new TableDataSource("dummy"),
                new MultipleIntervalSegmentSpec(
                    ImmutableList.of(
                        new Interval(
                            "2015-01-01/2015-01-02"
                        )
                    )
                ),
                descending,
                VirtualColumns.EMPTY,
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new CountAggregatorFactory("metric1"),
                    new LongSumAggregatorFactory("metric0", "metric0")
                ),
                null,
                null
            )
        );

    final Result<TimeseriesResultValue> result = new Result<>(
        // test timestamps that result in integer size millis
        new DateTime(123L),
        new TimeseriesResultValue(
            ImmutableMap.of("metric1", 2, "metric0", 3)
        )
    );

    Object preparedValue = strategy.prepareForCache().apply(result);

    ObjectMapper objectMapper = TestHelper.getJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeseriesResultValue> fromCacheResult = strategy.pullFromCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }

  @Test
  public void testCacheKey() throws Exception
  {
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new CountAggregatorFactory("metric1"),
                                                 new LongSumAggregatorFactory("metric0", "metric0")
                                             )
                                         )
                                         .build();

    final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .build();

    // Test for https://github.com/druid-io/druid/issues/4093.
    Assert.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
  }
}
