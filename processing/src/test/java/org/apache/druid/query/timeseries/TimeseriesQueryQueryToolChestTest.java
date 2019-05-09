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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class TimeseriesQueryQueryToolChestTest
{
  private static final TimeseriesQueryQueryToolChest TOOL_CHEST = new TimeseriesQueryQueryToolChest(null);

  @Parameterized.Parameters(name = "descending={0}")
  public static Iterable<Object[]> constructorFeeder()
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
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                descending,
                VirtualColumns.EMPTY,
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new CountAggregatorFactory("metric1"),
                    new LongSumAggregatorFactory("metric0", "metric0"),
                    new StringLastAggregatorFactory("complexMetric", "test", null)
                ),
                ImmutableList.of(new ConstantPostAggregator("post", 10)),
                0,
                null
            )
        );

    final Result<TimeseriesResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TimeseriesResultValue(
            ImmutableMap.of(
                "metric1", 2,
                "metric0", 3,
                "complexMetric", new SerializablePairLongString(123L, "val1")
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result1);

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeseriesResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    final Result<TimeseriesResultValue> result2 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TimeseriesResultValue(
            ImmutableMap.of(
                "metric1", 2,
                "metric0", 3,
                "complexMetric", "val1",
                "post", 10
            )
        )
    );

    Object preparedResultLevelCacheValue = strategy.prepareForCache(true).apply(result2);
    Object fromResultLevelCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultLevelCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeseriesResultValue> fromResultLevelCacheRes = strategy.pullFromCache(true).apply(fromResultLevelCacheValue);
    Assert.assertEquals(result2, fromResultLevelCacheRes);

    final Result<TimeseriesResultValue> result3 = new Result<>(
        // null timestamp similar to grandTotal
        null,
        new TimeseriesResultValue(
            ImmutableMap.of("metric1", 2, "metric0", 3, "complexMetric", "val1", "post", 10)
        )
    );

    preparedResultLevelCacheValue = strategy.prepareForCache(true).apply(result3);
    fromResultLevelCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultLevelCacheValue),
        strategy.getCacheObjectClazz()
    );

    fromResultLevelCacheRes = strategy.pullFromCache(true).apply(fromResultLevelCacheValue);
    Assert.assertEquals(result3, fromResultLevelCacheRes);
  }

  @Test
  public void testCacheKey()
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

    // Test for https://github.com/apache/incubator-druid/issues/4093.
    Assert.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
  }

  @Test
  public void testResultLevelCacheKey()
  {
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
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
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "+",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
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
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "/",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .build();

    Assert.assertTrue(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
    Assert.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeResultLevelCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeResultLevelCacheKey(query2)
        )
    );
  }

  @Test
  public void testResultLevelCacheKeyWithGrandTotal()
  {
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
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
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "+",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
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
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "/",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
                                         .build();

    Assert.assertTrue(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
    Assert.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeResultLevelCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeResultLevelCacheKey(query2)
        )
    );
  }
}
