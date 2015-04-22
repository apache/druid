/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TopNQueryQueryToolChestTest
{

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> strategy =
        new TopNQueryQueryToolChest(null, null).getCacheStrategy(
            new TopNQuery(
                new TableDataSource("dummy"),
                new DefaultDimensionSpec("test", "test"),
                new NumericTopNMetricSpec("metric1"),
                3,
                new MultipleIntervalSegmentSpec(
                    ImmutableList.of(
                        new Interval(
                            "2015-01-01/2015-01-02"
                        )
                    )
                ),
                null,
                QueryGranularity.ALL,
                ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("metric1")),
                null,
                null
            )
        );

    final Result<TopNResultValue> result = new Result<>(
        // test timestamps that result in integer size millis
        new DateTime(123L),
        new TopNResultValue(
            Arrays.asList(
                ImmutableMap.<String, Object>of(
                    "test", "val1",
                    "metric1", 2
                )
            )
        )
    );

    Object preparedValue = strategy.prepareForCache().apply(
        result
    );

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromCacheResult = strategy.pullFromCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
