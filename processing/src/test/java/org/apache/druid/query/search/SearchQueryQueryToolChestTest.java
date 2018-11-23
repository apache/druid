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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.junit.Assert;
import org.junit.Test;

public class SearchQueryQueryToolChestTest
{

  @Test
  public void testCacheStrategy() throws Exception
  {
    CacheStrategy<Result<SearchResultValue>, Object, SearchQuery> strategy =
        new SearchQueryQueryToolChest(null, null).getCacheStrategy(
            new SearchQuery(
                new TableDataSource("dummy"),
                null,
                Granularities.ALL,
                1,
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                ImmutableList.of(Druids.DIMENSION_IDENTITY.apply("dim1")),
                new FragmentSearchQuerySpec(ImmutableList.of("a", "b")),
                null,
                null
            )
        );

    final Result<SearchResultValue> result = new Result<>(
        DateTimes.utc(123L),
        new SearchResultValue(ImmutableList.of(new SearchHit("dim1", "a")))
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result
    );

    ObjectMapper objectMapper = new DefaultObjectMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<SearchResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result, fromCacheResult);
  }
}
