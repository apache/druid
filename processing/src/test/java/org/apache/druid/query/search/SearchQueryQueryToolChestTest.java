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
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SearchQueryQueryToolChestTest extends InitializedNullHandlingTest
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
                VirtualColumns.EMPTY,
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

  @Test
  public void testCacheStrategyVirtualColumns()
  {
    SearchQueryQueryToolChest toolChest = new SearchQueryQueryToolChest(null, null);
    SearchQuery query1 = new SearchQuery(
        new TableDataSource("dummy"),
        null,
        Granularities.ALL,
        1,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        ImmutableList.of(Druids.DIMENSION_IDENTITY.apply("v0")),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn("v0", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            )
        ),
        new FragmentSearchQuerySpec(ImmutableList.of("a", "b")),
        null,
        null
    );

    SearchQuery query2 = new SearchQuery(
        new TableDataSource("dummy"),
        null,
        Granularities.ALL,
        1,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        ImmutableList.of(Druids.DIMENSION_IDENTITY.apply("v0")),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn("v0", "concat(dim2, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            )
        ),
        new FragmentSearchQuerySpec(ImmutableList.of("a", "b")),
        null,
        null
    );

    SearchQuery query3 = new SearchQuery(
        new TableDataSource("dummy"),
        null,
        Granularities.ALL,
        1,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        ImmutableList.of(Druids.DIMENSION_IDENTITY.apply("v0")),
        VirtualColumns.create(
            ImmutableList.of(
                new ExpressionVirtualColumn("v0", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
            )
        ),
        new FragmentSearchQuerySpec(ImmutableList.of("a", "b")),
        null,
        null
    );

    Assert.assertArrayEquals(
        toolChest.getCacheStrategy(query1).computeCacheKey(query1),
        toolChest.getCacheStrategy(query1).computeCacheKey(query1)
    );

    Assert.assertArrayEquals(
        toolChest.getCacheStrategy(query2).computeCacheKey(query2),
        toolChest.getCacheStrategy(query2).computeCacheKey(query2)
    );

    Assert.assertArrayEquals(
        toolChest.getCacheStrategy(query1).computeCacheKey(query1),
        toolChest.getCacheStrategy(query3).computeCacheKey(query3)
    );

    Assert.assertFalse(
        Arrays.equals(
            toolChest.getCacheStrategy(query1).computeCacheKey(query1),
            toolChest.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
  }
}
