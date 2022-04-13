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
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery.Builder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GroupByQueryBuilderTest
{
  private Builder builder;

  @Before
  public void setup()
  {
    builder = new Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setQuerySegmentSpec(QueryRunnerTestHelper.EMPTY_INTERVAL)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION, QueryRunnerTestHelper.MARKET_DIMENSION)
        )
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("index", "index"));
  }

  @Test
  public void testQueryIdWhenContextInBuilderIsNullReturnContextContainingQueryId()
  {
    final GroupByQuery query = builder
        .queryId("queryId")
        .build();
    Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId"), query.getContext());
  }

  @Test
  public void testQueryIdWhenBuilderHasNonnullContextWithoutQueryIdReturnMergedContext()
  {
    final GroupByQuery query = builder
        .setContext(ImmutableMap.of("my", "context"))
        .queryId("queryId")
        .build();
    Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "queryId", "my", "context"), query.getContext());
  }

  @Test
  public void testQueryIdWhenBuilderHasNonnullContextWithQueryIdReturnMergedContext()
  {
    final GroupByQuery query = builder
        .setContext(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "queryId"))
        .queryId("realQueryId")
        .build();
    Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
  }

  @Test
  public void testContextAfterSettingQueryIdReturnContextWithoutQueryId()
  {
    final GroupByQuery query = builder
        .queryId("queryId")
        .setContext(ImmutableMap.of("my", "context"))
        .build();
    Assert.assertEquals(ImmutableMap.of("my", "context"), query.getContext());
  }

  @Test
  public void testContextContainingQueryIdAfterSettingQueryIdOverwriteQueryId()
  {
    final GroupByQuery query = builder
        .queryId("queryId")
        .setContext(ImmutableMap.of("my", "context", BaseQuery.QUERY_ID, "realQueryId"))
        .build();
    Assert.assertEquals(ImmutableMap.of(BaseQuery.QUERY_ID, "realQueryId", "my", "context"), query.getContext());
  }
}
