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

package io.druid.query.aggregation.distinctcount;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DistinctCountTopNQueryTest
{

  @Test
  public void testTopNWithDistinctCountAgg() throws Exception
  {
    TopNQueryEngine engine = new TopNQueryEngine(
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    IncrementalIndex index = new OnheapIncrementalIndex(
        0, QueryGranularities.SECOND, new AggregatorFactory[]{new CountAggregatorFactory("cnt")}, 1000
    );
    String visitor_id = "visitor_id";
    String client_type = "client_type";
    DateTime time = new DateTime("2016-03-04T00:00:00.000Z");
    long timestamp = time.getMillis();
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "0", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "1", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "2", client_type, "android")
        )
    );

    TopNQuery query = new TopNQueryBuilder().dataSource(QueryRunnerTestHelper.dataSource)
                          .granularity(QueryRunnerTestHelper.allGran)
                          .intervals(QueryRunnerTestHelper.fullOnInterval)
                          .dimension(client_type)
                          .metric("UV")
                          .threshold(10)
                          .aggregators(
                              Lists.newArrayList(
                                  QueryRunnerTestHelper.rowsCount,
                                  new DistinctCountAggregatorFactory("UV", visitor_id, null)
                              )
                          )
                          .build();

    final Iterable<Result<TopNResultValue>> results = Sequences.toList(
        engine.query(
            query,
            new IncrementalIndexStorageAdapter(index)
        ),
        Lists.<Result<TopNResultValue>>newLinkedList()
    );

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            time,
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        client_type, "iphone",
                        "UV", 2L,
                        "rows", 2L
                    ),
                    ImmutableMap.<String, Object>of(
                        client_type, "android",
                        "UV", 1L,
                        "rows", 1L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
