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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class DistinctCountTimeseriesQueryTest
{

  @Test
  public void testTopNWithDistinctCountAgg() throws Exception
  {
    TimeseriesQueryEngine engine =  new TimeseriesQueryEngine();

    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .withMetrics(new CountAggregatorFactory("cnt"))
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

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

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Lists.newArrayList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new DistinctCountAggregatorFactory("UV", visitor_id, null)
                                      )
                                  )
                                  .build();

    final Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        engine.process(
            query,
            new IncrementalIndexStorageAdapter(index)
        ),
        Lists.<Result<TimeseriesResultValue>>newLinkedList()
    );

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            time,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("UV", 3, "rows", 3L)
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
