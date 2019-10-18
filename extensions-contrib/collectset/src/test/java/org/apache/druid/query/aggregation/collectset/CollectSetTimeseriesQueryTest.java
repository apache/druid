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

package org.apache.druid.query.aggregation.collectset;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.druid.query.aggregation.collectset.CollectSetTestHelper.DIMENSIONS;

public class CollectSetTimeseriesQueryTest
{
  @Test
  public void testTimeseriesQueryWithCollectSetAgg() throws Exception
  {
    TimeseriesQueryEngine engine = new TimeseriesQueryEngine();

    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .build()
        )
        .setMaxRowCount(1000)
        .buildOnheap();

    DateTime time = DateTimes.of("2000-01-01T00:00:00.000Z");

    for (InputRow inputRow : CollectSetTestHelper.INPUT_ROWS) {
      index.add(inputRow);
    }

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Lists.newArrayList(
                                          new CollectSetAggregatorFactory(DIMENSIONS[0], DIMENSIONS[0]),
                                          new CollectSetAggregatorFactory(DIMENSIONS[1], DIMENSIONS[1]),
                                          new CollectSetAggregatorFactory(DIMENSIONS[2], DIMENSIONS[2])
                                      )
                                  )
                                  .build();

    final Iterable<Result<TimeseriesResultValue>> results =
        engine.process(query, new IncrementalIndexStorageAdapter(index)).toList();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            time,
            new TimeseriesResultValue(
                ImmutableMap.of(
                    DIMENSIONS[0], Sets.newHashSet("0", "1", "2"),
                    DIMENSIONS[1], Sets.newHashSet("android", "iphone"),
                    DIMENSIONS[2], Sets.newHashSet("text", "video", "image"))
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
