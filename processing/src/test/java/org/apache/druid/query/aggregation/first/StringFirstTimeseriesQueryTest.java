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

package org.apache.druid.query.aggregation.first;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.SerializablePairLongStringSerde;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class StringFirstTimeseriesQueryTest extends InitializedNullHandlingTest
{
  private static final String VISITOR_ID = "visitor_id";
  private static final String CLIENT_TYPE = "client_type";
  private static final String FIRST_CLIENT_TYPE = "first_client_type";

  private static final DateTime TIME1 = DateTimes.of("2016-03-04T00:00:00.000Z");
  private static final DateTime TIME2 = DateTimes.of("2016-03-04T01:00:00.000Z");

  private IncrementalIndex incrementalIndex;
  private QueryableIndex queryableIndex;

  @Before
  public void setUp() throws IndexSizeExceededException
  {
    final SerializablePairLongStringSerde serde = new SerializablePairLongStringSerde();
    ComplexMetrics.registerSerde(serde.getTypeName(), serde);

    incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .withMetrics(new CountAggregatorFactory("cnt"))
                .withMetrics(new StringFirstAggregatorFactory(FIRST_CLIENT_TYPE, CLIENT_TYPE, 1024))
                .build()
        )
        .setMaxRowCount(1000)
        .build();

    incrementalIndex.add(
        new MapBasedInputRow(
            TIME1,
            Lists.newArrayList(VISITOR_ID, CLIENT_TYPE),
            ImmutableMap.of(VISITOR_ID, "0", CLIENT_TYPE, "iphone")
        )
    );
    incrementalIndex.add(
        new MapBasedInputRow(
            TIME1,
            Lists.newArrayList(VISITOR_ID, CLIENT_TYPE),
            ImmutableMap.of(VISITOR_ID, "1", CLIENT_TYPE, "iphone")
        )
    );
    incrementalIndex.add(
        new MapBasedInputRow(
            TIME2,
            Lists.newArrayList(VISITOR_ID, CLIENT_TYPE),
            ImmutableMap.of(VISITOR_ID, "0", CLIENT_TYPE, "android")
        )
    );

    queryableIndex = TestIndex.persistRealtimeAndLoadMMapped(incrementalIndex);
  }

  @Test
  public void testTimeseriesQuery()
  {
    TimeseriesQueryEngine engine = new TimeseriesQueryEngine();


    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      ImmutableList.of(
                                          new StringFirstAggregatorFactory("nonfolding", CLIENT_TYPE, 1024),
                                          new StringFirstAggregatorFactory("folding", FIRST_CLIENT_TYPE, 1024),
                                          new StringFirstAggregatorFactory("nonexistent", "nonexistent", 1024),
                                          new StringFirstAggregatorFactory("numeric", "cnt", 1024)
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            TIME1,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("nonfolding", new SerializablePairLongString(TIME1.getMillis(), "iphone"))
                    .put("folding", new SerializablePairLongString(TIME1.getMillis(), "iphone"))
                    .put("nonexistent", new SerializablePairLongString(DateTimes.MAX.getMillis(), null))
                    .put("numeric", new SerializablePairLongString(DateTimes.MAX.getMillis(), null))
                    .build()
            )
        )
    );

    final Iterable<Result<TimeseriesResultValue>> iiResults =
        engine.process(query, new IncrementalIndexStorageAdapter(incrementalIndex)).toList();

    final Iterable<Result<TimeseriesResultValue>> qiResults =
        engine.process(query, new QueryableIndexStorageAdapter(queryableIndex)).toList();

    TestHelper.assertExpectedResults(expectedResults, iiResults, "incremental index");
    TestHelper.assertExpectedResults(expectedResults, qiResults, "queryable index");
  }
}
