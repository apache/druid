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

package org.apache.druid.query.topn;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.timeline.SegmentId;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Based on TopNQueryRunnerTest
 */
public class TopNQueryRunnerBenchmark extends AbstractBenchmark
{

  public enum TestCases
  {
    rtIndex, mMappedTestIndex, mergedRealtimeIndex, rtIndexOffheap
  }

  private static final String MARKET_DIMENSION = "market";
  private static final SegmentId SEGMENT_ID = SegmentId.dummy("testSegment");

  private static final TopNQuery QUERY = new TopNQueryBuilder()
      .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
      .granularity(QueryRunnerTestHelper.ALL_GRAN)
      .dimension(MARKET_DIMENSION)
      .metric(QueryRunnerTestHelper.INDEX_METRIC)
      .threshold(4)
      .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
      .aggregators(
          Lists.newArrayList(
              Iterables.concat(
                  QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS,
                  Lists.newArrayList(
                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                      new DoubleMinAggregatorFactory("minIndex", "index")
                  )
              )
          )
      )
      .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
      .build();
  private static final Map<TestCases, QueryRunner> TEST_CASE_MAP = new HashMap<>();

  @BeforeClass
  public static void setUp()
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        new StupidPool<ByteBuffer>(
            "TopNQueryRunnerFactory-directBufferPool",
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                // See OffheapByteBufferPool
                // Instead of causing a circular dependency, we simply mimic its behavior
                return ByteBuffer.allocateDirect(2000);
              }
            }
        ),
        new TopNQueryQueryToolChest(new TopNQueryConfig()),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    TEST_CASE_MAP.put(
        TestCases.rtIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), SEGMENT_ID),
            null
        )
    );
    TEST_CASE_MAP.put(
        TestCases.mMappedTestIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SEGMENT_ID),
            null
        )
    );
    TEST_CASE_MAP.put(
        TestCases.mergedRealtimeIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(TestIndex.mergedRealtimeIndex(), SEGMENT_ID),
            null
        )
    );
    //Thread.sleep(10000);
  }

  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testmMapped()
  {
    TEST_CASE_MAP.get(TestCases.mMappedTestIndex).run(QueryPlus.wrap(QUERY));
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testrtIndex()
  {
    TEST_CASE_MAP.get(TestCases.rtIndex).run(QueryPlus.wrap(QUERY));
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testMerged()
  {
    TEST_CASE_MAP.get(TestCases.mergedRealtimeIndex).run(QueryPlus.wrap(QUERY));
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testOffHeap()
  {
    TEST_CASE_MAP.get(TestCases.rtIndexOffheap).run(QueryPlus.wrap(QUERY));
  }
}
