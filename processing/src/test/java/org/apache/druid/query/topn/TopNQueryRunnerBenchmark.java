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
import java.util.Collections;
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

  private static final String marketDimension = "market";
  private static final SegmentId segmentId = SegmentId.dummy("testSegment");

  private static final HashMap<String, Object> context = new HashMap<String, Object>();

  private static final TopNQuery query = new TopNQueryBuilder()
      .dataSource(QueryRunnerTestHelper.dataSource)
      .granularity(QueryRunnerTestHelper.allGran)
      .dimension(marketDimension)
      .metric(QueryRunnerTestHelper.indexMetric)
      .threshold(4)
      .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
      .aggregators(
          Lists.newArrayList(
              Iterables.concat(
                  QueryRunnerTestHelper.commonDoubleAggregators,
                  Lists.newArrayList(
                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                      new DoubleMinAggregatorFactory("minIndex", "index")
                  )
              )
          )
      )
      .postAggregators(Collections.singletonList(QueryRunnerTestHelper.addRowsIndexConstant))
      .build();
  private static final Map<TestCases, QueryRunner> testCaseMap = new HashMap<>();

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
        new TopNQueryQueryToolChest(new TopNQueryConfig(), QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    testCaseMap.put(
        TestCases.rtIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), segmentId),
            null
        )
    );
    testCaseMap.put(
        TestCases.mMappedTestIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), segmentId),
            null
        )
    );
    testCaseMap.put(
        TestCases.mergedRealtimeIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(TestIndex.mergedRealtimeIndex(), segmentId),
            null
        )
    );
    //Thread.sleep(10000);
  }

  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testmMapped()
  {
    testCaseMap.get(TestCases.mMappedTestIndex).run(QueryPlus.wrap(query), context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testrtIndex()
  {
    testCaseMap.get(TestCases.rtIndex).run(QueryPlus.wrap(query), context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testMerged()
  {
    testCaseMap.get(TestCases.mergedRealtimeIndex).run(QueryPlus.wrap(query), context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testOffHeap()
  {
    testCaseMap.get(TestCases.rtIndexOffheap).run(QueryPlus.wrap(query), context);
  }
}
