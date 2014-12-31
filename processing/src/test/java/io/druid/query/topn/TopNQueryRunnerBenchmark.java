/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.StupidPool;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Based on TopNQueryRunnerTest
 */
public class TopNQueryRunnerBenchmark extends AbstractBenchmark
{

  public static enum TestCases
  {
    rtIndex, mMappedTestIndex, mergedRealtimeIndex, rtIndexOffheap
  }

  private static final String marketDimension = "market";
  private static final String segmentId = "testSegment";

  private static final HashMap<String, Object> context = new HashMap<String, Object>();

  private static final TopNQuery query = new TopNQueryBuilder()
      .dataSource(QueryRunnerTestHelper.dataSource)
      .granularity(QueryRunnerTestHelper.allGran)
      .dimension(marketDimension)
      .metric(QueryRunnerTestHelper.indexMetric)
      .threshold(4)
      .intervals(QueryRunnerTestHelper.fullOnInterval)
      .aggregators(
          Lists.<AggregatorFactory>newArrayList(
              Iterables.concat(
                  QueryRunnerTestHelper.commonAggregators,
                  Lists.newArrayList(
                      new MaxAggregatorFactory("maxIndex", "index"),
                      new MinAggregatorFactory("minIndex", "index")
                  )
              )
          )
      )
      .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
      .build();
  private static final Map<TestCases, QueryRunner> testCaseMap = Maps.newHashMap();

  @BeforeClass
  public static void setUp() throws Exception
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        new StupidPool<ByteBuffer>(
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
    testCaseMap.put(
        TestCases.rtIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(false), segmentId)
        )
    );
    testCaseMap.put(
        TestCases.mMappedTestIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(segmentId, TestIndex.getMMappedTestIndex())
        )
    );
    testCaseMap.put(
        TestCases.mergedRealtimeIndex,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new QueryableIndexSegment(segmentId, TestIndex.mergedRealtimeIndex())
        )
    );
    testCaseMap.put(
        TestCases.rtIndexOffheap,
        QueryRunnerTestHelper.makeQueryRunner(
            factory,
            new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(true), segmentId)
        )
    );
    //Thread.sleep(10000);
  }

  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testmMapped()
  {
    testCaseMap.get(TestCases.mMappedTestIndex).run(query, context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testrtIndex()
  {
    testCaseMap.get(TestCases.rtIndex).run(query, context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testMerged()
  {
    testCaseMap.get(TestCases.mergedRealtimeIndex).run(query, context);
  }

  @Ignore
  @BenchmarkOptions(warmupRounds = 10000, benchmarkRounds = 10000)
  @Test
  public void testOffHeap()
  {
    testCaseMap.get(TestCases.rtIndexOffheap).run(query, context);
  }
}
