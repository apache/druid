/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest
{
  interface IndexCreator
  {
    public IncrementalIndex createIndex(AggregatorFactory[] aggregatorFactories);
  }

  private final IndexCreator indexCreator;

  public IncrementalIndexTest(
      IndexCreator indexCreator
  )
  {
    this.indexCreator = indexCreator;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[][]{
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex(AggregatorFactory[] factories)
                  {
                    return IncrementalIndexTest.createIndex(true, factories);
                  }
                }
            },
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex(AggregatorFactory[] factories)
                  {
                    return IncrementalIndexTest.createIndex(false, factories);
                  }
                }
            }

        }
    );
  }

  public static IncrementalIndex createIndex(boolean offheap, AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = defaultAggregatorFactories;
    }
    if (offheap) {
      return new OffheapIncrementalIndex(
          0L,
          QueryGranularity.NONE,
          aggregatorFactories,
          TestQueryRunners.pool,
          true,
          100 * 1024 * 1024
      );
    } else {
      return new OnheapIncrementalIndex(
          0L, QueryGranularity.NONE, aggregatorFactories, 1000000
      );
    }
  }

  public static void populateIndex(long timestamp, IncrementalIndex index) throws IndexSizeExceededException
  {
    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2")
        )
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", "3", "dim2", "4")
        )
    );
  }

  public static MapBasedInputRow getRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, dimName + rowID);
    }
    dimensionList.add("DimLong");
    builder.put("DimLong", (long) rowID);
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, i);
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static final AggregatorFactory[] defaultAggregatorFactories = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  @Test
  public void testCaseSensitivity() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = indexCreator.createIndex(defaultAggregatorFactories);
    populateIndex(timestamp, index);
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), index.getDimensions());
    Assert.assertEquals(2, index.size());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("1"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("2"), row.getDimension("dim2"));

    row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("3"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("4"), row.getDimension("dim2"));
  }

  @Test(timeout = 60000)
  public void testConcurrentAddRead() throws InterruptedException, ExecutionException
  {
    final int dimensionCount = 5;
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
      ingestAggregatorFactories.add(
          new LongSumAggregatorFactory(
              String.format("sumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
      ingestAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              String.format("doubleSumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
    }

    final ArrayList<AggregatorFactory> queryAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    queryAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
      queryAggregatorFactories.add(
          new LongSumAggregatorFactory(
              String.format("sumResult%s", i),
              String.format("sumResult%s", i)
          )
      );
      queryAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              String.format("doubleSumResult%s", i),
              String.format("doubleSumResult%s", i)
          )
      );
    }


    final IncrementalIndex index = indexCreator.createIndex(ingestAggregatorFactories.toArray(new AggregatorFactory[dimensionCount]));
    final int taskCount = 30;
    final int concurrentThreads = 3;
    final int elementsPerThread = 100;
    final ListeningExecutorService indexExecutor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            concurrentThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("index-executor-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            concurrentThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("query-executor-%d")
                .build()
        )
    );
    final long timestamp = System.currentTimeMillis();
    final Interval queryInterval = new Interval("1900-01-01T00:00:00Z/2900-01-01T00:00:00Z");
    final List<ListenableFuture<?>> indexFutures = new LinkedList<>();
    final List<ListenableFuture<?>> queryFutures = new LinkedList<>();
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(new QueryConfig()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final AtomicInteger currentlyRunning = new AtomicInteger(0);
    final AtomicBoolean concurrentlyRan = new AtomicBoolean(false);
    final AtomicInteger someoneRan = new AtomicInteger(0);
    for (int j = 0; j < taskCount; j++) {
      indexFutures.add(
          indexExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  currentlyRunning.incrementAndGet();
                  try {
                    for (int i = 0; i < elementsPerThread; i++) {
                      index.add(getLongRow(timestamp + i, i, dimensionCount));
                      someoneRan.incrementAndGet();
                    }
                  }
                  catch (IndexSizeExceededException e) {
                    throw Throwables.propagate(e);
                  }
                  currentlyRunning.decrementAndGet();
                }
              }
          )
      );
      queryFutures.add(
          queryExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
                      factory.createRunner(incrementalIndexSegment),
                      factory.getToolchest()
                  );
                  TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                                .dataSource("xxx")
                                                .granularity(QueryGranularity.ALL)
                                                .intervals(ImmutableList.of(queryInterval))
                                                .aggregators(queryAggregatorFactories)
                                                .build();
                  Map<String, Object> context = new HashMap<String, Object>();
                  for (Result<TimeseriesResultValue> result :
                      Sequences.toList(runner.run(query, context), new LinkedList<Result<TimeseriesResultValue>>())
                      ) {
                    final Integer maxRanCount = someoneRan.get()
                                                + concurrentThreads; // is race condition between the index.add and the incrementAndGet
                    if (maxRanCount > 0) {
                      final Double sumResult = result.getValue().getDoubleMetric("doubleSumResult0");
                      // Eventually consistent, but should be somewhere in that range
                      // Actual result is validated after all writes are guaranteed done.
                      Assert.assertTrue(
                          String.format("%d >= %g >= 0 violated", maxRanCount, sumResult),
                          sumResult >= 0 && sumResult <= maxRanCount
                      );
                    }
                  }
                  if (currentlyRunning.get() > 0) {
                    concurrentlyRan.set(true);
                  }
                }
              }
          )
      );
    }
    List<ListenableFuture<?>> allFutures = new ArrayList<>(queryFutures.size() + indexFutures.size());
    allFutures.addAll(queryFutures);
    allFutures.addAll(indexFutures);
    Futures.allAsList(allFutures).get();
    Assume.assumeTrue("Did not hit concurrency, please try again", concurrentlyRan.get());
    queryExecutor.shutdown();
    indexExecutor.shutdown();
    QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
        factory.createRunner(incrementalIndexSegment),
        factory.getToolchest()
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(ImmutableList.of(queryInterval))
                                  .aggregators(queryAggregatorFactories)
                                  .build();
    Map<String, Object> context = new HashMap<String, Object>();
    List<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, context),
        new LinkedList<Result<TimeseriesResultValue>>()
    );
    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(elementsPerThread, result.getValue().getLongMetric("rows").intValue());
      for (int i = 0; i < dimensionCount; ++i) {
        Assert.assertEquals(
            String.format("Failed long sum on dimension %d", i),
            elementsPerThread * taskCount * i,
            result.getValue().getLongMetric(String.format("sumResult%s", i)).intValue()
        );
        Assert.assertEquals(
            String.format("Failed double sum on dimension %d", i),
            elementsPerThread * taskCount * i,
            result.getValue().getDoubleMetric(String.format("doubleSumResult%s", i)).intValue()
        );
      }
      Assert.assertEquals(elementsPerThread, result.getValue().getLongMetric("rows").intValue());
    }
  }

  @Test
  public void testConcurrentAdd() throws ExecutionException, InterruptedException
  {
    final IncrementalIndex index = indexCreator.createIndex(
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("DimLongSum", "DimLong")
        }
    );
    final int threadCount = 10;
    final int elementsPerThread = 1000;
    final int dimensionCount = 5;
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    final long timestamp = System.currentTimeMillis();
    final Collection<ListenableFuture<?>> futures = new LinkedList<>();
    for (int j = 0; j < threadCount; j++) {
      futures.add(
          executor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  for (int i = 0; i < elementsPerThread; i++) {
                    try {
                      index.add(getRow(timestamp + i, i, dimensionCount));
                    }
                    catch (IndexSizeExceededException e) {
                      throw Throwables.propagate(e);
                    }
                  }
                }
              }
          )
      );
    }
    Futures.allAsList(futures).get();
    Assert.assertEquals(dimensionCount + 1, index.getDimensions().size());
    Assert.assertEquals(index.getFacts().size(), index.size());
    Iterator<Row> iterator = index.iterator();
    int curr = 0;
    long sum = 0l;
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Assert.assertEquals(timestamp + curr, row.getTimestampFromEpoch());
      sum += row.getLongMetric("count");
      Assert.assertEquals("Long Sum didn't add up", curr * threadCount, row.getLongMetric("DimLongSum"));
      Assert.assertEquals(
          String.format("Row count not equal to thread count on row %d", curr + 1),
          threadCount,
          row.getLongMetric("count")
      );
      curr++;
    }
    Assert.assertEquals(elementsPerThread * threadCount, sum);
  }

  @Test
  public void testOffheapIndexIsFull() throws IndexSizeExceededException
  {
    if (!(indexCreator.createIndex(null) instanceof OffheapIncrementalIndex)) {
      // Skip for the on-heap tests
      return;
    }
    OffheapIncrementalIndex index = new OffheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        TestQueryRunners.pool,
        true,
        (10 + 2) * 1024 * 1024
    );
    int rowCount = 0;
    for (int i = 0; i < 500; i++) {
      rowCount = index.add(getRow(System.currentTimeMillis(), i, 100));
      if (!index.canAppendRow()) {
        break;
      }
    }

    Assert.assertTrue("rowCount : " + rowCount, rowCount > 200 && rowCount < 600);
  }
}
