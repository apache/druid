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

package org.apache.druid.benchmark.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;
import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark for {@link OnheapIncrementalIndex} doing queries and adds simultaneously.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class OnheapIncrementalIndexBenchmark
{
  static final int DIMENSION_COUNT = 5;

  static {
    NullHandling.initializeForTests();
  }

  /**
   * Number of index and query tasks.
   */
  private final int taskCount = 30;

  /**
   * Number of elements to add for each index task.
   */
  private final int elementsPerAddTask = 1 << 15;

  /**
   * Number of query tasks to run simultaneously.
   */
  private final int queryThreads = 4;

  private AggregatorFactory[] factories;
  private IncrementalIndex incrementalIndex;
  private ListeningExecutorService indexExecutor;
  private ListeningExecutorService queryExecutor;

  private static MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, Integer.valueOf(rowID).longValue());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  @Setup(Level.Trial)
  public void setupFactories()
  {
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(DIMENSION_COUNT + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < DIMENSION_COUNT; ++i) {
      ingestAggregatorFactories.add(
          new LongSumAggregatorFactory(
              StringUtils.format("sumResult%s", i),
              StringUtils.format("Dim_%s", i)
          )
      );
      ingestAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              StringUtils.format("doubleSumResult%s", i),
              StringUtils.format("Dim_%s", i)
          )
      );
    }
    factories = ingestAggregatorFactories.toArray(new AggregatorFactory[0]);
  }

  @Setup(Level.Trial)
  public void setupExecutors()
  {
    indexExecutor = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("index-executor-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    queryExecutor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            queryThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("query-executor-%d")
                .build()
        )
    );
  }

  @Setup(Level.Invocation)
  public void setupIndex()
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException
  {
    final Constructor<? extends OnheapIncrementalIndex> constructor =
        OnheapIncrementalIndex.class.getDeclaredConstructor(
            IncrementalIndexSchema.class,
            int.class,
            long.class,
            boolean.class,
            boolean.class
        );

    constructor.setAccessible(true);

    this.incrementalIndex =
        constructor.newInstance(
            new IncrementalIndexSchema.Builder().withMetrics(factories).build(),
            elementsPerAddTask * taskCount,
            1_000_000_000L,
            false,
            false
        );
  }

  @TearDown(Level.Invocation)
  public void tearDownIndex()
  {
    incrementalIndex.close();
    incrementalIndex = null;
  }

  @TearDown(Level.Trial)
  public void tearDownExecutors() throws InterruptedException
  {
    indexExecutor.shutdown();
    queryExecutor.shutdown();
    if (!indexExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("Could not shut down indexExecutor");
    }
    if (!queryExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("Could not shut down queryExecutor");
    }
    indexExecutor = null;
    queryExecutor = null;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void concurrentAddRead() throws InterruptedException, ExecutionException
  {
    final ArrayList<AggregatorFactory> queryAggregatorFactories = new ArrayList<>(DIMENSION_COUNT + 1);
    queryAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < DIMENSION_COUNT; ++i) {
      queryAggregatorFactories.add(
          new LongSumAggregatorFactory(
              StringUtils.format("sumResult%s", i),
              StringUtils.format("sumResult%s", i)
          )
      );
      queryAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              StringUtils.format("doubleSumResult%s", i),
              StringUtils.format("doubleSumResult%s", i)
          )
      );
    }

    final long timestamp = System.currentTimeMillis();
    final Interval queryInterval = Intervals.of("1900-01-01T00:00:00Z/2900-01-01T00:00:00Z");
    final List<ListenableFuture<?>> indexFutures = new ArrayList<>();
    final List<ListenableFuture<?>> queryFutures = new ArrayList<>();
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(incrementalIndex, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final AtomicInteger currentlyRunning = new AtomicInteger(0);
    final AtomicBoolean concurrentlyRan = new AtomicBoolean(false);
    final AtomicBoolean someoneRan = new AtomicBoolean(false);
    for (int j = 0; j < taskCount; j++) {
      indexFutures.add(
          indexExecutor.submit(
              () -> {
                currentlyRunning.incrementAndGet();
                try {
                  for (int i = 0; i < elementsPerAddTask; i++) {
                    incrementalIndex.add(getLongRow(timestamp + i, 1, DIMENSION_COUNT));
                  }
                }
                catch (IndexSizeExceededException e) {
                  throw new RuntimeException(e);
                }
                currentlyRunning.decrementAndGet();
                someoneRan.set(true);
              }
          )
      );

      queryFutures.add(
          queryExecutor.submit(
              () -> {
                QueryRunner<Result<TimeseriesResultValue>> runner =
                    new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
                        factory.createRunner(incrementalIndexSegment),
                        factory.getToolchest()
                    );
                TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                              .dataSource("xxx")
                                              .granularity(Granularities.ALL)
                                              .intervals(ImmutableList.of(queryInterval))
                                              .aggregators(queryAggregatorFactories)
                                              .build();
                List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
                for (Result<TimeseriesResultValue> result : results) {
                  if (someoneRan.get()) {
                    Assert.assertTrue(result.getValue().getDoubleMetric("doubleSumResult0") > 0);
                  }
                }
                if (currentlyRunning.get() > 0) {
                  concurrentlyRan.set(true);
                }
              }
          )
      );

    }
    List<ListenableFuture<?>> allFutures = new ArrayList<>(queryFutures.size() + indexFutures.size());
    allFutures.addAll(queryFutures);
    allFutures.addAll(indexFutures);
    Futures.allAsList(allFutures).get();
    QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
        factory.createRunner(incrementalIndexSegment),
        factory.getToolchest()
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(queryInterval))
                                  .aggregators(queryAggregatorFactories)
                                  .build();
    List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    final int expectedVal = elementsPerAddTask * taskCount;
    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(elementsPerAddTask, result.getValue().getLongMetric("rows").intValue());
      for (int i = 0; i < DIMENSION_COUNT; ++i) {
        Assert.assertEquals(
            StringUtils.format("Failed long sum on dimension %d", i),
            expectedVal,
            result.getValue().getLongMetric(StringUtils.format("sumResult%s", i)).intValue()
        );
        Assert.assertEquals(
            StringUtils.format("Failed double sum on dimension %d", i),
            expectedVal,
            result.getValue().getDoubleMetric(StringUtils.format("doubleSumResult%s", i)).intValue()
        );
      }
    }
  }
}
