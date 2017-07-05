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

package io.druid.segment.incremental;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.Clock;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
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
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extending AbstractBenchmark means only runs if explicitly called
 */
@RunWith(Parameterized.class)
public class OnheapIncrementalIndexBenchmark extends AbstractBenchmark
{
  private static AggregatorFactory[] factories;
  static final int dimensionCount = 5;

  static {

    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
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

  private static final class MapIncrementalIndex extends OnheapIncrementalIndex
  {
    private final AtomicInteger indexIncrement = new AtomicInteger(0);
    ConcurrentHashMap<Integer, Aggregator[]> indexedMap = new ConcurrentHashMap<Integer, Aggregator[]>();

    public MapIncrementalIndex(
        IncrementalIndexSchema incrementalIndexSchema,
        boolean deserializeComplexMetrics,
        boolean reportParseExceptions,
        boolean concurrentEventAdd,
        boolean sortFacts,
        int maxRowCount
    )
    {
      super(
          incrementalIndexSchema,
          deserializeComplexMetrics,
          reportParseExceptions,
          concurrentEventAdd,
          sortFacts,
          maxRowCount
      );
    }

    public MapIncrementalIndex(
        long minTimestamp,
        Granularity gran,
        AggregatorFactory[] metrics,
        int maxRowCount
    )
    {
      super(
          new IncrementalIndexSchema.Builder()
            .withMinTimestamp(minTimestamp)
            .withQueryGranularity(gran)
            .withMetrics(metrics)
            .build(),
        true,
        true,
        false,
        true,
        maxRowCount
      );
    }

    @Override
    protected Aggregator[] concurrentGet(int offset)
    {
      // All get operations should be fine
      return indexedMap.get(offset);
    }

    @Override
    protected void concurrentSet(int offset, Aggregator[] value)
    {
      indexedMap.put(offset, value);
    }

    @Override
    protected Integer addToFacts(
        AggregatorFactory[] metrics,
        boolean deserializeComplexMetrics,
        boolean reportParseExceptions,
        InputRow row,
        AtomicInteger numEntries,
        TimeAndDims key,
        ThreadLocal<InputRow> rowContainer,
        Supplier<InputRow> rowSupplier
    ) throws IndexSizeExceededException
    {

      final Integer priorIdex = getFacts().getPriorIndex(key);

      Aggregator[] aggs;

      if (null != priorIdex) {
        aggs = indexedMap.get(priorIdex);
      } else {
        aggs = new Aggregator[metrics.length];

        for (int i = 0; i < metrics.length; i++) {
          final AggregatorFactory agg = metrics[i];
          aggs[i] = agg.factorize(
              makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics)
          );
        }
        Integer rowIndex;

        do {
          rowIndex = indexIncrement.incrementAndGet();
        } while (null != indexedMap.putIfAbsent(rowIndex, aggs));


        // Last ditch sanity checks
        if (numEntries.get() >= maxRowCount && getFacts().getPriorIndex(key) == TimeAndDims.EMPTY_ROW_INDEX) {
          throw new IndexSizeExceededException("Maximum number of rows reached");
        }
        final int prev = getFacts().putIfAbsent(key, rowIndex);
        if (TimeAndDims.EMPTY_ROW_INDEX == prev) {
          numEntries.incrementAndGet();
        } else {
          // We lost a race
          aggs = indexedMap.get(prev);
          // Free up the misfire
          indexedMap.remove(rowIndex);
          // This is expected to occur ~80% of the time in the worst scenarios
        }
      }

      rowContainer.set(row);

      for (Aggregator agg : aggs) {
        synchronized (agg) {
          try {
            agg.aggregate();
          }
          catch (ParseException e) {
            // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
            if (reportParseExceptions) {
              throw e;
            }
          }
        }
      }

      rowContainer.set(null);


      return numEntries.get();
    }

    @Override
    public int getLastRowIndex()
    {
      return indexIncrement.get() - 1;
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        new Object[]{OnheapIncrementalIndex.class},
        new Object[]{MapIncrementalIndex.class}
    );
  }

  private final Class<? extends OnheapIncrementalIndex> incrementalIndex;

  public OnheapIncrementalIndexBenchmark(Class<? extends OnheapIncrementalIndex> incrementalIndex)
  {
    this.incrementalIndex = incrementalIndex;
  }


  private static MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, new Integer(rowID).longValue());
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  @Ignore
  @Test
  @BenchmarkOptions(callgc = true, clock = Clock.REAL_TIME, warmupRounds = 10, benchmarkRounds = 20)
  public void testConcurrentAddRead()
      throws InterruptedException, ExecutionException, NoSuchMethodException, IllegalAccessException,
             InvocationTargetException, InstantiationException
  {

    final int taskCount = 30;
    final int concurrentThreads = 3;
    final int elementsPerThread = 1 << 15;

    final IncrementalIndex incrementalIndex = this.incrementalIndex.getConstructor(
        IncrementalIndexSchema.class,
        boolean.class,
        boolean.class,
        boolean.class,
        boolean.class,
        int.class
    ).newInstance(
        new IncrementalIndexSchema.Builder().withMetrics(factories).build(),
        true,
        true,
        false,
        true,
        elementsPerThread * taskCount
    );
    final ArrayList<AggregatorFactory> queryAggregatorFactories = new ArrayList<>(dimensionCount + 1);
    queryAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < dimensionCount; ++i) {
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
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(incrementalIndex, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final AtomicInteger currentlyRunning = new AtomicInteger(0);
    final AtomicBoolean concurrentlyRan = new AtomicBoolean(false);
    final AtomicBoolean someoneRan = new AtomicBoolean(false);
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
                      incrementalIndex.add(getLongRow(timestamp + i, 1, dimensionCount));
                    }
                  }
                  catch (IndexSizeExceededException e) {
                    throw Throwables.propagate(e);
                  }
                  currentlyRunning.decrementAndGet();
                  someoneRan.set(true);
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
                                                .granularity(Granularities.ALL)
                                                .intervals(ImmutableList.of(queryInterval))
                                                .aggregators(queryAggregatorFactories)
                                                .build();
                  Map<String, Object> context = new HashMap<String, Object>();
                  for (Result<TimeseriesResultValue> result :
                      Sequences.toList(
                          runner.run(query, context),
                          new LinkedList<Result<TimeseriesResultValue>>()
                      )
                      ) {
                    if (someoneRan.get()) {
                      Assert.assertTrue(result.getValue().getDoubleMetric("doubleSumResult0") > 0);
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
    //Assert.assertTrue("Did not hit concurrency, please try again", concurrentlyRan.get());
    queryExecutor.shutdown();
    indexExecutor.shutdown();
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
    Map<String, Object> context = new HashMap<String, Object>();
    List<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, context),
        new LinkedList<Result<TimeseriesResultValue>>()
    );
    final int expectedVal = elementsPerThread * taskCount;
    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(elementsPerThread, result.getValue().getLongMetric("rows").intValue());
      for (int i = 0; i < dimensionCount; ++i) {
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
