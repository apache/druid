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

package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
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
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.CloserRule;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

  @Rule
  public final CloserRule closer = new CloserRule(false);

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
                    return IncrementalIndexTest.createIndex(factories);
                  }
                }
            },
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex(AggregatorFactory[] factories)
                  {
                    return new OffheapIncrementalIndex(
                        0L, QueryGranularities.NONE, factories, 1000000,
                        new StupidPool<ByteBuffer>(
                            new Supplier<ByteBuffer>()
                            {
                              @Override
                              public ByteBuffer get()
                              {
                                return ByteBuffer.allocate(256 * 1024);
                              }
                            }
                        )
                    );
                  }
                }
            },
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex(AggregatorFactory[] factories)
                  {
                    return IncrementalIndexTest.createNoRollupIndex(factories);
                  }
                }
            },
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex(AggregatorFactory[] factories)
                  {
                    return new OffheapIncrementalIndex(
                        0L, QueryGranularities.NONE, false, factories, 1000000,
                        new StupidPool<ByteBuffer>(
                            new Supplier<ByteBuffer>()
                            {
                              @Override
                              public ByteBuffer get()
                              {
                                return ByteBuffer.allocate(256 * 1024);
                              }
                            }
                        )
                    );
                  }
                }
            }

        }
    );
  }

  public static AggregatorFactory[] getDefaultAggregatorFactories()
  {
    return defaultAggregatorFactories;
  }

  public static AggregatorFactory[] getDefaultCombiningAggregatorFactories()
  {
    return defaultCombiningAggregatorFactories;
  }

  public static IncrementalIndex createIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = defaultAggregatorFactories;
    }

    return new OnheapIncrementalIndex(
        0L, QueryGranularities.NONE, aggregatorFactories, 1000000
    );
  }

  public static IncrementalIndex createNoRollupIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = defaultAggregatorFactories;
    }

    return new OnheapIncrementalIndex(
        0L, QueryGranularities.NONE, false, aggregatorFactories, 1000000
    );
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
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static MapBasedInputRow getLongRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, (Long) 1L);
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static final AggregatorFactory[] defaultAggregatorFactories = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  private static final AggregatorFactory[] defaultCombiningAggregatorFactories = new AggregatorFactory[]{
      defaultAggregatorFactories[0].getCombiningFactory()
  };

  @Test
  public void testCaseSensitivity() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = closer.closeLater(indexCreator.createIndex(defaultAggregatorFactories));

    populateIndex(timestamp, index);
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), index.getDimensionNames());
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

  @Test
  public void testFilteredAggregators() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = closer.closeLater(
        indexCreator.createIndex(new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new FilteredAggregatorFactory(
                new CountAggregatorFactory("count_selector_filtered"),
                new SelectorDimFilter("dim2", "2", null)
            ),
            new FilteredAggregatorFactory(
                new CountAggregatorFactory("count_bound_filtered"),
                new BoundDimFilter("dim2", "2", "3", false, true, null, null, StringComparators.NUMERIC)
            ),
            new FilteredAggregatorFactory(
                new CountAggregatorFactory("count_multivaldim_filtered"),
                new SelectorDimFilter("dim3", "b", null)
            ),
            new FilteredAggregatorFactory(
                new CountAggregatorFactory("count_numeric_filtered"),
                new SelectorDimFilter("met1", "11", null)
            )
        })
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2", "dim3"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "dim3", Lists.newArrayList("b", "a"), "met1", 10)
        )
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2", "dim3"),
            ImmutableMap.<String, Object>of("dim1", "3", "dim2", "4", "dim3", Lists.newArrayList("c", "d"), "met1", 11)
        )
    );

    Assert.assertEquals(Arrays.asList("dim1", "dim2", "dim3"), index.getDimensionNames());
    Assert.assertEquals(
        Arrays.asList(
            "count",
            "count_selector_filtered",
            "count_bound_filtered",
            "count_multivaldim_filtered",
            "count_numeric_filtered"
        ),
        index.getMetricNames()
    );
    Assert.assertEquals(2, index.size());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("1"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("2"), row.getDimension("dim2"));
    Assert.assertEquals(Arrays.asList("a", "b"), row.getDimension("dim3"));
    Assert.assertEquals(1L, row.getLongMetric("count"));
    Assert.assertEquals(1L, row.getLongMetric("count_selector_filtered"));
    Assert.assertEquals(1L, row.getLongMetric("count_bound_filtered"));
    Assert.assertEquals(1L, row.getLongMetric("count_multivaldim_filtered"));
    Assert.assertEquals(0L, row.getLongMetric("count_numeric_filtered"));

    row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("3"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("4"), row.getDimension("dim2"));
    Assert.assertEquals(Arrays.asList("c", "d"), row.getDimension("dim3"));
    Assert.assertEquals(1L, row.getLongMetric("count"));
    Assert.assertEquals(0L, row.getLongMetric("count_selector_filtered"));
    Assert.assertEquals(0L, row.getLongMetric("count_bound_filtered"));
    Assert.assertEquals(0L, row.getLongMetric("count_multivaldim_filtered"));
    Assert.assertEquals(1L, row.getLongMetric("count_numeric_filtered"));
  }

  @Test
  public void testSingleThreadedIndexingAndQuery() throws Exception
  {
    final int dimensionCount = 5;
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>();
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

    final IncrementalIndex index = closer.closeLater(
        indexCreator.createIndex(
            ingestAggregatorFactories.toArray(
                new AggregatorFactory[ingestAggregatorFactories.size()]
            )
        )
    );

    final long timestamp = System.currentTimeMillis();

    final int rows = 50;

    //ingesting same data twice to have some merging happening
    for (int i = 0; i < rows; i++) {
      index.add(getLongRow(timestamp + i, i, dimensionCount));
    }

    for (int i = 0; i < rows; i++) {
      index.add(getLongRow(timestamp + i, i, dimensionCount));
    }

    //run a timeseries query on the index and verify results
    final ArrayList<AggregatorFactory> queryAggregatorFactories = new ArrayList<>();
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

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(ImmutableList.of(new Interval("2000/2030")))
                                  .aggregators(queryAggregatorFactories)
                                  .build();

    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
        factory.createRunner(incrementalIndexSegment),
        factory.getToolchest()
    );


    List<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, new HashMap<String, Object>()),
        new LinkedList<Result<TimeseriesResultValue>>()
    );
    Result<TimeseriesResultValue> result = Iterables.getOnlyElement(results);
    boolean isRollup = index.isRollup();
    Assert.assertEquals(rows * (isRollup ? 1 : 2), result.getValue().getLongMetric("rows").intValue());
    for (int i = 0; i < dimensionCount; ++i) {
      Assert.assertEquals(
          String.format("Failed long sum on dimension %d", i),
          2*rows,
          result.getValue().getLongMetric(String.format("sumResult%s", i)).intValue()
      );
      Assert.assertEquals(
          String.format("Failed double sum on dimension %d", i),
          2*rows,
          result.getValue().getDoubleMetric(String.format("doubleSumResult%s", i)).intValue()
      );
    }
  }

  @Test(timeout = 60_000L)
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


    final IncrementalIndex index = closer.closeLater(
        indexCreator.createIndex(ingestAggregatorFactories.toArray(new AggregatorFactory[dimensionCount]))
    );
    final int concurrentThreads = 2;
    final int elementsPerThread = 10_000;
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
    final List<ListenableFuture<?>> indexFutures = Lists.newArrayListWithExpectedSize(concurrentThreads);
    final List<ListenableFuture<?>> queryFutures = Lists.newArrayListWithExpectedSize(concurrentThreads);
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final AtomicInteger currentlyRunning = new AtomicInteger(0);
    final AtomicInteger concurrentlyRan = new AtomicInteger(0);
    final AtomicInteger someoneRan = new AtomicInteger(0);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch readyLatch = new CountDownLatch(concurrentThreads * 2);
    final AtomicInteger queriesAccumualted = new AtomicInteger(0);
    for (int j = 0; j < concurrentThreads; j++) {
      indexFutures.add(
          indexExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  readyLatch.countDown();
                  try {
                    startLatch.await();
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
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

      final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                          .dataSource("xxx")
                                          .granularity(QueryGranularities.ALL)
                                          .intervals(ImmutableList.of(queryInterval))
                                          .aggregators(queryAggregatorFactories)
                                          .build();
      queryFutures.add(
          queryExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  readyLatch.countDown();
                  try {
                    startLatch.await();
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                  while (concurrentlyRan.get() == 0) {
                    QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
                        factory.createRunner(incrementalIndexSegment),
                        factory.getToolchest()
                    );
                    Map<String, Object> context = new HashMap<String, Object>();
                    Sequence<Result<TimeseriesResultValue>> sequence = runner.run(query, context);

                    for (Double result :
                        sequence.accumulate(
                            new Double[0], new Accumulator<Double[], Result<TimeseriesResultValue>>()
                            {
                              @Override
                              public Double[] accumulate(
                                  Double[] accumulated, Result<TimeseriesResultValue> in
                              )
                              {
                                if (currentlyRunning.get() > 0) {
                                  concurrentlyRan.incrementAndGet();
                                }
                                queriesAccumualted.incrementAndGet();
                                return Lists.asList(in.getValue().getDoubleMetric("doubleSumResult0"), accumulated)
                                            .toArray(new Double[accumulated.length + 1]);
                              }
                            }
                        )
                        ) {
                      final Integer maxValueExpected = someoneRan.get() + concurrentThreads;
                      if (maxValueExpected > 0) {
                        // Eventually consistent, but should be somewhere in that range
                        // Actual result is validated after all writes are guaranteed done.
                        Assert.assertTrue(
                            String.format("%d >= %g >= 0 violated", maxValueExpected, result),
                            result >= 0 && result <= maxValueExpected
                        );
                      }
                    }
                  }
                }
              }
          )
      );
    }
    readyLatch.await();
    startLatch.countDown();
    List<ListenableFuture<?>> allFutures = new ArrayList<>(queryFutures.size() + indexFutures.size());
    allFutures.addAll(queryFutures);
    allFutures.addAll(indexFutures);
    Futures.allAsList(allFutures).get();
    Assert.assertTrue("Queries ran too fast", queriesAccumualted.get() > 0);
    Assert.assertTrue("Did not hit concurrency, please try again", concurrentlyRan.get() > 0);
    queryExecutor.shutdown();
    indexExecutor.shutdown();
    QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
        factory.createRunner(incrementalIndexSegment),
        factory.getToolchest()
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(ImmutableList.of(queryInterval))
                                  .aggregators(queryAggregatorFactories)
                                  .build();
    Map<String, Object> context = new HashMap<String, Object>();
    List<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query, context),
        new LinkedList<Result<TimeseriesResultValue>>()
    );
    boolean isRollup = index.isRollup();
    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(
          elementsPerThread * (isRollup ? 1 : concurrentThreads),
          result.getValue().getLongMetric("rows").intValue()
      );
      for (int i = 0; i < dimensionCount; ++i) {
        Assert.assertEquals(
            String.format("Failed long sum on dimension %d", i),
            elementsPerThread * concurrentThreads,
            result.getValue().getLongMetric(String.format("sumResult%s", i)).intValue()
        );
        Assert.assertEquals(
            String.format("Failed double sum on dimension %d", i),
            elementsPerThread * concurrentThreads,
            result.getValue().getDoubleMetric(String.format("doubleSumResult%s", i)).intValue()
        );
      }
    }
  }

  @Test
  public void testConcurrentAdd() throws Exception
  {
    final IncrementalIndex index = closer.closeLater(indexCreator.createIndex(defaultAggregatorFactories));
    final int threadCount = 10;
    final int elementsPerThread = 200;
    final int dimensionCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final long timestamp = System.currentTimeMillis();
    final CountDownLatch latch = new CountDownLatch(threadCount);
    for (int j = 0; j < threadCount; j++) {
      executor.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                for (int i = 0; i < elementsPerThread; i++) {
                  index.add(getRow(timestamp + i, i, dimensionCount));
                }
              }
              catch (Exception e) {
                e.printStackTrace();
              }
              latch.countDown();
            }
          }
      );
    }
    Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));

    boolean isRollup = index.isRollup();
    Assert.assertEquals(dimensionCount, index.getDimensionNames().size());
    Assert.assertEquals(elementsPerThread * (isRollup ? 1 : threadCount), index.size());
    Iterator<Row> iterator = index.iterator();
    int curr = 0;
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Assert.assertEquals(timestamp + (isRollup ? curr : curr / threadCount), row.getTimestampFromEpoch());
      Assert.assertEquals(Float.valueOf(isRollup ? threadCount : 1), (Float) row.getFloatMetric("count"));
      curr++;
    }
    Assert.assertEquals(elementsPerThread * (isRollup ? 1 : threadCount), curr);
  }

  @Test
  public void testgetDimensions()
  {
    final IncrementalIndex<Aggregator> incrementalIndex = new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder().withQueryGranularity(QueryGranularities.NONE)
                                            .withMetrics(
                                                new AggregatorFactory[]{
                                                    new CountAggregatorFactory(
                                                        "count"
                                                    )
                                                }
                                            )
                                            .withDimensionsSpec(
                                                new DimensionsSpec(
                                                    DimensionsSpec.getDefaultSchemas(Arrays.asList("dim0", "dim1")),
                                                    null,
                                                    null
                                                )
                                            )
                                            .build(),
        true,
        1000000
    );
    closer.closeLater(incrementalIndex);

    Assert.assertEquals(Arrays.asList("dim0", "dim1"), incrementalIndex.getDimensionNames());
  }
}
