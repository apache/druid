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

package org.apache.druid.segment.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndex.Builder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.AfterClass;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest extends InitializedNullHandlingTest
{
  interface IndexCreator
  {
    IncrementalIndex createIndex(AggregatorFactory[] aggregatorFactories);
  }

  private static final Closer RESOURCE_CLOSER = Closer.create();

  @AfterClass
  public static void teardown() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  private final IndexCreator indexCreator;

  @Rule
  public final CloserRule closerRule = new CloserRule(false);

  public IncrementalIndexTest(IndexCreator indexCreator)
  {
    this.indexCreator = indexCreator;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> params = new ArrayList<>();
    params.add(new Object[] {(IndexCreator) IncrementalIndexTest::createIndex});
    final CloseableStupidPool<ByteBuffer> pool1 = new CloseableStupidPool<>(
        "OffheapIncrementalIndex-bufferPool",
        () -> ByteBuffer.allocate(256 * 1024)
    );
    RESOURCE_CLOSER.register(pool1);
    params.add(
        new Object[] {
            (IndexCreator) factories -> new Builder()
                .setSimpleTestingIndexSchema(factories)
                .setMaxRowCount(1000000)
                .buildOffheap(pool1)
        }
    );
    params.add(new Object[] {(IndexCreator) IncrementalIndexTest::createNoRollupIndex});
    final CloseableStupidPool<ByteBuffer> pool2 = new CloseableStupidPool<>(
        "OffheapIncrementalIndex-bufferPool",
        () -> ByteBuffer.allocate(256 * 1024)
    );
    RESOURCE_CLOSER.register(pool2);
    params.add(
        new Object[] {
            (IndexCreator) factories -> new Builder()
                .setIndexSchema(
                    new IncrementalIndexSchema.Builder()
                        .withMetrics(factories)
                        .withRollup(false)
                        .build()
                )
                .setMaxRowCount(1000000)
                .buildOffheap(pool2)
        }
    );

    return params;
  }

  public static AggregatorFactory[] getDefaultCombiningAggregatorFactories()
  {
    return DEFAULT_COMBINING_AGGREGATOR_FACTORIES;
  }

  public static IncrementalIndex createIndex(
      AggregatorFactory[] aggregatorFactories,
      DimensionsSpec dimensionsSpec
  )
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = DEFAULT_AGGREGATOR_FACTORIES;
    }

    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(dimensionsSpec)
                .withMetrics(aggregatorFactories)
                .build()
        )
        .setMaxRowCount(1000000)
        .buildOnheap();
  }

  public static IncrementalIndex createIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = DEFAULT_AGGREGATOR_FACTORIES;
    }

    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(aggregatorFactories)
        .setMaxRowCount(1000000)
        .buildOnheap();
  }

  public static IncrementalIndex createNoRollupIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = DEFAULT_AGGREGATOR_FACTORIES;
    }

    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(false, aggregatorFactories)
        .setMaxRowCount(1000000)
        .buildOnheap();
  }

  public static void populateIndex(long timestamp, IncrementalIndex index) throws IndexSizeExceededException
  {
    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2")
        )
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "3", "dim2", "4")
        )
    );
  }

  public static MapBasedInputRow getRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, dimName + rowID);
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static MapBasedInputRow getLongRow(long timestamp, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = StringUtils.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, (Long) 1L);
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  private static final AggregatorFactory[] DEFAULT_AGGREGATOR_FACTORIES = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  private static final AggregatorFactory[] DEFAULT_COMBINING_AGGREGATOR_FACTORIES = new AggregatorFactory[]{
      DEFAULT_AGGREGATOR_FACTORIES[0].getCombiningFactory()
  };

  @Test
  public void testCaseSensitivity() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex(DEFAULT_AGGREGATOR_FACTORIES));

    populateIndex(timestamp, index);
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), index.getDimensionNames());
    Assert.assertEquals(2, index.size());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Collections.singletonList("1"), row.getDimension("dim1"));
    Assert.assertEquals(Collections.singletonList("2"), row.getDimension("dim2"));

    row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Collections.singletonList("3"), row.getDimension("dim1"));
    Assert.assertEquals(Collections.singletonList("4"), row.getDimension("dim2"));
  }

  @Test
  public void testFilteredAggregators() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = closerRule.closeLater(
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
            ImmutableMap.of("dim1", "1", "dim2", "2", "dim3", Lists.newArrayList("b", "a"), "met1", 10)
        )
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2", "dim3"),
            ImmutableMap.of("dim1", "3", "dim2", "4", "dim3", Lists.newArrayList("c", "d"), "met1", 11)
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
    Assert.assertEquals(Collections.singletonList("1"), row.getDimension("dim1"));
    Assert.assertEquals(Collections.singletonList("2"), row.getDimension("dim2"));
    Assert.assertEquals(Arrays.asList("a", "b"), row.getDimension("dim3"));
    Assert.assertEquals(1L, row.getMetric("count"));
    Assert.assertEquals(1L, row.getMetric("count_selector_filtered"));
    Assert.assertEquals(1L, row.getMetric("count_bound_filtered"));
    Assert.assertEquals(1L, row.getMetric("count_multivaldim_filtered"));
    Assert.assertEquals(0L, row.getMetric("count_numeric_filtered"));

    row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Collections.singletonList("3"), row.getDimension("dim1"));
    Assert.assertEquals(Collections.singletonList("4"), row.getDimension("dim2"));
    Assert.assertEquals(Arrays.asList("c", "d"), row.getDimension("dim3"));
    Assert.assertEquals(1L, row.getMetric("count"));
    Assert.assertEquals(0L, row.getMetric("count_selector_filtered"));
    Assert.assertEquals(0L, row.getMetric("count_bound_filtered"));
    Assert.assertEquals(0L, row.getMetric("count_multivaldim_filtered"));
    Assert.assertEquals(1L, row.getMetric("count_numeric_filtered"));
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

    final IncrementalIndex index = closerRule.closeLater(
        indexCreator.createIndex(
            ingestAggregatorFactories.toArray(
                new AggregatorFactory[0]
            )
        )
    );

    final long timestamp = System.currentTimeMillis();

    final int rows = 50;

    //ingesting same data twice to have some merging happening
    for (int i = 0; i < rows; i++) {
      index.add(getLongRow(timestamp + i, dimensionCount));
    }

    for (int i = 0; i < rows; i++) {
      index.add(getLongRow(timestamp + i, dimensionCount));
    }

    //run a timeseries query on the index and verify results
    final ArrayList<AggregatorFactory> queryAggregatorFactories = new ArrayList<>();
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

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("xxx")
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(Intervals.of("2000/2030")))
                                  .aggregators(queryAggregatorFactories)
                                  .build();

    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
        factory.createRunner(incrementalIndexSegment),
        factory.getToolchest()
    );


    List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    Result<TimeseriesResultValue> result = Iterables.getOnlyElement(results);
    boolean isRollup = index.isRollup();
    Assert.assertEquals(rows * (isRollup ? 1 : 2), result.getValue().getLongMetric("rows").intValue());
    for (int i = 0; i < dimensionCount; ++i) {
      Assert.assertEquals(
          "Failed long sum on dimension " + i,
          2 * rows,
          result.getValue().getLongMetric("sumResult" + i).intValue()
      );
      Assert.assertEquals(
          "Failed double sum on dimension " + i,
          2 * rows,
          result.getValue().getDoubleMetric("doubleSumResult" + i).intValue()
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


    final IncrementalIndex index = closerRule.closeLater(
        indexCreator.createIndex(ingestAggregatorFactories.toArray(new AggregatorFactory[0]))
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
    final Interval queryInterval = Intervals.of("1900-01-01T00:00:00Z/2900-01-01T00:00:00Z");
    final List<ListenableFuture<?>> indexFutures = Lists.newArrayListWithExpectedSize(concurrentThreads);
    final List<ListenableFuture<?>> queryFutures = Lists.newArrayListWithExpectedSize(concurrentThreads);
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);
    final QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
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
                    throw new RuntimeException(e);
                  }
                  currentlyRunning.incrementAndGet();
                  try {
                    for (int i = 0; i < elementsPerThread; i++) {
                      index.add(getLongRow(timestamp + i, dimensionCount));
                      someoneRan.incrementAndGet();
                    }
                  }
                  catch (IndexSizeExceededException e) {
                    throw new RuntimeException(e);
                  }
                  currentlyRunning.decrementAndGet();
                }
              }
          )
      );

      final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                          .dataSource("xxx")
                                          .granularity(Granularities.ALL)
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
                    throw new RuntimeException(e);
                  }
                  while (concurrentlyRan.get() == 0) {
                    QueryRunner<Result<TimeseriesResultValue>> runner = new FinalizeResultsQueryRunner<Result<TimeseriesResultValue>>(
                        factory.createRunner(incrementalIndexSegment),
                        factory.getToolchest()
                    );
                    Sequence<Result<TimeseriesResultValue>> sequence = runner.run(QueryPlus.wrap(query));

                    Double[] results = sequence.accumulate(
                        new Double[0],
                        new Accumulator<Double[], Result<TimeseriesResultValue>>()
                        {
                          @Override
                          public Double[] accumulate(Double[] accumulated, Result<TimeseriesResultValue> in)
                          {
                            if (currentlyRunning.get() > 0) {
                              concurrentlyRan.incrementAndGet();
                            }
                            queriesAccumualted.incrementAndGet();
                            return Lists.asList(in.getValue().getDoubleMetric("doubleSumResult0"), accumulated)
                                        .toArray(new Double[0]);
                          }
                        }
                    );
                    for (Double result : results) {
                      final Integer maxValueExpected = someoneRan.get() + concurrentThreads;
                      if (maxValueExpected > 0) {
                        // Eventually consistent, but should be somewhere in that range
                        // Actual result is validated after all writes are guaranteed done.
                        Assert.assertTrue(
                            StringUtils.format("%d >= %g >= 0 violated", maxValueExpected, result),
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
                                  .granularity(Granularities.ALL)
                                  .intervals(ImmutableList.of(queryInterval))
                                  .aggregators(queryAggregatorFactories)
                                  .build();
    List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    boolean isRollup = index.isRollup();
    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(
          elementsPerThread * (isRollup ? 1 : concurrentThreads),
          result.getValue().getLongMetric("rows").intValue()
      );
      for (int i = 0; i < dimensionCount; ++i) {
        Assert.assertEquals(
            StringUtils.format("Failed long sum on dimension %d", i),
            elementsPerThread * concurrentThreads,
            result.getValue().getLongMetric(StringUtils.format("sumResult%s", i)).intValue()
        );
        Assert.assertEquals(
            StringUtils.format("Failed double sum on dimension %d", i),
            elementsPerThread * concurrentThreads,
            result.getValue().getDoubleMetric(StringUtils.format("doubleSumResult%s", i)).intValue()
        );
      }
    }
  }

  @Test
  public void testConcurrentAdd() throws Exception
  {
    final IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex(DEFAULT_AGGREGATOR_FACTORIES));
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
      Assert.assertEquals(isRollup ? threadCount : 1, row.getMetric("count").intValue());
      curr++;
    }
    Assert.assertEquals(elementsPerThread * (isRollup ? 1 : threadCount), curr);
  }

  @Test
  public void testgetDimensions()
  {
    final IncrementalIndex<Aggregator> incrementalIndex = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(new CountAggregatorFactory("count"))
                .withDimensionsSpec(
                    new DimensionsSpec(
                        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim0", "dim1")),
                        null,
                        null
                    )
                )
                .build()
        )
        .setMaxRowCount(1000000)
        .buildOnheap();
    closerRule.closeLater(incrementalIndex);

    Assert.assertEquals(Arrays.asList("dim0", "dim1"), incrementalIndex.getDimensionNames());
  }

  @Test
  public void testDynamicSchemaRollup() throws IndexSizeExceededException
  {
    IncrementalIndex<Aggregator> index = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(/* empty */)
        .setMaxRowCount(10)
        .buildOnheap();
    closerRule.closeLater(index);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host")
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871670000L,
            Arrays.asList("name", "table"),
            ImmutableMap.of("name", "name2", "table", "table")
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host")
        )
    );

    Assert.assertEquals(2, index.size());
  }
}
