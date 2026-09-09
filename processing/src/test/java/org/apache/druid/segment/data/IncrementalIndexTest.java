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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.ParseException;
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
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.CloserExtension;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
@ParameterizedClass
@MethodSource("constructorFeeder")
public class IncrementalIndexTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;
  private final boolean isPreserveExistingMetrics;

  @RegisterExtension
  public final CloserExtension closer = new CloserExtension(false);

  public IncrementalIndexTest(String indexType, String mode, boolean isPreserveExistingMetrics) throws JsonProcessingException
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    this.isPreserveExistingMetrics = isPreserveExistingMetrics;
    indexCreator = closer.closeLater(new IncrementalIndexCreator(indexType, (builder, args) -> builder
        .setSimpleTestingIndexSchema("rollup".equals(mode), isPreserveExistingMetrics, (AggregatorFactory[]) args[0])
        .setMaxRowCount(1_000_000)
        .build()
    ));
  }

  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.indexTypeCartesianProduct(
        ImmutableList.of("rollup", "plain"),
        ImmutableList.of(true, false)
    );
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

    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(dimensionsSpec)
                .withMetrics(aggregatorFactories)
                .build()
        )
        .setMaxRowCount(1000000)
        .build();
  }

  public static IncrementalIndex createIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = DEFAULT_AGGREGATOR_FACTORIES;
    }

    return new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(aggregatorFactories)
        .setMaxRowCount(1000000)
        .build();
  }

  public static IncrementalIndex createNoRollupIndex(AggregatorFactory[] aggregatorFactories)
  {
    if (null == aggregatorFactories) {
      aggregatorFactories = DEFAULT_AGGREGATOR_FACTORIES;
    }

    return new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(false, false, aggregatorFactories)
        .setMaxRowCount(1000000)
        .build();
  }

  public static void populateIndex(long timestamp, IncrementalIndex index)
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
    List<String> dimensionList = new ArrayList<>(dimensionCount);
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
    List<String> dimensionList = new ArrayList<>(dimensionCount);
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
    IncrementalIndex index = indexCreator.createIndex((Object) DEFAULT_AGGREGATOR_FACTORIES);

    populateIndex(timestamp, index);
    Assertions.assertEquals(Arrays.asList("__time", "dim1", "dim2"), index.getDimensionNames(true));
    Assertions.assertEquals(Arrays.asList("dim1", "dim2"), index.getDimensionNames(false));
    Assertions.assertEquals(2, index.numRows());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assertions.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assertions.assertEquals(Collections.singletonList("1"), row.getDimension("dim1"));
    Assertions.assertEquals(Collections.singletonList("2"), row.getDimension("dim2"));

    row = rows.next();
    Assertions.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assertions.assertEquals(Collections.singletonList("3"), row.getDimension("dim1"));
    Assertions.assertEquals(Collections.singletonList("4"), row.getDimension("dim2"));
  }

  @Test
  public void testFilteredAggregators() throws Exception
  {
    long timestamp = System.currentTimeMillis();
    IncrementalIndex index = indexCreator.createIndex((Object) new AggregatorFactory[]{
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
    });

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

    Assertions.assertEquals(Arrays.asList("__time", "dim1", "dim2", "dim3"), index.getDimensionNames(true));
    Assertions.assertEquals(Arrays.asList("dim1", "dim2", "dim3"), index.getDimensionNames(false));
    Assertions.assertEquals(
        Arrays.asList(
            "count",
            "count_selector_filtered",
            "count_bound_filtered",
            "count_multivaldim_filtered",
            "count_numeric_filtered"
        ),
        index.getMetricNames()
    );
    Assertions.assertEquals(2, index.numRows());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assertions.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assertions.assertEquals(Collections.singletonList("1"), row.getDimension("dim1"));
    Assertions.assertEquals(Collections.singletonList("2"), row.getDimension("dim2"));
    Assertions.assertEquals(Arrays.asList("a", "b"), row.getDimension("dim3"));
    Assertions.assertEquals(1L, row.getMetric("count"));
    Assertions.assertEquals(1L, row.getMetric("count_selector_filtered"));
    Assertions.assertEquals(1L, row.getMetric("count_bound_filtered"));
    Assertions.assertEquals(1L, row.getMetric("count_multivaldim_filtered"));
    Assertions.assertEquals(0L, row.getMetric("count_numeric_filtered"));

    row = rows.next();
    Assertions.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assertions.assertEquals(Collections.singletonList("3"), row.getDimension("dim1"));
    Assertions.assertEquals(Collections.singletonList("4"), row.getDimension("dim2"));
    Assertions.assertEquals(Arrays.asList("c", "d"), row.getDimension("dim3"));
    Assertions.assertEquals(1L, row.getMetric("count"));
    Assertions.assertEquals(0L, row.getMetric("count_selector_filtered"));
    Assertions.assertEquals(0L, row.getMetric("count_bound_filtered"));
    Assertions.assertEquals(0L, row.getMetric("count_multivaldim_filtered"));
    Assertions.assertEquals(1L, row.getMetric("count_numeric_filtered"));
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

    final IncrementalIndex index = indexCreator.createIndex(
        (Object) ingestAggregatorFactories.toArray(
            new AggregatorFactory[0]
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
    Assertions.assertEquals(rows * (isRollup ? 1 : 2), result.getValue().getLongMetric("rows").intValue());
    for (int i = 0; i < dimensionCount; ++i) {
      Assertions.assertEquals(
          2 * rows,
          result.getValue().getLongMetric("sumResult" + i).intValue(),
          "Failed long sum on dimension " + i
      );
      Assertions.assertEquals(
          2 * rows,
          result.getValue().getDoubleMetric("doubleSumResult" + i).intValue(),
          "Failed double sum on dimension " + i
      );
    }
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 60_000L, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
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


    final IncrementalIndex index = indexCreator.createIndex(
        (Object) ingestAggregatorFactories.toArray(new AggregatorFactory[0])
    );
    final int addThreads = 1;
    final int elementsPerThread = 10_000;
    final ListeningExecutorService indexExecutor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            addThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("index-executor-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            addThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("query-executor-%d")
                .build()
        )
    );
    final long timestamp = System.currentTimeMillis();
    final Interval queryInterval = Intervals.of("1900-01-01T00:00:00Z/2900-01-01T00:00:00Z");
    final List<ListenableFuture<?>> indexFutures = Lists.newArrayListWithExpectedSize(addThreads);
    final List<ListenableFuture<?>> queryFutures = Lists.newArrayListWithExpectedSize(addThreads);
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
    final CountDownLatch readyLatch = new CountDownLatch(addThreads * 2);
    final AtomicInteger queriesAccumualted = new AtomicInteger(0);
    for (int j = 0; j < addThreads; j++) {
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
                  for (int i = 0; i < elementsPerThread; i++) {
                    index.add(getLongRow(timestamp + i, dimensionCount));
                    someoneRan.incrementAndGet();
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
                        new Accumulator<>()
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
                      final int maxValueExpected = someoneRan.get() + addThreads;
                      if (maxValueExpected > 0) {
                        // Eventually consistent, but should be somewhere in that range
                        // Actual result is validated after all writes are guaranteed done.
                        Assertions.assertTrue(
                            result >= 0 && result <= maxValueExpected,
                            StringUtils.format("%d >= %g >= 0 violated", maxValueExpected, result)
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
    Assertions.assertTrue(queriesAccumualted.get() > 0, "Queries ran too fast");
    Assertions.assertTrue(concurrentlyRan.get() > 0, "Did not hit concurrency, please try again");
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
      Assertions.assertEquals(
          elementsPerThread * (isRollup ? 1 : addThreads),
          result.getValue().getLongMetric("rows").intValue()
      );
      for (int i = 0; i < dimensionCount; ++i) {
        Assertions.assertEquals(
            elementsPerThread * addThreads,
            result.getValue().getLongMetric(StringUtils.format("sumResult%s", i)).intValue(),
            StringUtils.format("Failed long sum on dimension %d", i)
        );
        Assertions.assertEquals(
            elementsPerThread * addThreads,
            result.getValue().getDoubleMetric(StringUtils.format("doubleSumResult%s", i)).intValue(),
            StringUtils.format("Failed double sum on dimension %d", i)
        );
      }
    }
  }

  @Test
  public void testgetDimensions()
  {
    final IncrementalIndex incrementalIndex = indexCreator.createIndex(
        (builder, args) -> builder
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withMetrics(new CountAggregatorFactory("count"))
                    .withDimensionsSpec(
                        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim0", "dim1")))
                    )
                    .build()
            )
            .setMaxRowCount(1000000)
            .build()
    );

    Assertions.assertEquals(Arrays.asList("__time", "dim0", "dim1"), incrementalIndex.getDimensionNames(true));
    Assertions.assertEquals(Arrays.asList("dim0", "dim1"), incrementalIndex.getDimensionNames(false));
  }

  @Test
  public void testDynamicSchemaRollup()
  {
    final IncrementalIndex index = indexCreator.createIndex(
        (builder, args) -> builder
            .setSimpleTestingIndexSchema(/* empty */)
            .setMaxRowCount(10)
            .build()
    );

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

    Assertions.assertEquals(2, index.numRows());
  }

  @Test
  public void testSchemaRollupWithRowWithExistingMetricsAndWithoutMetric()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 2)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 3)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 2, "sum_of_x", 4)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 3, "sum_of_x", 5)
        )
    );

    Assertions.assertEquals(index.isRollup() ? 1 : 4, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(isPreserveExistingMetrics ? 7 : 4, row.getMetric("count").intValue());
        Assertions.assertEquals(isPreserveExistingMetrics ? 14 : 5, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 4 rows
        if (rowCount == 1 || rowCount == 2) {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(1 + rowCount, row.getMetric("sum_of_x").intValue());
        } else {
          if (isPreserveExistingMetrics) {
            Assertions.assertEquals(rowCount - 1, row.getMetric("count").intValue());
            Assertions.assertEquals(1 + rowCount, row.getMetric("sum_of_x").intValue());
          } else {
            Assertions.assertEquals(1, row.getMetric("count").intValue());
            Assertions.assertNull(row.getMetric("sum_of_x"));
          }
        }
      }
    }
  }

  @Test
  public void testSchemaRollupWithRowWithExistingMetricsAndWithoutMetricUsingAggregatorWithDifferentReturnType()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        // FloatSumAggregator combine method takes in two Float but return Double
        new FloatSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 2)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 3)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 2, "sum_of_x", 4)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 3, "sum_of_x", 5)
        )
    );

    Assertions.assertEquals(index.isRollup() ? 1 : 4, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(isPreserveExistingMetrics ? 7 : 4, row.getMetric("count").intValue());
        Assertions.assertEquals(isPreserveExistingMetrics ? 14 : 5, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 4 rows
        if (rowCount == 1 || rowCount == 2) {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(1 + rowCount, row.getMetric("sum_of_x").intValue());
        } else {
          if (isPreserveExistingMetrics) {
            Assertions.assertEquals(rowCount - 1, row.getMetric("count").intValue());
            Assertions.assertEquals(1 + rowCount, row.getMetric("sum_of_x").intValue());
          } else {
            Assertions.assertEquals(1, row.getMetric("count").intValue());
            Assertions.assertNull(row.getMetric("sum_of_x"));
          }
        }
      }
    }
  }

  @Test
  public void testSchemaRollupWithRowWithOnlyExistingMetrics()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 2, "sum_of_x", 4)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "count", 3, "x", 3, "sum_of_x", 5)
        )
    );

    Assertions.assertEquals(index.isRollup() ? 1 : 2, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(isPreserveExistingMetrics ? 5 : 2, row.getMetric("count").intValue());
        Assertions.assertEquals(isPreserveExistingMetrics ? 9 : 3, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 2 rows
        if (rowCount == 1) {
          if (isPreserveExistingMetrics) {
            Assertions.assertEquals(2, row.getMetric("count").intValue());
            Assertions.assertEquals(4, row.getMetric("sum_of_x").intValue());
          } else {
            Assertions.assertEquals(1, row.getMetric("count").intValue());
            Assertions.assertNull(row.getMetric("sum_of_x"));
          }
        } else {
          Assertions.assertEquals(isPreserveExistingMetrics ? 3 : 1, row.getMetric("count").intValue());
          Assertions.assertEquals(isPreserveExistingMetrics ? 5 : 3, row.getMetric("sum_of_x").intValue());
        }
      }
    }
  }

  @Test
  public void testSchemaRollupWithRowsWithNoMetrics()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 4)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host", "x", 3)
        )
    );

    Assertions.assertEquals(index.isRollup() ? 1 : 2, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(2, row.getMetric("count").intValue());
        Assertions.assertEquals(7, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 2 rows
        if (rowCount == 1) {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(4, row.getMetric("sum_of_x").intValue());
        } else {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(3, row.getMetric("sum_of_x").intValue());
        }
      }
    }
  }

  @Test
  public void testSchemaRollupWithRowWithMixedTypeMetrics()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    final MapBasedInputRow firstRow = new MapBasedInputRow(
        1481871600000L,
        Arrays.asList("name", "host"),
        ImmutableMap.of("name", "name1", "host", "host", "count", "not a number 1", "sum_of_x", 4)
    );
    final MapBasedInputRow secondRow = new MapBasedInputRow(
        1481871600000L,
        Arrays.asList("name", "host"),
        ImmutableMap.of("name", "name1", "host", "host", "count", 3, "x", 3, "sum_of_x", "not a number 2")
    );

    if (isPreserveExistingMetrics) {
      Assertions.assertThrows(ParseException.class, () -> {
        index.add(firstRow);
        index.add(secondRow);
      });
      return;
    }
    index.add(firstRow);
    index.add(secondRow);

    Assertions.assertEquals(index.isRollup() ? 1 : 2, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(2, row.getMetric("count").intValue());
        Assertions.assertEquals(3, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 2 rows
        if (rowCount == 1) {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertNull(row.getMetric("sum_of_x"));
        } else {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(3, row.getMetric("sum_of_x").intValue());
        }
      }
    }
  }

  @Test
  public void testSchemaRollupWithRowsWithNonRolledUpSameColumnName()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum_of_x", "x")
    };
    final IncrementalIndex index = indexCreator.createIndex((Object) aggregatorFactories);
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "sum_of_x", 100, "x", 4)
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "sum_of_x", 100, "x", 3)
        )
    );

    Assertions.assertEquals(index.isRollup() ? 1 : 2, index.numRows());
    Iterator<Row> iterator = index.iterator();
    int rowCount = 0;
    while (iterator.hasNext()) {
      rowCount++;
      Row row = iterator.next();
      Assertions.assertEquals(1481871600000L, row.getTimestampFromEpoch());
      if (index.isRollup()) {
        // All rows are rollup into one row
        Assertions.assertEquals(2, row.getMetric("count").intValue());
        Assertions.assertEquals(isPreserveExistingMetrics ? 200 : 7, row.getMetric("sum_of_x").intValue());
      } else {
        // We still have 2 rows
        if (rowCount == 1) {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(isPreserveExistingMetrics ? 100 : 4, row.getMetric("sum_of_x").intValue());
        } else {
          Assertions.assertEquals(1, row.getMetric("count").intValue());
          Assertions.assertEquals(isPreserveExistingMetrics ? 100 : 3, row.getMetric("sum_of_x").intValue());
        }
      }
    }
  }
}
