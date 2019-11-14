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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class GroupByLimitPushDownMultiNodeMergeTest
{
  public static final ObjectMapper JSON_MAPPER;

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  private File tmpDir;
  private QueryRunnerFactory<ResultRow, GroupByQuery> groupByFactory;
  private QueryRunnerFactory<ResultRow, GroupByQuery> groupByFactory2;
  private List<IncrementalIndex> incrementalIndices = new ArrayList<>();
  private List<QueryableIndex> groupByIndices = new ArrayList<>();
  private ExecutorService executorService;
  private Closer resourceCloser;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std().addValue(
            ExprMacroTable.class,
            ExprMacroTable.nil()
        )
    );
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private IncrementalIndex makeIncIndex(boolean withRollup)
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(new DimensionsSpec(
                    Arrays.asList(
                        new StringDimensionSchema("dimA"),
                        new LongDimensionSchema("metA")
                    ),
                    null,
                    null
                ))
                .withRollup(withRollup)
                .build()
        )
        .setReportParseExceptions(false)
        .setConcurrentEventAdd(true)
        .setMaxRowCount(1000)
        .buildOnheap();
  }

  @Before
  public void setup() throws Exception
  {
    tmpDir = Files.createTempDir();

    InputRow row;
    List<String> dimNames = Arrays.asList("dimA", "metA");
    Map<String, Object> event;

    final IncrementalIndex indexA = makeIncIndex(false);
    incrementalIndices.add(indexA);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 2395L);
    row = new MapBasedInputRow(1505260888888L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 8L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 5028L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1505264400400L, dimNames, event);
    indexA.add(row);

    final File fileA = INDEX_MERGER_V9.persist(
        indexA,
        new File(tmpDir, "A"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexA = INDEX_IO.loadIndex(fileA);


    final IncrementalIndex indexB = makeIncIndex(false);
    incrementalIndices.add(indexB);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 4718L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 18L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 2698L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexB.add(row);

    final File fileB = INDEX_MERGER_V9.persist(
        indexB,
        new File(tmpDir, "B"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    final IncrementalIndex indexC = makeIncIndex(false);
    incrementalIndices.add(indexC);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 2395L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexC.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 8L);
    row = new MapBasedInputRow(1605260800000L, dimNames, event);
    indexC.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 5028L);
    row = new MapBasedInputRow(1705264400000L, dimNames, event);
    indexC.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1805264400000L, dimNames, event);
    indexC.add(row);

    final File fileC = INDEX_MERGER_V9.persist(
        indexC,
        new File(tmpDir, "C"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexC = INDEX_IO.loadIndex(fileC);


    final IncrementalIndex indexD = makeIncIndex(false);
    incrementalIndices.add(indexD);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 4718L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexD.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 18L);
    row = new MapBasedInputRow(1605260800000L, dimNames, event);
    indexD.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 2698L);
    row = new MapBasedInputRow(1705264400000L, dimNames, event);
    indexD.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1805264400000L, dimNames, event);
    indexD.add(row);

    final File fileD = INDEX_MERGER_V9.persist(
        indexD,
        new File(tmpDir, "D"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexD = INDEX_IO.loadIndex(fileD);

    groupByIndices = Arrays.asList(qindexA, qindexB, qindexC, qindexD);
    resourceCloser = Closer.create();
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(3, "GroupByThreadPool[%d]");

    final CloseableStupidPool<ByteBuffer> bufferPool = new CloseableStupidPool<>(
        "GroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", 10_000_000),
        0,
        Integer.MAX_VALUE
    );

    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    final CloseableDefaultBlockingPool<ByteBuffer> mergePool = new CloseableDefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 10_000_000),
        2
    );
    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    final CloseableDefaultBlockingPool<ByteBuffer> mergePool2 = new CloseableDefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 10_000_000),
        2
    );

    resourceCloser.register(bufferPool);
    resourceCloser.register(mergePool);
    resourceCloser.register(mergePool2);

    final GroupByQueryConfig config = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return "v2";
      }

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        return -1;
      }

      @Override
      public long getMaxOnDiskStorage()
      {
        return 1_000_000_000L;
      }
    };
    config.setSingleThreaded(false);
    config.setMaxIntermediateRows(Integer.MAX_VALUE);
    config.setMaxResults(Integer.MAX_VALUE);

    DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
    {
      @Override
      public int getNumThreads()
      {
        // Used by "v2" strategy for concurrencyHint
        return 2;
      }

      @Override
      public String getFormatString()
      {
        return null;
      }
    };

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final Supplier<QueryConfig> queryConfigSupplier = Suppliers.ofInstance(
        new QueryConfig()
    );
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            druidProcessingConfig,
            configSupplier,
            queryConfigSupplier,
            bufferPool,
            mergePool,
            new ObjectMapper(new SmileFactory()),
            NOOP_QUERYWATCHER
        )
    );

    final GroupByStrategySelector strategySelector2 = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            druidProcessingConfig,
            configSupplier,
            queryConfigSupplier,
            bufferPool,
            mergePool2,
            new ObjectMapper(new SmileFactory()),
            NOOP_QUERYWATCHER
        )
    );

    groupByFactory = new GroupByQueryRunnerFactory(
        strategySelector,
        new GroupByQueryQueryToolChest(
            strategySelector,
            noopIntervalChunkingQueryRunnerDecorator()
        )
    );

    groupByFactory2 = new GroupByQueryRunnerFactory(
        strategySelector2,
        new GroupByQueryQueryToolChest(
            strategySelector2,
            noopIntervalChunkingQueryRunnerDecorator()
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    for (IncrementalIndex incrementalIndex : incrementalIndices) {
      incrementalIndex.close();
    }

    for (QueryableIndex queryableIndex : groupByIndices) {
      queryableIndex.close();
    }

    resourceCloser.close();

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testDescendingNumerics()
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1(2))
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory2.mergeRunners(executorService, getRunner2(3))
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> finalRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            new QueryRunner<ResultRow>()
            {
              @Override
              public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
              {
                return Sequences
                    .simple(
                        ImmutableList.of(
                            theRunner.run(queryPlus, responseContext),
                            theRunner2.run(queryPlus, responseContext)
                        )
                    )
                    .flatMerge(Function.identity(), queryPlus.getQuery().getResultOrdering());
              }
            }
        ),
        (QueryToolChest) toolChest
    );

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1900000000000L))
    );

    DefaultLimitSpec ls2 = new DefaultLimitSpec(
        Arrays.asList(
            new OrderByColumnSpec("d0", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC),
            new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC),
            new OrderByColumnSpec("d2", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC)
        ),
        100
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "d0:v",
                "timestamp_extract(\"__time\",'YEAR','UTC')",
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionVirtualColumn(
                "d1:v",
                "timestamp_extract(\"__time\",'MONTH','UTC')",
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionVirtualColumn(
                "d2:v",
                "timestamp_extract(\"__time\",'DAY','UTC')",
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(
            new DefaultDimensionSpec("d0:v", "d0", ValueType.LONG),
            new DefaultDimensionSpec("d1:v", "d1", ValueType.LONG),
            new DefaultDimensionSpec("d2:v", "d2", ValueType.LONG)
        ).setAggregatorSpecs(new CountAggregatorFactory("a0"))
        .setLimitSpec(
            ls2
        )
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, true
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = finalRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "d0", 2027L,
        "d1", 3L,
        "d2", 17L,
        "a0", 2L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "d0", 2024L,
        "d1", 1L,
        "d2", 14L,
        "a0", 2L
    );
    ResultRow expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "d0", 2020L,
        "d1", 11L,
        "d2", 13L,
        "a0", 2L
    );
    ResultRow expectedRow3 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "d0", 2017L,
        "d1", 9L,
        "d2", 13L,
        "a0", 2L
    );
    System.out.println(results);
    Assert.assertEquals(4, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
    Assert.assertEquals(expectedRow2, results.get(2));
    Assert.assertEquals(expectedRow3, results.get(3));
  }

  @Test
  public void testPartialLimitPushDownMerge()
  {
    // one segment's results use limit push down, the other doesn't because of insufficient buffer capacity

    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1(0))
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory2.mergeRunners(executorService, getRunner2(1))
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> finalRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            new QueryRunner<ResultRow>()
            {
              @Override
              public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
              {
                return Sequences
                    .simple(
                        ImmutableList.of(
                            theRunner.run(queryPlus, responseContext),
                            theRunner2.run(queryPlus, responseContext)
                        )
                    )
                    .flatMerge(Function.identity(), queryPlus.getQuery().getResultOrdering());
              }
            }
        ),
        (QueryToolChest) toolChest
    );

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimA", "dimA"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "hour",
                ValueType.LONG,
                new TimeFormatExtractionFn(
                    null,
                    null,
                    null,
                    new PeriodGranularity(new Period("PT1H"), null, DateTimeZone.UTC),
                    true
                )
            )
        )
        .setAggregatorSpecs(new LongSumAggregatorFactory("metASum", "metA"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec("hour", OrderByColumnSpec.Direction.ASCENDING, StringComparators.NUMERIC),
                    new OrderByColumnSpec("dimA", OrderByColumnSpec.Direction.ASCENDING)
                ),
                1000
            )
        )
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, true
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = finalRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "dimA", "mango",
        "hour", 1505260800000L,
        "metASum", 26L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "dimA", "pomegranate",
        "hour", 1505260800000L,
        "metASum", 7113L
    );
    ResultRow expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "dimA", "mango",
        "hour", 1505264400000L,
        "metASum", 10L
    );
    ResultRow expectedRow3 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "2017-07-14T02:40:00.000Z",
        "dimA", "pomegranate",
        "hour", 1505264400000L,
        "metASum", 7726L
    );

    Assert.assertEquals(4, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
    Assert.assertEquals(expectedRow2, results.get(2));
    Assert.assertEquals(expectedRow3, results.get(3));
  }

  private List<QueryRunner<ResultRow>> getRunner1(int qIndexNumber)
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index = groupByIndices.get(qIndexNumber);
    QueryRunner<ResultRow> runner = makeQueryRunner(
        groupByFactory,
        SegmentId.dummy(index.toString()),
        new QueryableIndexSegment(index, SegmentId.dummy(index.toString()))
    );
    runners.add(groupByFactory.getToolchest().preMergeQueryDecoration(runner));
    return runners;
  }

  private List<QueryRunner<ResultRow>> getRunner2(int qIndexNumber)
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index2 = groupByIndices.get(qIndexNumber);
    QueryRunner<ResultRow> tooSmallRunner = makeQueryRunner(
        groupByFactory2,
        SegmentId.dummy(index2.toString()),
        new QueryableIndexSegment(index2, SegmentId.dummy(index2.toString()))
    );
    runners.add(groupByFactory2.getToolchest().preMergeQueryDecoration(tooSmallRunner));
    return runners;
  }

  private static class OffheapBufferGenerator implements Supplier<ByteBuffer>
  {
    private static final Logger log = new Logger(OffheapBufferGenerator.class);

    private final String description;
    private final int computationBufferSize;
    private final AtomicLong count = new AtomicLong(0);

    public OffheapBufferGenerator(String description, int computationBufferSize)
    {
      this.description = description;
      this.computationBufferSize = computationBufferSize;
    }

    @Override
    public ByteBuffer get()
    {
      log.info(
          "Allocating new %s buffer[%,d] of size[%,d]",
          description,
          count.getAndIncrement(),
          computationBufferSize
      );

      return ByteBuffer.allocateDirect(computationBufferSize);
    }
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<>(
        new BySegmentQueryRunner<>(segmentId, adapter.getDataInterval().getStart(), factory.createRunner(adapter)),
        (QueryToolChest<T, Query<T>>) factory.getToolchest()
    );
  }

  public static final QueryWatcher NOOP_QUERYWATCHER = new QueryWatcher()
  {
    @Override
    public void registerQuery(Query query, ListenableFuture future)
    {

    }
  };

  public static IntervalChunkingQueryRunnerDecorator noopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null)
    {
      @Override
      public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest)
      {
        return new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
          {
            return delegate.run(queryPlus, responseContext);
          }
        };
      }
    };
  }
}
