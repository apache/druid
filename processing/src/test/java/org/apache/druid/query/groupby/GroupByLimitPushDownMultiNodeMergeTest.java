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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private IncrementalIndex makeIncIndex(boolean withRollup)
  {
    return makeIncIndex(withRollup, Arrays.asList(
        new StringDimensionSchema("dimA"),
        new LongDimensionSchema("metA")
    ));
  }

  private IncrementalIndex makeIncIndex(boolean withRollup, List<DimensionSchema> dimensions)
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(
                    new DimensionsSpec(
                        Arrays.asList(
                            new StringDimensionSchema("dimA"),
                            new LongDimensionSchema("metA")
                        )
                    )
                )
                .withRollup(withRollup)
                .build()
        )
        .setMaxRowCount(1000)
        .build();
  }

  @Before
  public void setup() throws Exception
  {
    tmpDir = FileUtils.createTempDir();

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
        IndexSpec.DEFAULT,
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
        IndexSpec.DEFAULT,
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
        IndexSpec.DEFAULT,
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
        IndexSpec.DEFAULT,
        null
    );
    QueryableIndex qindexD = INDEX_IO.loadIndex(fileD);

    List<String> dimNames2 = Arrays.asList("dimA", "dimB", "metA");
    List<DimensionSchema> dimensions = Arrays.asList(
        new StringDimensionSchema("dimA"),
        new StringDimensionSchema("dimB"),
        new LongDimensionSchema("metA")
    );
    final IncrementalIndex indexE = makeIncIndex(false, dimensions);
    incrementalIndices.add(indexE);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("dimB", "raw");
    event.put("metA", 5L);
    row = new MapBasedInputRow(1505260800000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("dimB", "ripe");
    event.put("metA", 9L);
    row = new MapBasedInputRow(1605260800000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("dimB", "raw");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1705264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("dimB", "ripe");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "grape");
    event.put("dimB", "raw");
    event.put("metA", 5L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "apple");
    event.put("dimB", "ripe");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "apple");
    event.put("dimB", "raw");
    event.put("metA", 1L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "apple");
    event.put("dimB", "ripe");
    event.put("metA", 4L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "apple");
    event.put("dimB", "raw");
    event.put("metA", 1L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "banana");
    event.put("dimB", "ripe");
    event.put("metA", 4L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "orange");
    event.put("dimB", "raw");
    event.put("metA", 9L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "peach");
    event.put("dimB", "ripe");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "orange");
    event.put("dimB", "raw");
    event.put("metA", 2L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    event = new HashMap<>();
    event.put("dimA", "strawberry");
    event.put("dimB", "ripe");
    event.put("metA", 10L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexE.add(row);

    final File fileE = INDEX_MERGER_V9.persist(
        indexE,
        new File(tmpDir, "E"),
        IndexSpec.DEFAULT,
        null
    );
    QueryableIndex qindexE = INDEX_IO.loadIndex(fileE);

    final IncrementalIndex indexF = makeIncIndex(false, dimensions);
    incrementalIndices.add(indexF);

    event = new HashMap<>();
    event.put("dimA", "kiwi");
    event.put("dimB", "raw");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1505260800000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "watermelon");
    event.put("dimB", "ripe");
    event.put("metA", 14L);
    row = new MapBasedInputRow(1605260800000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "kiwi");
    event.put("dimB", "raw");
    event.put("metA", 8L);
    row = new MapBasedInputRow(1705264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "kiwi");
    event.put("dimB", "ripe");
    event.put("metA", 8L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "lemon");
    event.put("dimB", "raw");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "cherry");
    event.put("dimB", "ripe");
    event.put("metA", 2L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "cherry");
    event.put("dimB", "raw");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "avocado");
    event.put("dimB", "ripe");
    event.put("metA", 12L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "cherry");
    event.put("dimB", "raw");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "plum");
    event.put("dimB", "ripe");
    event.put("metA", 5L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "plum");
    event.put("dimB", "raw");
    event.put("metA", 3L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    event = new HashMap<>();
    event.put("dimA", "lime");
    event.put("dimB", "ripe");
    event.put("metA", 7L);
    row = new MapBasedInputRow(1805264400000L, dimNames2, event);
    indexF.add(row);

    final File fileF = INDEX_MERGER_V9.persist(
        indexF,
        new File(tmpDir, "F"),
        IndexSpec.DEFAULT,
        null
    );
    QueryableIndex qindexF = INDEX_IO.loadIndex(fileF);

    groupByIndices = Arrays.asList(qindexA, qindexB, qindexC, qindexD, qindexE, qindexF);
    resourceCloser = Closer.create();
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(3, "GroupByThreadPool[%d]");

    final TestBufferPool bufferPool = TestBufferPool.offHeap(10_000_000, Integer.MAX_VALUE);
    final TestBufferPool mergePool = TestBufferPool.offHeap(10_000_000, 2);
    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    final TestBufferPool mergePool2 = TestBufferPool.offHeap(10_000_000, 2);

    resourceCloser.register(() -> {
      // Verify that all objects have been returned to the pools.
      Assert.assertEquals(0, bufferPool.getOutstandingObjectCount());
      Assert.assertEquals(0, mergePool.getOutstandingObjectCount());
      Assert.assertEquals(0, mergePool2.getOutstandingObjectCount());
    });

    final GroupByQueryConfig config = new GroupByQueryConfig()
    {

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        return -1;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(1_000_000_000L);
      }
    };
    config.setSingleThreaded(false);

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
    final GroupingEngine groupingEngine = new GroupingEngine(
        druidProcessingConfig,
        configSupplier,
        bufferPool,
        mergePool,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        NOOP_QUERYWATCHER
    );

    final GroupingEngine groupingEngine2 = new GroupingEngine(
        druidProcessingConfig,
        configSupplier,
        bufferPool,
        mergePool2,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        NOOP_QUERYWATCHER
    );

    groupByFactory = new GroupByQueryRunnerFactory(
        groupingEngine,
        new GroupByQueryQueryToolChest(groupingEngine)
    );

    groupByFactory2 = new GroupByQueryRunnerFactory(
        groupingEngine2,
        new GroupByQueryQueryToolChest(groupingEngine2)
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
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionVirtualColumn(
                "d1:v",
                "timestamp_extract(\"__time\",'MONTH','UTC')",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionVirtualColumn(
                "d2:v",
                "timestamp_extract(\"__time\",'DAY','UTC')",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(
            new DefaultDimensionSpec("d0:v", "d0", ColumnType.LONG),
            new DefaultDimensionSpec("d1:v", "d1", ColumnType.LONG),
            new DefaultDimensionSpec("d2:v", "d2", ColumnType.LONG)
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
                ColumnType.LONG,
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

  @Test
  public void testForcePushLimitDownAccuracyWhenSortHasNonGroupingFields()
  {
    // The two testing segments have non overlapping groups, so the result should be 100% accurate even
    // forceLimitPushDown is applied
    List<ResultRow> resultsWithoutLimitPushDown = testForcePushLimitDownAccuracyWhenSortHasNonGroupingFieldsHelper(ImmutableMap.of());
    List<ResultRow> resultsWithLimitPushDown = testForcePushLimitDownAccuracyWhenSortHasNonGroupingFieldsHelper(ImmutableMap.of(
        GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, true,
        GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true
    ));

    List<ResultRow> expectedResults = ImmutableList.of(
        ResultRow.of("mango", "ripe", 16),
        ResultRow.of("kiwi", "raw", 15),
        ResultRow.of("watermelon", "ripe", 14),
        ResultRow.of("avocado", "ripe", 12),
        ResultRow.of("orange", "raw", 11)
    );

    Assert.assertEquals(expectedResults.toString(), resultsWithoutLimitPushDown.toString());
    Assert.assertEquals(expectedResults.toString(), resultsWithLimitPushDown.toString());
  }

  private List<ResultRow> testForcePushLimitDownAccuracyWhenSortHasNonGroupingFieldsHelper(Map<String, Object> context)
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1(4))
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory2.mergeRunners(executorService, getRunner2(5))
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

    DefaultLimitSpec ls = new DefaultLimitSpec(
        Collections.singletonList(
            new OrderByColumnSpec("a0", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC)
        ),
        5
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimA", "d0", ColumnType.STRING),
            new DefaultDimensionSpec("dimB", "d1", ColumnType.STRING)
        ).setAggregatorSpecs(new LongSumAggregatorFactory("a0", "metA"))
        .setLimitSpec(ls)
        .setContext(context)
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = finalRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    return queryResult.toList();
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

  public static final QueryWatcher NOOP_QUERYWATCHER = (query, future) -> {};
}
