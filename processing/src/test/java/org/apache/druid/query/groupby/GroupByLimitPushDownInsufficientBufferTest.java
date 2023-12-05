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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
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
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
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
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
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

public class GroupByLimitPushDownInsufficientBufferTest extends InitializedNullHandlingTest
{
  public static final ObjectMapper JSON_MAPPER;

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  private File tmpDir;
  private QueryRunnerFactory<ResultRow, GroupByQuery> groupByFactory;
  private QueryRunnerFactory<ResultRow, GroupByQuery> tooSmallGroupByFactory;
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
    event.put("dimA", "hello");
    event.put("metA", 100);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 95);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "fubaz");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "zortaxx");
    event.put("metA", 999);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "blarg");
    event.put("metA", 125);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "blerg");
    event.put("metA", 130);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);


    final File fileA = INDEX_MERGER_V9.persist(
        indexA,
        new File(tmpDir, "A"),
        IndexSpec.DEFAULT,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    QueryableIndex qindexA = INDEX_IO.loadIndex(fileA);


    final IncrementalIndex indexB = makeIncIndex(false);
    incrementalIndices.add(indexB);

    event = new HashMap<>();
    event.put("dimA", "foo");
    event.put("metA", 200);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 95);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "zebra");
    event.put("metA", 180);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "blorg");
    event.put("metA", 120);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    final File fileB = INDEX_MERGER_V9.persist(
        indexB,
        new File(tmpDir, "B"),
        IndexSpec.DEFAULT,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    groupByIndices = Arrays.asList(qindexA, qindexB);
    resourceCloser = Closer.create();
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(3, "GroupByThreadPool[%d]");

    final TestBufferPool bufferPool = TestBufferPool.offHeap(10_000_000, Integer.MAX_VALUE);

    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    final TestBufferPool mergePool = TestBufferPool.offHeap(10_000_000, 2);
    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    final TestBufferPool tooSmallMergePool = TestBufferPool.onHeap(255, 2);

    resourceCloser.register(() -> {
      // Verify that all objects have been returned to the pools.
      Assert.assertEquals(0, mergePool.getOutstandingObjectCount());
      Assert.assertEquals(0, tooSmallMergePool.getOutstandingObjectCount());
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

    DruidProcessingConfig tooSmallDruidProcessingConfig = new DruidProcessingConfig()
    {
      @Override
      public int intermediateComputeSizeBytes()
      {
        return 255;
      }

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

    final GroupingEngine tooSmallEngine = new GroupingEngine(
        tooSmallDruidProcessingConfig,
        configSupplier,
        bufferPool,
        tooSmallMergePool,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        NOOP_QUERYWATCHER
    );


    groupByFactory = new GroupByQueryRunnerFactory(
        groupingEngine,
        new GroupByQueryQueryToolChest(groupingEngine)
    );

    tooSmallGroupByFactory = new GroupByQueryRunnerFactory(
        tooSmallEngine,
        new GroupByQueryQueryToolChest(tooSmallEngine)
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
  public void testPartialLimitPushDownMerge()
  {
    // one segment's results use limit push down, the other doesn't because of insufficient buffer capacity

    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            tooSmallGroupByFactory.mergeRunners(executorService, getRunner2())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner3 = new FinalizeResultsQueryRunner<>(
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
        Collections.singletonList(Intervals.utc(0, 1000000))
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimA", null))
        .setAggregatorSpecs(new LongSumAggregatorFactory("metA", "metA"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("dimA", OrderByColumnSpec.Direction.DESCENDING)),
                3
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = theRunner3.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "zortaxx",
        "metA", 999L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "zebra",
        "metA", 180L
    );
    ResultRow expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "world",
        "metA", 150L
    );

    Assert.assertEquals(3, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
    Assert.assertEquals(expectedRow2, results.get(2));
  }

  @Test
  public void testPartialLimitPushDownMergeForceAggs()
  {
    // one segment's results use limit push down, the other doesn't because of insufficient buffer capacity

    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1())
        ),
        (QueryToolChest) toolChest
    );


    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            tooSmallGroupByFactory.mergeRunners(executorService, getRunner2())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner3 = new FinalizeResultsQueryRunner<>(
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
        Collections.singletonList(Intervals.utc(0, 1000000))
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimA", null))
        .setAggregatorSpecs(new LongSumAggregatorFactory("metA", "metA"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec("metA", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC)
                ),
                3
            )
        )
        .setGranularity(Granularities.ALL)
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN,
                true
            )
        )
        .build();

    Sequence<ResultRow> queryResult = theRunner3.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "zortaxx",
        "metA", 999L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "foo",
        "metA", 200L
    );
    ResultRow expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
        query,
        "1970-01-01T00:00:00.000Z",
        "dimA", "mango",
        "metA", 190L
    );

    Assert.assertEquals(3, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
    Assert.assertEquals(expectedRow2, results.get(2));
  }

  private List<QueryRunner<ResultRow>> getRunner1()
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index = groupByIndices.get(0);
    QueryRunner<ResultRow> runner = makeQueryRunner(
        groupByFactory,
        SegmentId.dummy(index.toString()),
        new QueryableIndexSegment(index, SegmentId.dummy(index.toString()))
    );
    runners.add(groupByFactory.getToolchest().preMergeQueryDecoration(runner));
    return runners;
  }

  private List<QueryRunner<ResultRow>> getRunner2()
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index2 = groupByIndices.get(1);
    QueryRunner<ResultRow> tooSmallRunner = makeQueryRunner(
        tooSmallGroupByFactory,
        SegmentId.dummy(index2.toString()),
        new QueryableIndexSegment(index2, SegmentId.dummy(index2.toString()))
    );
    runners.add(tooSmallGroupByFactory.getToolchest().preMergeQueryDecoration(tooSmallRunner));
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

  public static final QueryWatcher NOOP_QUERYWATCHER = new QueryWatcher()
  {
    @Override
    public void registerQueryFuture(Query query, ListenableFuture future)
    {

    }
  };
}
