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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.collections.BlockingPool;
import io.druid.collections.DefaultBlockingPool;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.ExprMacroTable;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.query.groupby.strategy.GroupByStrategyV1;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class GroupByLimitPushDownInsufficientBufferTest
{
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;
  private File tmpDir;
  private QueryRunnerFactory<Row, GroupByQuery> groupByFactory;
  private QueryRunnerFactory<Row, GroupByQuery> tooSmallGroupByFactory;
  private List<IncrementalIndex> incrementalIndices = Lists.newArrayList();
  private List<QueryableIndex> groupByIndices = Lists.newArrayList();
  private ExecutorService executorService;

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
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
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
        new IndexSpec(),
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
        new IndexSpec(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    groupByIndices = Arrays.asList(qindexA, qindexB);
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(3, "GroupByThreadPool[%d]");

    NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", 10_000_000),
        0,
        Integer.MAX_VALUE
    );

    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    BlockingPool<ByteBuffer> mergePool = new DefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 10_000_000),
        2
    );
    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    BlockingPool<ByteBuffer> tooSmallMergePool = new DefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 255),
        2
    );

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
            bufferPool,
            mergePool,
            new ObjectMapper(new SmileFactory()),
            NOOP_QUERYWATCHER
        )
    );

    final GroupByStrategySelector tooSmallStrategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            tooSmallDruidProcessingConfig,
            configSupplier,
            bufferPool,
            tooSmallMergePool,
            new ObjectMapper(new SmileFactory()),
            NOOP_QUERYWATCHER
        )
    );

    groupByFactory = new GroupByQueryRunnerFactory(
        strategySelector,
        new GroupByQueryQueryToolChest(
            strategySelector,
            NoopIntervalChunkingQueryRunnerDecorator()
        )
    );

    tooSmallGroupByFactory = new GroupByQueryRunnerFactory(
        tooSmallStrategySelector,
        new GroupByQueryQueryToolChest(
            tooSmallStrategySelector,
            NoopIntervalChunkingQueryRunnerDecorator()
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

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testPartialLimitPushDownMerge() throws Exception
  {
    // one segment's results use limit push down, the other doesn't because of insufficient buffer capacity

    QueryToolChest<Row, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<Row> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<Row> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            tooSmallGroupByFactory.mergeRunners(executorService, getRunner2())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<Row> theRunner3 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
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
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new DefaultDimensionSpec("dimA", null)
        ))
        .setAggregatorSpecs(
            Arrays.asList(new LongSumAggregatorFactory("metA", "metA"))
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(new OrderByColumnSpec("dimA", OrderByColumnSpec.Direction.DESCENDING)),
                3
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<Row> queryResult = theRunner3.run(QueryPlus.wrap(query), Maps.newHashMap());
    List<Row> results = Sequences.toList(queryResult, Lists.<Row>newArrayList());

    Row expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "zortaxx",
        "metA", 999L
    );
    Row expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "zebra",
        "metA", 180L
    );
    Row expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
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
  public void testPartialLimitPushDownMergeForceAggs() throws Exception
  {
    // one segment's results use limit push down, the other doesn't because of insufficient buffer capacity

    QueryToolChest<Row, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<Row> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getRunner1())
        ),
        (QueryToolChest) toolChest
    );


    QueryRunner<Row> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            tooSmallGroupByFactory.mergeRunners(executorService, getRunner2())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<Row> theRunner3 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
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
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new DefaultDimensionSpec("dimA", null)
        ))
        .setAggregatorSpecs(
            Arrays.asList(new LongSumAggregatorFactory("metA", "metA"))
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec("metA", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC)
                ),
                3
            )
        )
        .setGranularity(Granularities.ALL)
        .setContext(
            ImmutableMap.<String, Object>of(
              GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN,
              true
            )
        )
        .build();

    Sequence<Row> queryResult = theRunner3.run(QueryPlus.wrap(query), Maps.newHashMap());
    List<Row> results = Sequences.toList(queryResult, Lists.<Row>newArrayList());

    Row expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "zortaxx",
        "metA", 999L
    );
    Row expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "foo",
        "metA", 200L
    );
    Row expectedRow2 = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "mango",
        "metA", 190L
    );

    Assert.assertEquals(3, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
    Assert.assertEquals(expectedRow2, results.get(2));
  }

  private List<QueryRunner<Row>> getRunner1()
  {
    List<QueryRunner<Row>> runners = Lists.newArrayList();
    QueryableIndex index = groupByIndices.get(0);
    QueryRunner<Row> runner = makeQueryRunner(
        groupByFactory,
        index.toString(),
        new QueryableIndexSegment(index.toString(), index)
    );
    runners.add(groupByFactory.getToolchest().preMergeQueryDecoration(runner));
    return runners;
  }

  private List<QueryRunner<Row>> getRunner2()
  {
    List<QueryRunner<Row>> runners = Lists.newArrayList();
    QueryableIndex index2 = groupByIndices.get(1);
    QueryRunner<Row> tooSmallRunner = makeQueryRunner(
        tooSmallGroupByFactory,
        index2.toString(),
        new QueryableIndexSegment(index2.toString(), index2)
    );
    runners.add(tooSmallGroupByFactory.getToolchest().preMergeQueryDecoration(tooSmallRunner));
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
      String segmentId,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        new BySegmentQueryRunner<T>(
            segmentId, adapter.getDataInterval().getStart(),
            factory.createRunner(adapter)
        ),
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

  public static IntervalChunkingQueryRunnerDecorator NoopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null)
    {
      @Override
      public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest)
      {
        return new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
          {
            return delegate.run(queryPlus, responseContext);
          }
        };
      }
    };
  }
}
