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
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
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

public class GroupByMultiSegmentTest
{
  public static final ObjectMapper JSON_MAPPER;

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  private File tmpDir;
  private QueryRunnerFactory<Row, GroupByQuery> groupByFactory;
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
    event.put("dimA", "hello");
    event.put("metA", 100);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);
    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
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
    event.put("dimA", "foo");
    event.put("metA", 100);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);
    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    final File fileB = INDEX_MERGER_V9.persist(
        indexB,
        new File(tmpDir, "B"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    groupByIndices = Arrays.asList(qindexA, qindexB);
    resourceCloser = Closer.create();
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(2, "GroupByThreadPool[%d]");

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

    resourceCloser.register(bufferPool);
    resourceCloser.register(mergePool);
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

    groupByFactory = new GroupByQueryRunnerFactory(
        strategySelector,
        new GroupByQueryQueryToolChest(
            strategySelector,
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

    resourceCloser.close();

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testHavingAndNoLimitPushDown()
  {
    QueryToolChest<Row, GroupByQuery> toolChest = groupByFactory.getToolchest();
    QueryRunner<Row> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, makeGroupByMultiRunners())
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
                Collections.singletonList(new OrderByColumnSpec("dimA", OrderByColumnSpec.Direction.ASCENDING)),
                1
            )
        )
        .setHavingSpec(
            new GreaterThanHavingSpec("metA", 110)
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<Row> queryResult = theRunner.run(QueryPlus.wrap(query), new HashMap<>());
    List<Row> results = queryResult.toList();

    Row expectedRow = GroupByQueryRunnerTestHelper.createExpectedRow(
        "1970-01-01T00:00:00.000Z",
        "dimA", "world",
        "metA", 150L
    );

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expectedRow, results.get(0));
  }

  private List<QueryRunner<Row>> makeGroupByMultiRunners()
  {
    List<QueryRunner<Row>> runners = new ArrayList<>();

    for (QueryableIndex qindex : groupByIndices) {
      QueryRunner<Row> runner = makeQueryRunner(
          groupByFactory,
          SegmentId.dummy(qindex.toString()),
          new QueryableIndexSegment(qindex, SegmentId.dummy(qindex.toString()))
      );
      runners.add(groupByFactory.getToolchest().preMergeQueryDecoration(runner));
    }
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

  public static IntervalChunkingQueryRunnerDecorator NoopIntervalChunkingQueryRunnerDecorator()
  {
    return new IntervalChunkingQueryRunnerDecorator(null, null, null) {
      @Override
      public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest)
      {
        return new QueryRunner<T>() {
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
