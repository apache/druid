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
import org.apache.druid.js.JavaScriptConfig;
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
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
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

public class NestedQueryPushDownTest extends InitializedNullHandlingTest
{
  private static final IndexIO INDEX_IO;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  public static final ObjectMapper JSON_MAPPER;
  private File tmpDir;
  private QueryRunnerFactory<ResultRow, GroupByQuery> groupByFactory;
  private QueryRunnerFactory<ResultRow, GroupByQuery> groupByFactory2;
  private List<IncrementalIndex> incrementalIndices = new ArrayList<>();
  private List<QueryableIndex> groupByIndices = new ArrayList<>();
  private ExecutorService executorService;
  private Closer closer;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std().addValue(
            ExprMacroTable.class,
            ExprMacroTable.nil()
        )
    );
    INDEX_IO = new IndexIO(JSON_MAPPER, ColumnConfig.DEFAULT);
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(new DimensionsSpec(
                    Arrays.asList(
                        new StringDimensionSchema("dimA"),
                        new StringDimensionSchema("dimB"),
                        new LongDimensionSchema("metA"),
                        new LongDimensionSchema("metB")
                    )
                ))
                .build()
        )
        .setMaxRowCount(1000)
        .build();
  }

  @Before
  public void setup() throws Exception
  {
    closer = Closer.create();
    tmpDir = FileUtils.createTempDir();

    InputRow row;
    List<String> dimNames = Arrays.asList("dimA", "metA", "dimB", "metB");
    Map<String, Object> event;

    final IncrementalIndex indexA = makeIncIndex();
    incrementalIndices.add(indexA);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 10L);
    row = new MapBasedInputRow(1505260888888L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 20L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 10L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexA.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 20L);
    row = new MapBasedInputRow(1505264400400L, dimNames, event);
    indexA.add(row);

    final File fileA = INDEX_MERGER_V9.persist(
        indexA,
        new File(tmpDir, "A"),
        IndexSpec.DEFAULT,
        null
    );
    QueryableIndex qindexA = INDEX_IO.loadIndex(fileA);


    final IncrementalIndex indexB = makeIncIndex();
    incrementalIndices.add(indexB);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 10L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 1000L);
    event.put("dimB", "sweet");
    event.put("metB", 20L);
    row = new MapBasedInputRow(1505260800000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "pomegranate");
    event.put("metA", 1000L);
    event.put("dimB", "sour");
    event.put("metB", 10L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexB.add(row);

    event = new HashMap<>();
    event.put("dimA", "mango");
    event.put("metA", 1000L);
    event.put("dimB", "sour");
    event.put("metB", 20L);
    row = new MapBasedInputRow(1505264400000L, dimNames, event);
    indexB.add(row);

    final File fileB = INDEX_MERGER_V9.persist(
        indexB,
        new File(tmpDir, "B"),
        IndexSpec.DEFAULT,
        null
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    groupByIndices = Arrays.asList(qindexA, qindexB);
    setupGroupByFactory();
  }


  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(3, "GroupByThreadPool[%d]");

    TestBufferPool bufferPool = TestBufferPool.offHeap(10_000_000, Integer.MAX_VALUE);
    TestBufferPool mergePool = TestBufferPool.offHeap(10_000_000, 10);
    TestBufferPool mergePool2 = TestBufferPool.offHeap(10_000_000, 10);
    closer.register(() -> {
      // Verify that all objects have been returned to the pool.
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
    final GroupingEngine engine1 = new GroupingEngine(
        druidProcessingConfig,
        configSupplier,
        bufferPool,
        mergePool,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        NOOP_QUERYWATCHER
    );
    final GroupingEngine engine2 = new GroupingEngine(
        druidProcessingConfig,
        configSupplier,
        bufferPool,
        mergePool2,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        NOOP_QUERYWATCHER
    );

    groupByFactory = new GroupByQueryRunnerFactory(
        engine1,
        new GroupByQueryQueryToolChest(engine1)
    );

    groupByFactory2 = new GroupByQueryRunnerFactory(
        engine2,
        new GroupByQueryQueryToolChest(engine2)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
    closer = null;

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
  public void testSimpleDoubleAggregation()
  {

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("totalSum", "metASum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "dimB", "sour",
        "totalSum", 2000L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "dimB", "sweet",
        "totalSum", 6000L
    );

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
  }

  @Test
  public void testNestedQueryWithRenamedDimensions()
  {

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "newDimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("newDimB", "renamedDimB"))
        .setAggregatorSpecs(new LongMaxAggregatorFactory("maxBSum", "metBSum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .build();

    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "renamedDimB", "sour",
        "maxBSum", 20L
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "renamedDimB", "sweet",
        "maxBSum", 60L
    );
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
  }

  @Test
  public void testDimensionFilterOnOuterAndInnerQueries()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .setDimFilter(new JavaScriptDimFilter(
            "dimA",
            "function(dim){ return dim == 'mango' }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setDimensions(new DefaultDimensionSpec("dimA", "newDimA"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("finalSum", "metASum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .setDimFilter(new JavaScriptDimFilter(
            "dimA",
            "function(dim){ return dim == 'pomegranate' }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setQuerySegmentSpec(intervalSpec)
        .build();

    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testDimensionFilterOnOuterQuery()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setDimensions(new DefaultDimensionSpec("dimA", "newDimA"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("finalSum", "metASum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .setDimFilter(new JavaScriptDimFilter(
            "dimA",
            "function(dim){ return dim == 'mango' }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setQuerySegmentSpec(intervalSpec)
        .build();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "finalSum", 4000L,
        "newDimA", "mango"
    );
    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
  }

  @Test
  public void testDimensionFilterOnInnerQuery()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .setDimFilter(new JavaScriptDimFilter(
            "dimA",
            "function(dim){ return dim == 'mango' }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setDimensions(new DefaultDimensionSpec("dimA", "newDimA"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("finalSum", "metASum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "finalSum", 4000L,
        "newDimA", "mango"
    );
    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
  }

  @Test
  public void testSubqueryWithExtractionFnInOuterQuery()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setDimensions(new ExtractionDimensionSpec("dimA", "extractedDimA", new RegexDimExtractionFn("^(p)", true,
                                                                                                     "replacement"
        )))
        .setAggregatorSpecs(new LongSumAggregatorFactory("finalSum", "metASum"))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "finalSum", 4000L,
        "extractedDimA", "p"
    );
    ResultRow expectedRow1 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "finalSum", 4000L,
        "extractedDimA", "replacement"
    );
    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
    Assert.assertEquals(expectedRow1, results.get(1));
  }

  @Test
  public void testHavingClauseInNestedPushDownQuery()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );
    GroupByQuery innerQuery = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(innerQuery)
        .setDimensions(new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("finalSum", "metBSum"))
        .setHavingSpec(new GreaterThanHavingSpec("finalSum", 70L))
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true
            )
        )
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(intervalSpec)
        .build();

    ResultRow expectedRow0 = GroupByQueryRunnerTestHelper.createExpectedRow(
        nestedQuery,
        "2017-07-14T02:40:00.000Z",
        "dimB", "sweet",
        "finalSum", 90L
    );
    Sequence<ResultRow> queryResult = runNestedQueryWithForcePushDown(nestedQuery);
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expectedRow0, results.get(0));
  }

  private Sequence<ResultRow> runNestedQueryWithForcePushDown(GroupByQuery nestedQuery)
  {
    ResponseContext context = ResponseContext.createEmpty();
    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    GroupByQuery pushDownQuery = nestedQuery;
    QueryRunner<ResultRow> segment1Runner = new FinalizeResultsQueryRunner<ResultRow>(
        toolChest.mergeResults(
            groupByFactory.mergeRunners(executorService, getQueryRunnerForSegment1())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> segment2Runner = new FinalizeResultsQueryRunner<ResultRow>(
        toolChest.mergeResults(
            groupByFactory2.mergeRunners(executorService, getQueryRunnerForSegment2())
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> queryRunnerForSegments = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            (queryPlus, responseContext) -> Sequences
                .simple(
                    ImmutableList.of(
                        Sequences.map(
                            segment1Runner.run(queryPlus, responseContext),
                            toolChest.makePreComputeManipulatorFn(
                                (GroupByQuery) queryPlus.getQuery(),
                                MetricManipulatorFns.deserializing()
                            )
                        ),
                        Sequences.map(
                            segment2Runner.run(queryPlus, responseContext),
                            toolChest.makePreComputeManipulatorFn(
                                (GroupByQuery) queryPlus.getQuery(),
                                MetricManipulatorFns.deserializing()
                            )
                        )
                    )
                )
                .flatMerge(Function.identity(), queryPlus.getQuery().getResultOrdering())
        ),
        (QueryToolChest) toolChest
    );
    GroupingEngine groupingEngine = ((GroupByQueryRunnerFactory) groupByFactory).getGroupingEngine();
    // Historicals execute the query with force push down flag as false
    GroupByQuery queryWithPushDownDisabled = pushDownQuery.withOverriddenContext(ImmutableMap.of(
        GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY,
        false
    ));
    Sequence<ResultRow> pushDownQueryResults = groupingEngine.mergeResults(
        queryRunnerForSegments,
        queryWithPushDownDisabled,
        context
    );

    return toolChest.mergeResults((queryPlus, responseContext) -> pushDownQueryResults)
                    .run(QueryPlus.wrap(nestedQuery), context);
  }

  @Test
  public void testQueryRewriteForPushDown()
  {
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(1500000000000L, 1600000000000L))
    );

    String outputNameB = "dimBOutput";
    String outputNameAgg = "totalSum";
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("blah")
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimA", "dimA"), new DefaultDimensionSpec("dimB", "dimB"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("metASum", "metA"),
            new LongSumAggregatorFactory("metBSum", "metB")
        )
        .setGranularity(Granularities.ALL)
        .build();

    GroupByQuery nestedQuery = GroupByQuery
        .builder()
        .setDataSource(query)
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(new DefaultDimensionSpec("dimB", outputNameB))
        .setAggregatorSpecs(new LongSumAggregatorFactory(outputNameAgg, "metASum"))
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, true))
        .setGranularity(Granularities.ALL)
        .build();
    QueryToolChest<ResultRow, GroupByQuery> toolChest = groupByFactory.getToolchest();
    GroupByQuery rewrittenQuery = ((GroupByQueryQueryToolChest) toolChest).rewriteNestedQueryForPushDown(nestedQuery);
    Assert.assertEquals(outputNameB, rewrittenQuery.getDimensions().get(0).getDimension());
    Assert.assertEquals(outputNameAgg, rewrittenQuery.getAggregatorSpecs().get(0).getName());
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


  private List<QueryRunner<ResultRow>> getQueryRunnerForSegment1()
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index = groupByIndices.get(0);
    QueryRunner<ResultRow> runner = makeQueryRunnerForSegment(
        groupByFactory,
        SegmentId.dummy(index.toString()),
        new QueryableIndexSegment(index, SegmentId.dummy(index.toString()))
    );
    runners.add(groupByFactory.getToolchest().preMergeQueryDecoration(runner));
    return runners;
  }

  private List<QueryRunner<ResultRow>> getQueryRunnerForSegment2()
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    QueryableIndex index2 = groupByIndices.get(1);
    QueryRunner<ResultRow> tooSmallRunner = makeQueryRunnerForSegment(
        groupByFactory2,
        SegmentId.dummy(index2.toString()),
        new QueryableIndexSegment(index2, SegmentId.dummy(index2.toString()))
    );
    runners.add(groupByFactory2.getToolchest().preMergeQueryDecoration(tooSmallRunner));
    return runners;
  }

  private static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunnerForSegment(
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
