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

package org.apache.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AggregatorMergeStrategy;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnSize;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.LogicalSegment;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class SegmentMetadataQueryTest extends InitializedNullHandlingTest
{
  private static final SegmentMetadataQueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String DATASOURCE = "testDatasource";

  @SuppressWarnings("unchecked")
  public static QueryRunner makeMMappedQueryRunner(
      SegmentId segmentId,
      boolean rollup,
      boolean bitmaps,
      QueryRunnerFactory factory
  )
  {
    QueryableIndex index;
    if (bitmaps) {
      index = rollup ? TestIndex.getMMappedTestIndex() : TestIndex.getNoRollupMMappedTestIndex();
    } else {
      index = TestIndex.getNoBitmapMMappedTestIndex();
    }
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        segmentId,
        new QueryableIndexSegment(index, segmentId),
        null
    );
  }

  @SuppressWarnings("unchecked")
  public static QueryRunner makeIncrementalIndexQueryRunner(
      SegmentId segmentId,
      boolean rollup,
      boolean bitmaps,
      QueryRunnerFactory factory
  )
  {
    IncrementalIndex index;
    if (bitmaps) {
      index = rollup ? TestIndex.getIncrementalTestIndex() : TestIndex.getNoRollupIncrementalTestIndex();
    } else {
      index = TestIndex.getNoBitmapIncrementalTestIndex();
    }
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        segmentId,
        new IncrementalIndexSegment(index, segmentId),
        null
    );
  }

  private final QueryRunner runner1;
  private final QueryRunner runner2;
  private final boolean mmap1;
  private final boolean mmap2;
  private final boolean rollup1;
  private final boolean rollup2;
  private final boolean differentIds;
  private final SegmentMetadataQuery testQuery;
  private final SegmentAnalysis expectedSegmentAnalysis1;
  private final SegmentAnalysis expectedSegmentAnalysis2;
  private final boolean bitmaps;

  @Parameterized.Parameters(name = "mmap1 = {0}, mmap2 = {1}, rollup1 = {2}, rollup2 = {3}, differentIds = {4}, bitmaps={5}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{true, true, true, true, false, true},
        new Object[]{true, false, true, false, false, true},
        new Object[]{false, true, true, false, false, true},
        new Object[]{false, false, false, false, false, true},
        new Object[]{false, false, true, true, false, true},
        new Object[]{false, false, false, true, true, true},
        new Object[]{true, true, false, false, false, false}
    );
  }

  public SegmentMetadataQueryTest(
      boolean mmap1,
      boolean mmap2,
      boolean rollup1,
      boolean rollup2,
      boolean differentIds,
      boolean bitmaps
  )
  {
    final SegmentId id1 = SegmentId.dummy(differentIds ? "testSegment1" : DATASOURCE);
    final SegmentId id2 = SegmentId.dummy(differentIds ? "testSegment2" : DATASOURCE);
    this.runner1 = mmap1
                   ? makeMMappedQueryRunner(id1, rollup1, bitmaps, FACTORY)
                   : makeIncrementalIndexQueryRunner(id1, rollup1, bitmaps, FACTORY);
    this.runner2 = mmap2
                   ? makeMMappedQueryRunner(id2, rollup2, bitmaps, FACTORY)
                   : makeIncrementalIndexQueryRunner(id2, rollup2, bitmaps, FACTORY);
    this.mmap1 = mmap1;
    this.mmap2 = mmap2;
    this.rollup1 = rollup1;
    this.rollup2 = rollup2;
    this.differentIds = differentIds;
    this.bitmaps = bitmaps;
    testQuery = Druids.newSegmentMetadataQueryBuilder()
                      .dataSource(DATASOURCE)
                      .intervals("2013/2014")
                      .toInclude(new ListColumnIncluderator(Arrays.asList("__time", "index", "placement")))
                      .analysisTypes(
                          SegmentMetadataQuery.AnalysisType.CARDINALITY,
                          SegmentMetadataQuery.AnalysisType.SIZE,
                          SegmentMetadataQuery.AnalysisType.INTERVAL,
                          SegmentMetadataQuery.AnalysisType.MINMAX,
                          SegmentMetadataQuery.AnalysisType.AGGREGATORS
                      )
                      .merge(true)
                      .build();

    int preferedSize1 = 0;
    int placementSize2 = 0;
    int overallSize1 = 153543;
    int overallSize2 = 153543;
    if (bitmaps) {
      preferedSize1 = mmap1 ? 10881 : 10764;
      placementSize2 = mmap2 ? 10881 : 0;
      overallSize1 = mmap1 ? 201345 : 200831;
      overallSize2 = mmap2 ? 201345 : 200831;
    }

    final Map<String, AggregatorFactory> expectedAggregators = new HashMap<>();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }

    expectedSegmentAnalysis1 = new SegmentAnalysis(
        id1.toString(),
        ImmutableList.of(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        new LinkedHashMap<>(
            ImmutableMap.of(
                "__time",
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.toString(),
                    false,
                    false,
                    12090,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                "index",
                new ColumnAnalysis(
                    ColumnType.DOUBLE,
                    ValueType.DOUBLE.toString(),
                    false,
                    false,
                    9672,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    preferedSize1,
                    1,
                    "preferred",
                    "preferred",
                    null,
                    null
                )
            )
        ),
        overallSize1,
        1209,
        expectedAggregators,
        null,
        null,
        null
    );
    expectedSegmentAnalysis2 = new SegmentAnalysis(
        id2.toString(),
        ImmutableList.of(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        new LinkedHashMap<>(
            ImmutableMap.of(
                "__time",
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.toString(),
                    false,
                    false,
                    12090,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                "index",
                new ColumnAnalysis(
                    ColumnType.DOUBLE,
                    ValueType.DOUBLE.toString(),
                    false,
                    false,
                    9672,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    placementSize2,
                    1,
                    null,
                    null,
                    null,
                    null
                )
            )
        ),
        // null_column will be included only for incremental index, which makes a little bigger result than expected
        overallSize2,
        1209,
        expectedAggregators,
        null,
        null,
        null
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSegmentMetadataQuery()
  {
    List<SegmentAnalysis> results = runner1.run(QueryPlus.wrap(testQuery)).toList();

    Assert.assertEquals(Collections.singletonList(expectedSegmentAnalysis1), results);
  }

  @Test
  public void testSegmentMetadataQueryWithRollupMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                ),
                "placementish",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    true,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        null,
        rollup1 != rollup2 ? null : rollup1
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "placementish")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.ROLLUP)
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithHasMultipleValuesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    1,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                ),
                "placementish",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    true,
                    false,
                    0,
                    9,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "placementish")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithComplexColumnMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    1,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                ),
                "quality_uniques",
                new ColumnAnalysis(
                    ColumnType.ofComplex("hyperUnique"),
                    "hyperUnique",
                    false,
                    true,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "quality_uniques")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge()
  {
    int size1 = 0;
    int size2 = 0;
    if (bitmaps) {
      size1 = mmap1 ? 10881 : 10764;
      size2 = mmap2 ? 10881 : 10764;
    }
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.STRING,
        ValueType.STRING.toString(),
        false,
        false,
        size1 + size2,
        1,
        "preferred",
        "preferred",
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("placement", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge2()
  {
    int size1 = 0;
    int size2 = 0;
    if (bitmaps) {
      size1 = mmap1 ? 6882 : 6808;
      size2 = mmap2 ? 6882 : 6808;
    }
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.STRING,
        ValueType.STRING.toString(),
        false,
        false,
        size1 + size2,
        3,
        "spot",
        "upfront",
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("market", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge3()
  {
    int size1 = 0;
    int size2 = 0;
    if (bitmaps) {
      size1 = mmap1 ? 9765 : 9660;
      size2 = mmap2 ? 9765 : 9660;
    }
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.STRING,
        ValueType.STRING.toString(),
        false,
        false,
        size1 + size2,
        9,
        "automotive",
        "travel",
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("quality", analysis);
  }

  private void testSegmentMetadataQueryWithDefaultAnalysisMerge(
      String column,
      ColumnAnalysis analysis
  )
  {
    final Map<String, AggregatorFactory> expectedAggregators = new HashMap<>();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }

    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        ImmutableList.of(expectedSegmentAnalysis1.getIntervals().get(0)),
        new LinkedHashMap<>(
            ImmutableMap.of(
                "__time",
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.toString(),
                    false,
                    false,
                    12090 * 2,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                "index",
                new ColumnAnalysis(
                    ColumnType.DOUBLE,
                    ValueType.DOUBLE.toString(),
                    false,
                    false,
                    9672 * 2,
                    null,
                    null,
                    null,
                    null,
                    null
                ),
                column,
                analysis
            )
        ),
        expectedSegmentAnalysis1.getSize() + expectedSegmentAnalysis2.getSize(),
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        expectedAggregators,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    Query query = testQuery.withColumns(new ListColumnIncluderator(Arrays.asList("__time", "index", column)));

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithNoAnalysisTypesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes()
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorsMerge()
  {
    final Map<String, AggregatorFactory> expectedAggregators = new HashMap<>();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        expectedAggregators,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.AGGREGATORS)
        .merge(true) // if the aggregator strategy is unsepcified, it defaults to strict.
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorsMergeLenientStrategy()
  {
    final Map<String, AggregatorFactory> expectedAggregators = new HashMap<>();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        expectedAggregators,
        null,
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.AGGREGATORS)
        .merge(true)
        .aggregatorMergeStrategy(AggregatorMergeStrategy.LENIENT)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithTimestampSpecMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        new TimestampSpec("ds", "auto", null),
        null,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.TIMESTAMPSPEC)
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithQueryGranularityMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : SegmentId.dummy(DATASOURCE).toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    NullHandling.defaultStringValue(),
                    NullHandling.defaultStringValue(),
                    null,
                    null
                )
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        Granularities.NONE,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                Lists.newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    SegmentMetadataQuery query = Druids
        .newSegmentMetadataQueryBuilder()
        .dataSource(DATASOURCE)
        .intervals("2013/2014")
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes(SegmentMetadataQuery.AnalysisType.QUERYGRANULARITY)
        .merge(true)
        .build();
    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(QueryPlus.wrap(query)),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResults()
  {
    Result<BySegmentResultValue> bySegmentResult = new Result<BySegmentResultValue>(
        expectedSegmentAnalysis1.getIntervals().get(0).getStart(),
        new BySegmentResultValueClass(
            Collections.singletonList(
                expectedSegmentAnalysis1
            ), expectedSegmentAnalysis1.getId(), testQuery.getIntervals().get(0)
        )
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    QueryRunner singleSegmentQueryRunner = toolChest.preMergeQueryDecoration(runner1);
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                Execs.directExecutor(),
                //Note: It is essential to have atleast 2 query runners merged to reproduce the regression bug described in
                //https://github.com/apache/druid/pull/1172
                //the bug surfaces only when ordering is used which happens only when you have 2 things to compare
                Lists.newArrayList(singleSegmentQueryRunner, singleSegmentQueryRunner)
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(bySegmentResult, bySegmentResult),
        myRunner.run(QueryPlus.wrap(testQuery.withOverriddenContext(ImmutableMap.of(
            QueryContexts.BY_SEGMENT_KEY,
            true
        )))),
        "failed SegmentMetadata bySegment query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSerde() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"intervals\":[\"2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z\"],\n"
                      + "  \"analysisTypes\":[\"cardinality\",\"size\"]\n"
                      + "}";

    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysisTypes = EnumSet.of(
        SegmentMetadataQuery.AnalysisType.CARDINALITY,
        SegmentMetadataQuery.AnalysisType.SIZE
    );

    Query query = MAPPER.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getTableNames()));
    Assert.assertEquals(
        Intervals.of("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"),
        query.getIntervals().get(0)
    );
    Assert.assertEquals(expectedAnalysisTypes, ((SegmentMetadataQuery) query).getAnalysisTypes());
    Assert.assertEquals(AggregatorMergeStrategy.STRICT, ((SegmentMetadataQuery) query).getAggregatorMergeStrategy());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));

    // test copy
    Assert.assertEquals(query, Druids.SegmentMetadataQueryBuilder.copy((SegmentMetadataQuery) query).build());
  }

  @Test
  public void testSerdeWithDefaultInterval() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\"\n"
                      + "}";
    Query query = MAPPER.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertTrue(query.getDataSource() instanceof TableDataSource);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getTableNames()));
    Assert.assertEquals(Intervals.ETERNITY, query.getIntervals().get(0));
    Assert.assertTrue(((SegmentMetadataQuery) query).isUsingDefaultInterval());
    Assert.assertEquals(AggregatorMergeStrategy.STRICT, ((SegmentMetadataQuery) query).getAggregatorMergeStrategy());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));

    // test copy
    Assert.assertEquals(query, Druids.SegmentMetadataQueryBuilder.copy((SegmentMetadataQuery) query).build());
  }

  @Test
  public void testSerdeWithLatestAggregatorStrategy() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"aggregatorMergeStrategy\":\"latest\"\n"
                      + "}";
    Query query = MAPPER.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertTrue(query.getDataSource() instanceof TableDataSource);
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getTableNames()));
    Assert.assertEquals(Intervals.ETERNITY, query.getIntervals().get(0));
    Assert.assertTrue(((SegmentMetadataQuery) query).isUsingDefaultInterval());
    Assert.assertEquals(AggregatorMergeStrategy.LATEST, ((SegmentMetadataQuery) query).getAggregatorMergeStrategy());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));

    // test copy
    Assert.assertEquals(query, Druids.SegmentMetadataQueryBuilder.copy((SegmentMetadataQuery) query).build());
  }

  @Test
  public void testSerdeWithBothDeprecatedAndNewParameters()
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"lenientAggregatorMerge\":\"true\",\n"
                      + "  \"aggregatorMergeStrategy\":\"lenient\"\n"
                      + "}";

    ValueInstantiationException exception = Assert.assertThrows(
        ValueInstantiationException.class,
        () -> MAPPER.readValue(queryStr, Query.class)
    );

    Assert.assertTrue(
        exception.getCause().getMessage().contains(
            "Both lenientAggregatorMerge [true] and aggregatorMergeStrategy [lenient] parameters cannot be set. Consider using aggregatorMergeStrategy since lenientAggregatorMerge is deprecated."
        )
    );
  }

  @Test
  public void testDefaultIntervalAndFiltering()
  {
    SegmentMetadataQuery testQuery = Druids.newSegmentMetadataQueryBuilder()
                                           .dataSource(DATASOURCE)
                                           .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
                                           .merge(true)
                                           .build();
    /* No interval specified, should use default interval */
    Assert.assertTrue(testQuery.isUsingDefaultInterval());
    Assert.assertEquals(Intervals.ETERNITY, testQuery.getIntervals().get(0));
    Assert.assertEquals(testQuery.getIntervals().size(), 1);

    List<LogicalSegment> testSegments = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2012-01-01/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2012-01-01T01/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2013-01-05/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2013-05-20/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2014-01-05/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2014-02-05/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-19T01/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-20T02/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        }
    );

    /* Test default period filter */
    List<LogicalSegment> filteredSegments = new SegmentMetadataQueryQueryToolChest(
        new SegmentMetadataQueryConfig()
    ).filterSegments(
        testQuery,
        testSegments
    );

    List<LogicalSegment> expectedSegments = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-19T01/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-20T02/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        }
    );

    Assert.assertEquals(filteredSegments.size(), 2);
    for (int i = 0; i < filteredSegments.size(); i++) {
      Assert.assertEquals(expectedSegments.get(i).getInterval(), filteredSegments.get(i).getInterval());
    }

    /* Test 2 year period filtering */
    SegmentMetadataQueryConfig twoYearPeriodCfg = new SegmentMetadataQueryConfig("P2Y");
    List<LogicalSegment> filteredSegments2 = new SegmentMetadataQueryQueryToolChest(
        twoYearPeriodCfg
    ).filterSegments(
        testQuery,
        testSegments
    );

    List<LogicalSegment> expectedSegments2 = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2013-05-20/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2014-01-05/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2014-02-05/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-19T01/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2015-01-20T02/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        }
    );

    Assert.assertEquals(filteredSegments2.size(), 5);
    for (int i = 0; i < filteredSegments2.size(); i++) {
      Assert.assertEquals(expectedSegments2.get(i).getInterval(), filteredSegments2.get(i).getInterval());
    }
  }

  @Test
  public void testCacheKeyWithListColumnIncluderator()
  {
    SegmentMetadataQuery oneColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource(DATASOURCE)
                                                .toInclude(new ListColumnIncluderator(Collections.singletonList("foo")))
                                                .build();

    SegmentMetadataQuery twoColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource(DATASOURCE)
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("fo", "o")))
                                                .build();

    final byte[] oneColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()).getCacheStrategy(
                                                                                                                      oneColumnQuery)
                                                                                                                  .computeCacheKey(
                                                                                                                      oneColumnQuery);

    final byte[] twoColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()).getCacheStrategy(
                                                                                                                      twoColumnQuery)
                                                                                                                  .computeCacheKey(
                                                                                                                      twoColumnQuery);

    Assert.assertFalse(Arrays.equals(oneColumnQueryCacheKey, twoColumnQueryCacheKey));
  }

  @Test
  public void testAnanlysisTypesBeingSet()
  {
    SegmentMetadataQuery query1 = Druids.newSegmentMetadataQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .toInclude(new ListColumnIncluderator(Collections.singletonList("foo")))
                                        .build();

    SegmentMetadataQuery query2 = Druids.newSegmentMetadataQueryBuilder()
                                        .dataSource(DATASOURCE)
                                        .toInclude(new ListColumnIncluderator(Collections.singletonList("foo")))
                                        .analysisTypes(SegmentMetadataQuery.AnalysisType.MINMAX)
                                        .build();

    SegmentMetadataQueryConfig emptyCfg = new SegmentMetadataQueryConfig();
    SegmentMetadataQueryConfig analysisCfg = new SegmentMetadataQueryConfig();
    analysisCfg.setDefaultAnalysisTypes(EnumSet.of(SegmentMetadataQuery.AnalysisType.CARDINALITY));

    EnumSet<SegmentMetadataQuery.AnalysisType> analysis1 = query1.withFinalizedAnalysisTypes(emptyCfg)
                                                                 .getAnalysisTypes();
    EnumSet<SegmentMetadataQuery.AnalysisType> analysis2 = query2.withFinalizedAnalysisTypes(emptyCfg)
                                                                 .getAnalysisTypes();
    EnumSet<SegmentMetadataQuery.AnalysisType> analysisWCfg1 = query1.withFinalizedAnalysisTypes(analysisCfg)
                                                                     .getAnalysisTypes();
    EnumSet<SegmentMetadataQuery.AnalysisType> analysisWCfg2 = query2.withFinalizedAnalysisTypes(analysisCfg)
                                                                     .getAnalysisTypes();

    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysis1 = new SegmentMetadataQueryConfig().getDefaultAnalysisTypes();
    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysis2 = EnumSet.of(SegmentMetadataQuery.AnalysisType.MINMAX);
    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysisWCfg1 = EnumSet.of(SegmentMetadataQuery.AnalysisType.CARDINALITY);
    EnumSet<SegmentMetadataQuery.AnalysisType> expectedAnalysisWCfg2 = EnumSet.of(SegmentMetadataQuery.AnalysisType.MINMAX);

    Assert.assertEquals(analysis1, expectedAnalysis1);
    Assert.assertEquals(analysis2, expectedAnalysis2);
    Assert.assertEquals(analysisWCfg1, expectedAnalysisWCfg1);
    Assert.assertEquals(analysisWCfg2, expectedAnalysisWCfg2);
  }

  @Test
  public void testLongNullableColumn()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.LONG,
        ValueType.LONG.toString(),
        false,
        NullHandling.replaceWithDefault() ? false : true,
        19344,
        null,
        null,
        null,
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("longNumericNull", analysis);
  }

  @Test
  public void testDoubleNullableColumn()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.DOUBLE,
        ValueType.DOUBLE.toString(),
        false,
        NullHandling.replaceWithDefault() ? false : true,
        19344,
        null,
        null,
        null,
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("doubleNumericNull", analysis);
  }


  @Test
  public void testFloatNullableColumn()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.FLOAT,
        ValueType.FLOAT.toString(),
        false,
        NullHandling.replaceWithDefault() ? false : true,
        19344,
        null,
        null,
        null,
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("floatNumericNull", analysis);
  }

  @Test
  public void testStringNullOnlyColumn()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ColumnType.STRING,
        ValueType.STRING.toString(),
        false,
        true,
        0,
        1,
        NullHandling.defaultStringValue(),
        NullHandling.defaultStringValue(),
        null,
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("null_column", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithInvalidDatasourceTypes()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                InlineDataSource.fromIterable(
                    ImmutableList.of(new Object[0]),
                    RowSignature.builder().add("column", ColumnType.STRING).build()
                ),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                null,
                null
            )
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageIs(
                "Invalid dataSource type [InlineDataSource{signature={column:STRING}}]. SegmentMetadataQuery only supports table or union datasources.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                new LookupDataSource("lookyloo"),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                null,
                null
            )
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageIs(
                "Invalid dataSource type [LookupDataSource{lookupName='lookyloo'}]. SegmentMetadataQuery only supports table or union datasources.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                JoinDataSource.create(
                    new TableDataSource("table1"),
                    new TableDataSource("table2"),
                    "j.",
                    "x == \"j.x\"",
                    JoinType.LEFT,
                    null,
                    ExprMacroTable.nil(),
                    null
                ),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                null,
                null
            )
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageIs(
                "Invalid dataSource type [JoinDataSource{left=table1, right=table2, rightPrefix='j.', condition=x == \"j.x\", joinType=LEFT, leftFilter=null}]. SegmentMetadataQuery only supports table or union datasources.")
    );
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorMergeStrictStrategy()
  {
    // This is the default behavior -- if nothing is specified, the merge strategy is strict.
    Assert.assertEquals(
        AggregatorMergeStrategy.STRICT,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            null,
            null
        ).getAggregatorMergeStrategy()
    );

    Assert.assertEquals(
        AggregatorMergeStrategy.STRICT,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            false,
            null
        ).getAggregatorMergeStrategy()
    );

    Assert.assertEquals(
        AggregatorMergeStrategy.STRICT,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            null,
            AggregatorMergeStrategy.STRICT
        ).getAggregatorMergeStrategy()
    );
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorMergeLenientStrategy()
  {
    Assert.assertEquals(
        AggregatorMergeStrategy.LENIENT,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            true,
            null
        ).getAggregatorMergeStrategy()
    );

    Assert.assertEquals(
        AggregatorMergeStrategy.LENIENT,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            null,
            AggregatorMergeStrategy.LENIENT
        ).getAggregatorMergeStrategy()
    );
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorMergeLatestStrategy()
  {
    Assert.assertEquals(
        AggregatorMergeStrategy.LATEST,
        new SegmentMetadataQuery(
            new TableDataSource("foo"),
            new LegacySegmentSpec("2015-01-01/2015-01-02"),
            null,
            null,
            null,
            null,
            false,
            null,
            AggregatorMergeStrategy.LATEST
        ).getAggregatorMergeStrategy()
    );
  }

  @Test
  public void testSegmentMetadataQueryWithBothDeprecatedAndNewParameter()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                new TableDataSource("foo"),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                false,
                AggregatorMergeStrategy.STRICT
            )
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageIs(
                                 "Both lenientAggregatorMerge [false] and aggregatorMergeStrategy [strict] parameters cannot be set."
                                 + " Consider using aggregatorMergeStrategy since lenientAggregatorMerge is deprecated.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                new TableDataSource("foo"),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                true,
                AggregatorMergeStrategy.LENIENT
            )
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageIs(
                                 "Both lenientAggregatorMerge [true] and aggregatorMergeStrategy [lenient] parameters cannot be set."
                                 + " Consider using aggregatorMergeStrategy since lenientAggregatorMerge is deprecated.")
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new SegmentMetadataQuery(
                new TableDataSource("foo"),
                new LegacySegmentSpec("2015-01-01/2015-01-02"),
                null,
                null,
                null,
                null,
                false,
                false,
                AggregatorMergeStrategy.LATEST
            )
        ),
        DruidExceptionMatcher.invalidInput()
                             .expectMessageIs(
                                 "Both lenientAggregatorMerge [false] and aggregatorMergeStrategy [latest] parameters cannot be set."
                                 + " Consider using aggregatorMergeStrategy since lenientAggregatorMerge is deprecated.")
    );
  }

  @Test
  public void testColumnSizeAndSmooshAnalysis()
  {
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource(DATASOURCE)
                                       .intervals("2013/2014")
                                       .toInclude(
                                           new ListColumnIncluderator(
                                               Arrays.asList(
                                                   "__time",
                                                   "index",
                                                   "placement",
                                                   "floatNumericNull",
                                                   "quality_uniques"
                                               )
                                           )
                                       )
                                       .analysisTypes(
                                           SegmentMetadataQuery.AnalysisType.COLUMN_SIZE,
                                           SegmentMetadataQuery.AnalysisType.SMOOSH_SIZE
                                       )
                                       .merge(true)
                                       .build();
    List<SegmentAnalysis> results = runner1.run(QueryPlus.wrap(query)).toList();

    LinkedHashMap<String, Integer> smooshAnalysis;
    ColumnSize timeSize;
    ColumnSize indexSize;
    ColumnSize placementSize;
    ColumnSize floatNumericNullSize;
    ColumnSize qualityUniquesSize;
    if (mmap1) {
      timeSize = new ColumnSize(
          694L,
          new LinkedHashMap<>(
              ImmutableMap.of(
                  ColumnSize.LONG_COLUMN_PART,
                  ColumnPartSize.simple("compressed longs column compression:[lz4] values per block:[8192]", 694L)
              )
          ),
          null
      );

      if (bitmaps) {
        placementSize = new ColumnSize(
            104L,
            new LinkedHashMap<>(
                ImmutableMap.of(
                    ColumnSize.ENCODED_VALUE_COLUMN_PART,
                    ColumnPartSize.simple("dictionary encoded compressed[lz4] vsize[1]", 44L),
                    ColumnSize.STRING_VALUE_DICTIONARY_COLUMN_PART,
                    ColumnPartSize.simple("[generic v1] with [1] values", 27L),
                    ColumnSize.BITMAP_VALUE_INDEX_COLUMN_PART,
                    ColumnPartSize.simple("[generic v1] with [1] values", 33L)
                )
            ),
            null
        );
      } else {
        placementSize = new ColumnSize(
            71L,
            new LinkedHashMap<>(
                ImmutableMap.of(
                    ColumnSize.ENCODED_VALUE_COLUMN_PART,
                    ColumnPartSize.simple("dictionary encoded compressed[lz4] vsize[1]", 44L),
                    ColumnSize.STRING_VALUE_DICTIONARY_COLUMN_PART,
                    ColumnPartSize.simple("[generic v1] with [1] values", 27L)
                )
            ),
            null
        );
      }

      indexSize = new ColumnSize(
          9545L,
          new LinkedHashMap<>(
              ImmutableMap.of(
                  ColumnSize.DOUBLE_COLUMN_PART,
                  ColumnPartSize.simple("compressed doubles column compression:[lz4] values per block:[8192]", 9545L)
              )
          ),
          null
      );


      if (NullHandling.sqlCompatible()) {
        floatNumericNullSize = new ColumnSize(
            671L,
            new LinkedHashMap<>(
                ImmutableMap.of(
                    ColumnSize.FLOAT_COLUMN_PART,
                    ColumnPartSize.simple("compressed floats column compression:[lz4] values per block:[16384]", 93L),
                    ColumnSize.NULL_VALUE_INDEX_COLUMN_PART,
                    ColumnPartSize.simple("nullBitmap", 578L)
                )
            ),
            null
        );
      } else {
        floatNumericNullSize = new ColumnSize(
            93L,
            new LinkedHashMap<>(
                ImmutableMap.of(
                    ColumnSize.FLOAT_COLUMN_PART,
                    ColumnPartSize.simple("compressed floats column compression:[lz4] values per block:[16384]", 93L)
                )
            ),
            null
        );
      }

      qualityUniquesSize = new ColumnSize(
          21772L,
          new LinkedHashMap<>(
              ImmutableMap.of(
                  ColumnSize.DATA_SECTION,
                  ColumnPartSize.simple("[generic v1] with [1209] values", 21772)
              )
          ),
          null
      );

      if (NullHandling.sqlCompatible()) {
        smooshAnalysis = new LinkedHashMap<>(
            ImmutableMap.<String, Integer>builder()
                        .put("__time", 844)
                        .put("doubleNumericNull", 847)
                        .put("floatNumericNull", 823)
                        .put("index", 9709)
                        .put("index.drd", 1005)
                        .put("indexFloat", 5001)
                        .put("indexMaxFloat", 5001)
                        .put("indexMaxPlusTen", 9723)
                        .put("indexMin", 9709)
                        .put("indexMinFloat", 5001)
                        .put("longNumericNull", 836)
                        .put("market", bitmaps ? 1464 : 271)
                        .put("metadata.drd", bitmaps ? 669 : 670)
                        .put("null_column", 133)
                        .put("partial_null_column", bitmaps ? 1040 : 243)
                        .put("placement", bitmaps ? 267 : 234)
                        .put("placementish", bitmaps ? 7850 : 5207)
                        .put("quality", bitmaps ? 3018 : 374)
                        .put("qualityDouble", 284)
                        .put("qualityFloat", 252)
                        .put("qualityLong", 273)
                        .put("qualityNumericString", bitmaps ? 2999 : 355)
                        .put("quality_uniques", 21879)
                        .build()
        );
      } else {
        smooshAnalysis = new LinkedHashMap<>(
            ImmutableMap.<String, Integer>builder()
                        .put("__time", 798)
                        .put("doubleNumericNull", 223)
                        .put("floatNumericNull", 199)
                        .put("index", 9663)
                        .put("index.drd", 1005)
                        .put("indexFloat", 4955)
                        .put("indexMaxFloat", 4955)
                        .put("indexMaxPlusTen", 9677)
                        .put("indexMin", 9663)
                        .put("indexMinFloat", 4955)
                        .put("longNumericNull", 212)
                        .put("market", bitmaps ? 1464 : 271)
                        .put("metadata.drd", bitmaps ? 669 : 670)
                        .put("null_column", 133)
                        .put("partial_null_column", bitmaps ? 1040 : 243)
                        .put("placement", bitmaps ? 267 : 234)
                        .put("placementish", bitmaps ? 7850 : 5207)
                        .put("quality", bitmaps ? 3018 : 374)
                        .put("qualityDouble", 238)
                        .put("qualityFloat", 206)
                        .put("qualityLong", 227)
                        .put("qualityNumericString", bitmaps ? 2999 : 355)
                        .put("quality_uniques", 21879)
                        .build()
        );
      }
    } else {
      timeSize = null;
      indexSize = null;
      placementSize = null;
      floatNumericNullSize = null;
      qualityUniquesSize = null;
      smooshAnalysis = null;
    }
    final SegmentId id1 = SegmentId.dummy(differentIds ? "testSegment1" : DATASOURCE);
    SegmentAnalysis expected = new SegmentAnalysis(
        id1.toString(),
        null,
        new LinkedHashMap<>(
            ImmutableMap.of(
                "__time",
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.toString(),
                    false,
                    false,
                    0,
                    null,
                    null,
                    null,
                    timeSize,
                    null
                ),
                "floatNumericNull",
                new ColumnAnalysis(
                    ColumnType.FLOAT,
                    ValueType.FLOAT.toString(),
                    false,
                    NullHandling.sqlCompatible(),
                    0,
                    null,
                    null,
                    null,
                    floatNumericNullSize,
                    null
                ),
                "placement",
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.toString(),
                    false,
                    false,
                    0,
                    0,
                    null,
                    null,
                    placementSize,
                    null
                ),
                "index",
                new ColumnAnalysis(
                    ColumnType.DOUBLE,
                    ValueType.DOUBLE.toString(),
                    false,
                    false,
                    0,
                    null,
                    null,
                    null,
                    indexSize,
                    null
                ),
                "quality_uniques",
                new ColumnAnalysis(
                    HyperUniquesAggregatorFactory.TYPE,
                    HyperUniquesAggregatorFactory.TYPE.getComplexTypeName(),
                    false,
                    true,
                    0,
                    null,
                    null,
                    null,
                    qualityUniquesSize,
                    null
                )
            )
        ),
        0,
        1209,
        null,
        null,
        null,
        null,
        smooshAnalysis
    );
    Assert.assertEquals(Collections.singletonList(expected), results);
  }
}
