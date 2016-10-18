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

package io.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.LogicalSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class SegmentMetadataQueryTest
{
  private static final SegmentMetadataQueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @SuppressWarnings("unchecked")
  public static QueryRunner makeMMappedQueryRunner(
      String segmentId,
      boolean rollup,
      QueryRunnerFactory factory
  )
  {
    QueryableIndex index = rollup ? TestIndex.getMMappedTestIndex() : TestIndex.getNoRollupMMappedTestIndex();
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        segmentId,
        new QueryableIndexSegment(segmentId, index),
        null
    );
  }

  @SuppressWarnings("unchecked")
  public static QueryRunner makeIncrementalIndexQueryRunner(
      String segmentId,
      boolean rollup,
      QueryRunnerFactory factory
  )
  {
    IncrementalIndex index = rollup ? TestIndex.getIncrementalTestIndex() : TestIndex.getNoRollupIncrementalTestIndex();
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

  @Parameterized.Parameters(name = "mmap1 = {0}, mmap2 = {1}, rollup1 = {2}, rollup2 = {3}, differentIds = {4}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{true, true, true, true, false},
        new Object[]{true, false, true, false, false},
        new Object[]{false, true, true, false, false},
        new Object[]{false, false, false, false, false},
        new Object[]{false, false, true, true, false},
        new Object[]{false, false, false, true, true}
    );
  }

  public SegmentMetadataQueryTest(
      boolean mmap1,
      boolean mmap2,
      boolean rollup1,
      boolean rollup2,
      boolean differentIds
  )
  {
    final String id1 = differentIds ? "testSegment1" : "testSegment";
    final String id2 = differentIds ? "testSegment2" : "testSegment";
    this.runner1 = mmap1 ? makeMMappedQueryRunner(id1, rollup1, FACTORY) : makeIncrementalIndexQueryRunner(id1, rollup1, FACTORY);
    this.runner2 = mmap2 ? makeMMappedQueryRunner(id2, rollup2, FACTORY) : makeIncrementalIndexQueryRunner(id2, rollup2, FACTORY);
    this.mmap1 = mmap1;
    this.mmap2 = mmap2;
    this.rollup1 = rollup1;
    this.rollup2 = rollup2;
    this.differentIds = differentIds;
    testQuery = Druids.newSegmentMetadataQueryBuilder()
                      .dataSource("testing")
                      .intervals("2013/2014")
                      .toInclude(new ListColumnIncluderator(Arrays.asList("__time", "index", "placement")))
                      .analysisTypes(null)
                      .merge(true)
                      .build();

    expectedSegmentAnalysis1 = new SegmentAnalysis(
        id1,
        ImmutableList.of(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        ImmutableMap.of(
            "__time",
            new ColumnAnalysis(
                ValueType.LONG.toString(),
                false,
                12090,
                null,
                null,
                null,
                null
            ),
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                mmap1 ? 10881 : 10764,
                1,
                "preferred",
                "preferred",
                null
            ),
            "index",
            new ColumnAnalysis(
                ValueType.FLOAT.toString(),
                false,
                9672,
                null,
                null,
                null,
                null
            )
        ), mmap1 ? 93744 : 94517,
        1209,
        null,
        null,
        null,
        null
    );
    expectedSegmentAnalysis2 = new SegmentAnalysis(
        id2,
        ImmutableList.of(
            new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")
        ),
        ImmutableMap.of(
            "__time",
            new ColumnAnalysis(
                ValueType.LONG.toString(),
                false,
                12090,
                null,
                null,
                null,
                null
            ),
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                mmap2 ? 10881 : 0,
                1,
                null,
                null,
                null
            ),
            "index",
            new ColumnAnalysis(
                ValueType.FLOAT.toString(),
                false,
                9672,
                null,
                null,
                null,
                null
            )
        // null_column will be included only for incremental index, which makes a little bigger result than expected
        ), mmap2 ? 93744 : 94517,
        1209,
        null,
        null,
        null,
        null
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSegmentMetadataQuery()
  {
    List<SegmentAnalysis> results = Sequences.toList(
        runner1.run(testQuery, Maps.newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );

    Assert.assertEquals(Arrays.asList(expectedSegmentAnalysis1), results);
  }

  @Test
  public void testSegmentMetadataQueryWithRollupMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                0,
                null,
                null,
                null
            ),
            "placementish",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                true,
                0,
                0,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "placementish")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.ROLLUP)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithHasMultipleValuesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                1,
                null,
                null,
                null
            ),
            "placementish",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                true,
                0,
                9,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "placementish")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithComplexColumnMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                1,
                null,
                null,
                null
            ),
            "quality_uniques",
            new ColumnAnalysis(
                "hyperUnique",
                false,
                0,
                null,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement", "quality_uniques")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.CARDINALITY)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueType.STRING.toString(),
        false,
        (mmap1 ? 10881 : 10764) + (mmap2 ? 10881 : 10764),
        1,
        "preferred",
        "preferred",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("placement", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge2()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueType.STRING.toString(),
        false,
        (mmap1 ? 6882 : 6808) + (mmap2 ? 6882 : 6808),
        3,
        "spot",
        "upfront",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("market", analysis);
  }

  @Test
  public void testSegmentMetadataQueryWithDefaultAnalysisMerge3()
  {
    ColumnAnalysis analysis = new ColumnAnalysis(
        ValueType.STRING.toString(),
        false,
        (mmap1 ? 9765 : 9660) + (mmap2 ? 9765 : 9660),
        9,
        "automotive",
        "travel",
        null
    );
    testSegmentMetadataQueryWithDefaultAnalysisMerge("quality", analysis);
  }

  private void testSegmentMetadataQueryWithDefaultAnalysisMerge(
      String column,
      ColumnAnalysis analysis
  )
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        ImmutableList.of(expectedSegmentAnalysis1.getIntervals().get(0)),
        ImmutableMap.of(
            "__time",
            new ColumnAnalysis(
                ValueType.LONG.toString(),
                false,
                12090 * 2,
                null,
                null,
                null,
                null
            ),
            "index",
            new ColumnAnalysis(
                ValueType.FLOAT.toString(),
                false,
                9672 * 2,
                null,
                null,
                null,
                null
            ),
            column,
            analysis
        ),
        expectedSegmentAnalysis1.getSize() + expectedSegmentAnalysis2.getSize(),
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
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
        myRunner.run(query, Maps.newHashMap()),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithNoAnalysisTypesMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                0,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                  .analysisTypes()
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithAggregatorsMerge()
  {
    final Map<String, AggregatorFactory> expectedAggregators = Maps.newHashMap();
    for (AggregatorFactory agg : TestIndex.METRIC_AGGS) {
      expectedAggregators.put(agg.getName(), agg.getCombiningFactory());
    }
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                0,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.AGGREGATORS)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithTimestampSpecMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                0,
                null,
                null,
                null
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
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.TIMESTAMPSPEC)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
        "failed SegmentMetadata merging query"
    );
    exec.shutdownNow();
  }

  @Test
  public void testSegmentMetadataQueryWithQueryGranularityMerge()
  {
    SegmentAnalysis mergedSegmentAnalysis = new SegmentAnalysis(
        differentIds ? "merged" : "testSegment",
        null,
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                0,
                0,
                null,
                null,
                null
            )
        ),
        0,
        expectedSegmentAnalysis1.getNumRows() + expectedSegmentAnalysis2.getNumRows(),
        null,
        null,
        QueryGranularities.NONE,
        null
    );

    QueryToolChest toolChest = FACTORY.getToolchest();

    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner myRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                MoreExecutors.sameThreadExecutor(),
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(
                    toolChest.preMergeQueryDecoration(runner1),
                    toolChest.preMergeQueryDecoration(runner2)
                )
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(mergedSegmentAnalysis),
        myRunner.run(
            Druids.newSegmentMetadataQueryBuilder()
                  .dataSource("testing")
                  .intervals("2013/2014")
                  .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                  .analysisTypes(SegmentMetadataQuery.AnalysisType.QUERYGRANULARITY)
                  .merge(true)
                  .build(),
            Maps.newHashMap()
        ),
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
            Arrays.asList(
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
                MoreExecutors.sameThreadExecutor(),
                //Note: It is essential to have atleast 2 query runners merged to reproduce the regression bug described in
                //https://github.com/druid-io/druid/pull/1172
                //the bug surfaces only when ordering is used which happens only when you have 2 things to compare
                Lists.<QueryRunner<SegmentAnalysis>>newArrayList(singleSegmentQueryRunner, singleSegmentQueryRunner)
            )
        ),
        toolChest
    );

    TestHelper.assertExpectedObjects(
        ImmutableList.of(bySegmentResult, bySegmentResult),
        myRunner.run(
            testQuery.withOverriddenContext(ImmutableMap.<String, Object>of("bySegment", true)),
            Maps.newHashMap()
        ),
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
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));
    Assert.assertEquals(expectedAnalysisTypes, ((SegmentMetadataQuery) query).getAnalysisTypes());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));
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
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT), query.getIntervals().get(0));
    Assert.assertTrue(((SegmentMetadataQuery) query).isUsingDefaultInterval());

    // test serialize and deserialize
    Assert.assertEquals(query, MAPPER.readValue(MAPPER.writeValueAsString(query), Query.class));
  }

  @Test
  public void testDefaultIntervalAndFiltering() throws Exception
  {
    SegmentMetadataQuery testQuery = Druids.newSegmentMetadataQueryBuilder()
                                           .dataSource("testing")
                                           .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                           .merge(true)
                                           .build();

    Interval expectedInterval = new Interval(
        JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT
    );

    /* No interval specified, should use default interval */
    Assert.assertTrue(testQuery.isUsingDefaultInterval());
    Assert.assertEquals(testQuery.getIntervals().get(0), expectedInterval);
    Assert.assertEquals(testQuery.getIntervals().size(), 1);

    List<LogicalSegment> testSegments = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2012-01-01/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2012-01-01T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-05-20/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-02-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
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
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
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
            return new Interval("2013-05-20/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-01-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2014-02-05/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-19T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2015-01-20T02/PT1H");
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
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("foo")))
                                                .build();

    SegmentMetadataQuery twoColumnQuery = Druids.newSegmentMetadataQueryBuilder()
                                                .dataSource("testing")
                                                .toInclude(new ListColumnIncluderator(Arrays.asList("fo", "o")))
                                                .build();

    final byte[] oneColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(null).getCacheStrategy(oneColumnQuery)
                                                                                      .computeCacheKey(oneColumnQuery);

    final byte[] twoColumnQueryCacheKey = new SegmentMetadataQueryQueryToolChest(null).getCacheStrategy(twoColumnQuery)
                                                                                      .computeCacheKey(twoColumnQuery);

    Assert.assertFalse(Arrays.equals(oneColumnQueryCacheKey, twoColumnQueryCacheKey));
  }
}
