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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.error.ColumnNameRestrictedFault;
import org.apache.druid.msq.indexing.error.RowTooLargeFault;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


@RunWith(Parameterized.class)
public class MSQInsertTest extends MSQTestBase
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Parameterized.Parameters(name = "{index}:with context {0}")
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {SEQUENTIAL_MERGE, SEQUENTIAL_MERGE_MSQ_CONTEXT}
    };
    return Arrays.asList(data);
  }

  @Parameterized.Parameter(0)
  public String contextName;

  @Parameterized.Parameter(1)
  public Map<String, Object> context;


  @Test
  public void testInsertOnFoo1()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();
    long[] expectedArray = createExpectedFrameArray(expectedCounterRows, 1);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(context)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(Arrays.stream(expectedArray).sum()),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testInsertOnExternalDataSource() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(1),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithGroupByLimitWithoutClusterBy()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 limit 10 PARTITIONED by All")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(context)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         2, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         3, 0, "input0"
                     )

                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithGroupByLimitWithClusterBy()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2  limit 10 PARTITIONED by All clustered by 2,3")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(context)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         2, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         3, 0, "input0"
                     )
                     .verifyResults();

  }
  @Test
  public void testInsertOnFoo1WithTimeFunction()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithTimeFunctionWithSequential()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();
    long[] expectedArray = createExpectedFrameArray(expectedCounterRows, 1);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put(
                                                  MultiStageQueryContext.CTX_CLUSTER_STATISTICS_MERGE_MODE,
                                                  ClusterStatisticsMergeMode.SEQUENTIAL.toString()
                                              )
                                              .build();

    testIngestQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(MSQInsertTest.this.context)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(Arrays.stream(expectedArray).sum()),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithMultiValueDim()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3 FROM foo WHERE dim3 IS NOT NULL PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRows())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithLimitWithoutClusterBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3 FROM foo WHERE dim3 IS NOT NULL limit 10 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRows())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithLimitWithClusterBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3 FROM foo WHERE dim3 IS NOT NULL limit 10 PARTITIONED BY ALL TIME clustered by dim3")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRows())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithMultiValueDimGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3 FROM foo WHERE dim3 IS NOT NULL GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRowsGroupBy())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithMultiValueMeasureGroupBy()
  {
    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT count(dim3) FROM foo WHERE dim3 IS NOT NULL GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(context)
                     .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "Aggregate expression is illegal in GROUP BY clause"))
                     ))
                     .verifyPlanningErrors();
  }


  @Test
  public void testInsertOnFoo1WithMultiValueToArrayGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT MV_TO_ARRAY(dim3) AS dim3 FROM foo GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRowsToArray())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithMultiValueDimGroupByWithoutGroupByEnable()
  {
    Map<String, Object> localContext = ImmutableMap.<String, Object>builder()
                                                   .putAll(context)
                                                   .put("groupByEnableMultiValueUnnesting", false)
                                                   .build();


    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3, count(*) AS cnt1 FROM foo GROUP BY dim3 PARTITIONED BY ALL TIME")
                     .setQueryContext(localContext)
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "Column [dim3] is a multi-value string. Please wrap the column using MV_TO_ARRAY() to proceed further.")
                         )
                     ))
                     .verifyExecutionError();
  }

  @Test
  public void testRollUpOnFoo1UpOnFoo1()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();
    long[] expectedArray = createExpectedFrameArray(expectedCounterRows, 1);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context)
                                                                                .putAll(ROLLUP_CONTEXT_PARAMS)
                                                                                .build())
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(Arrays.stream(expectedArray).sum()),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testRollUpOnFoo1WithTimeFunction()
  {
    List<Object[]> expectedRows = expectedFooRows();
    int expectedCounterRows = expectedRows.size();
    long[] expectedArray = createExpectedFrameArray(expectedCounterRows, 1);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context).putAll(
                         ROLLUP_CONTEXT_PARAMS).build())
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedRows)
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedCounterRows).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(expectedArray).frames(expectedArray),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(Arrays.stream(expectedArray).sum()),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testRollUpOnFoo1WithTimeFunctionComplexCol()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", new ColumnType(ValueType.COMPLEX, "hyperUnique", null))
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(distinct m1) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context).putAll(
                         ROLLUP_CONTEXT_PARAMS).build())
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new HyperUniquesAggregatorFactory("cnt", "cnt", false, true))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRowsWithAggregatedComplexColumn())
                     .verifyResults();

  }


  @Test
  public void testRollUpOnFoo1ComplexCol()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", new ColumnType(ValueType.COMPLEX, "hyperUnique", null))
                                            .build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time , dim1 , count(distinct m1) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context).putAll(
                         ROLLUP_CONTEXT_PARAMS).build())
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new HyperUniquesAggregatorFactory("cnt", "cnt", false, true))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRowsWithAggregatedComplexColumn())
                     .verifyResults();

  }

  @Test
  public void testRollUpOnExternalDataSource() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context).putAll(
                         ROLLUP_CONTEXT_PARAMS).build())
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1).frames(1),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(1),
                         2, 0
                     )
                     .verifyResults();
  }

  @Test()
  public void testRollUpOnExternalDataSourceWithCompositeKey() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("namespace", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + " namespace , count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1,2  PARTITIONED by day ")
                     .setQueryContext(new ImmutableMap.Builder<String, Object>().putAll(context).putAll(
                         ROLLUP_CONTEXT_PARAMS).build())
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{1466985600000L, "Benutzer Diskussion", 2L},
                         new Object[]{1466985600000L, "File", 1L},
                         new Object[]{1466985600000L, "Kategoria", 1L},
                         new Object[]{1466985600000L, "Main", 14L},
                         new Object[]{1466985600000L, "Wikipedia", 1L},
                         new Object[]{1466985600000L, "Википедия", 1L}
                     ))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(6).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(6).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(6).frames(1),
                         1, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(6).frames(1),
                         2, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(6),
                         2, 0
                     )
                     .verifyResults();

  }

  @Test
  public void testInsertWrongTypeTimestamp()
  {
    final RowSignature rowSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();

    testIngestQuery()
        .setSql(
            "INSERT INTO foo1\n"
            + "SELECT dim1 AS __time, cnt\n"
            + "FROM foo\n"
            + "PARTITIONED BY DAY\n"
            + "CLUSTERED BY dim1")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(SqlPlanningException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Field \"__time\" must be of type TIMESTAMP"))
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testIncorrectInsertQuery()
  {
    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 clustered by dim1")
                     .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                             "CLUSTERED BY found before PARTITIONED BY. In Druid, the CLUSTERED BY clause must follow the PARTITIONED BY clause"))
                     ))
                     .verifyPlanningErrors();
  }


  @Test
  public void testInsertRestrictedColumns()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("namespace", ColumnType.STRING)
                                            .add("__bucket", ColumnType.LONG)
                                            .build();


    testIngestQuery()
        .setSql(" insert into foo1 SELECT\n"
                + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                + " namespace, __bucket\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [\"ignored\"],\"type\":\"local\"}',\n"
                + "    '{\"type\": \"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}, {\"name\": \"__bucket\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + ") PARTITIONED by day")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedMSQFault(new ColumnNameRestrictedFault("__bucket"))
        .verifyResults();
  }

  @Test
  public void testInsertQueryWithInvalidSubtaskCount()
  {
    Map<String, Object> localContext = ImmutableMap.<String, Object>builder()
                                                   .putAll(context)
                                                   .put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 1)
                                                   .build();
    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setQueryContext(localContext)
                     .setExpectedExecutionErrorMatcher(
                         ThrowableMessageMatcher.hasMessage(
                             CoreMatchers.startsWith(
                                 MultiStageQueryContext.CTX_MAX_NUM_TASKS
                                 + " cannot be less than 2 since at least 1 controller and 1 worker is necessary."
                             )
                         )
                     )
                     .verifyExecutionError();
  }

  @Test
  public void testInsertWithTooLargeRowShouldThrowException() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    Mockito.doReturn(500).when(workerMemoryParameters).getLargeFrameSize();

    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo")
                     .setQueryContext(context)
                     .setExpectedMSQFault(new RowTooLargeFault(500))
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "Row too large to add to frame"))
                     ))
                     .verifyExecutionError();
  }

  @Test
  public void testInsertLimitWithPeriodGranularityThrowsException()
  {
    testIngestQuery().setSql(" INSERT INTO foo "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "LIMIT 50 "
                             + "PARTITIONED BY MONTH")
                     .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "INSERT and REPLACE queries cannot have a LIMIT unless PARTITIONED BY is \"ALL\""))
                     ))
                     .setQueryContext(context)
                     .verifyPlanningErrors();
  }

  @Test
  public void testInsertOffsetThrowsException()
  {
    testIngestQuery().setSql(" INSERT INTO foo "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "LIMIT 50 "
                             + "OFFSET 10"
                             + "PARTITIONED BY ALL TIME")
                     .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(SqlPlanningException.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "INSERT and REPLACE queries cannot have an OFFSET"))
                     ))
                     .setQueryContext(context)
                     .verifyPlanningErrors();
  }

  @Nonnull
  private List<Object[]> expectedFooRows()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    if (!useDefault) {
      expectedRows.add(new Object[]{946684800000L, "", 1L});
    }
    expectedRows.addAll(ImmutableList.of(
        new Object[]{946771200000L, "10.1", 1L},
        new Object[]{946857600000L, "2", 1L},
        new Object[]{978307200000L, "1", 1L},
        new Object[]{978393600000L, "def", 1L},
        new Object[]{978480000000L, "abc", 1L}
    ));
    return expectedRows;
  }

  @Nonnull
  private List<Object[]> expectedFooRowsWithAggregatedComplexColumn()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    HyperLogLogCollector hyperLogLogCollector = HyperLogLogCollector.makeLatestCollector();
    hyperLogLogCollector.add(fn.hashInt(1).asBytes());
    if (!useDefault) {
      expectedRows.add(new Object[]{946684800000L, "", hyperLogLogCollector.estimateCardinalityRound()});
    }
    expectedRows.addAll(ImmutableList.of(
        new Object[]{946771200000L, "10.1", hyperLogLogCollector.estimateCardinalityRound()},
        new Object[]{946857600000L, "2", hyperLogLogCollector.estimateCardinalityRound()},
        new Object[]{978307200000L, "1", hyperLogLogCollector.estimateCardinalityRound()},
        new Object[]{978393600000L, "def", hyperLogLogCollector.estimateCardinalityRound()},
        new Object[]{978480000000L, "abc", hyperLogLogCollector.estimateCardinalityRound()}
    ));
    return expectedRows;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRows()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    if (!useDefault) {
      expectedRows.add(new Object[]{0L, ""});
    }

    expectedRows.addAll(
        ImmutableList.of(
            new Object[]{0L, ImmutableList.of("a", "b")},
            new Object[]{0L, ImmutableList.of("b", "c")},
            new Object[]{0L, "d"}
        ));
    return expectedRows;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsToArray()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    expectedRows.add(new Object[]{0L, null});
    if (!useDefault) {
      expectedRows.add(new Object[]{0L, ""});
    }

    expectedRows.addAll(ImmutableList.of(
        new Object[]{0L, ImmutableList.of("a", "b")},
        new Object[]{0L, ImmutableList.of("b", "c")},
        new Object[]{0L, "d"}
    ));
    return expectedRows;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroupBy()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    if (!useDefault) {
      expectedRows.add(new Object[]{0L, ""});
    }
    expectedRows.addAll(ImmutableList.of(
        new Object[]{0L, "a"},
        new Object[]{0L, "b"},
        new Object[]{0L, "c"},
        new Object[]{0L, "d"}
    ));
    return expectedRows;
  }

  @Nonnull
  private Set<SegmentId> expectedFooSegments()
  {
    Set<SegmentId> expectedSegments = new TreeSet<>();

    if (!useDefault) {
      expectedSegments.add(SegmentId.of("foo1", Intervals.of("2000-01-01T/P1D"), "test", 0));
    }
    expectedSegments.addAll(
        ImmutableSet.of(
            SegmentId.of("foo1", Intervals.of("2000-01-02T/P1D"), "test", 0),
            SegmentId.of("foo1", Intervals.of("2000-01-03T/P1D"), "test", 0),
            SegmentId.of("foo1", Intervals.of("2001-01-01T/P1D"), "test", 0),
            SegmentId.of("foo1", Intervals.of("2001-01-02T/P1D"), "test", 0),
            SegmentId.of("foo1", Intervals.of("2001-01-03T/P1D"), "test", 0)
        ));

    return expectedSegments;
  }
}
