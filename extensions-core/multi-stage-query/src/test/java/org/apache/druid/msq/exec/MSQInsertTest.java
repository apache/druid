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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.error.ColumnNameRestrictedFault;
import org.apache.druid.msq.indexing.error.RowTooLargeFault;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


@RunWith(Parameterized.class)
public class MSQInsertTest extends MSQTestBase
{

  private static final String WITH_APPEND_LOCK = "WITH_APPEND_LOCK";
  private static final Map<String, Object> QUERY_CONTEXT_WITH_APPEND_LOCK =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(
                      Tasks.TASK_LOCK_TYPE,
                      TaskLockType.APPEND.name().toLowerCase(Locale.ENGLISH)
                  )
                  .build();
  private final HashFunction fn = Hashing.murmur3_128();

  @Parameterized.Parameters(name = "{index}:with context {0}")
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT},
        {WITH_APPEND_LOCK, QUERY_CONTEXT_WITH_APPEND_LOCK}
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
  public void testInsertWithExistingTimeColumn() throws IOException
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[] {1678897351000L, "A"},
        new Object[] {1679588551000L, "B"},
        new Object[] {1682266951000L, "C"}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("flags", ColumnType.STRING)
                                            .build();

    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this,
                                                                    "/dataset-with-time-column.json"
    );
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    testIngestQuery().setSql(" INSERT INTO foo1 SELECT\n"
                             + "  __time,\n"
                             + "  flags\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"__time\", \"type\": \"long\"}, {\"name\": \"flags\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ")\n"
                             + "WHERE __time > TIMESTAMP '1999-01-01 00:00:00'\n"
                             + "PARTITIONED BY day")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }

  @Test
  public void testInsertWithUnnestInline()
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{1692226800000L, 1L},
        new Object[]{1692226800000L, 2L},
        new Object[]{1692226800000L, 3L}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.LONG)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 select TIME_PARSE('2023-08-16T23:00') as __time, d from UNNEST(ARRAY[1,2,3]) as unnested(d) PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }

  @Test
  public void testInsertWithUnnest()
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{946684800000L, "a"},
        new Object[]{946684800000L, "b"},
        new Object[]{946771200000L, "b"},
        new Object[]{946771200000L, "c"},
        new Object[]{946857600000L, "d"},
        new Object[]{978307200000L, NullHandling.sqlCompatible() ? "" : null},
        new Object[]{978393600000L, null},
        new Object[]{978480000000L, null}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.STRING)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 select __time, d from foo,UNNEST(MV_TO_ARRAY(dim3)) as unnested(d) PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }

  @Test
  public void testInsertWithUnnestWithVirtualColumns()
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{946684800000L, 1.0f},
        new Object[]{946684800000L, 1.0f},
        new Object[]{946771200000L, 2.0f},
        new Object[]{946771200000L, 2.0f},
        new Object[]{946857600000L, 3.0f},
        new Object[]{946857600000L, 3.0f},
        new Object[]{978307200000L, 4.0f},
        new Object[]{978307200000L, 4.0f},
        new Object[]{978393600000L, 5.0f},
        new Object[]{978393600000L, 5.0f},
        new Object[]{978480000000L, 6.0f},
        new Object[]{978480000000L, 6.0f}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.FLOAT)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 select __time, d from foo,UNNEST(ARRAY[m1,m2]) as unnested(d) PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
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
  public void testInsertOnFoo1WithTwoCountAggregatorsWithRollupContext()
  {
    final List<Object[]> expectedRows = expectedFooRows();

    // Add 1L to each expected row, since we have two count aggregators.
    for (int i = 0; i < expectedRows.size(); i++) {
      final Object[] expectedRow = expectedRows.get(i);
      final Object[] newExpectedRow = new Object[expectedRow.length + 1];
      System.arraycopy(expectedRow, 0, newExpectedRow, 0, expectedRow.length);
      newExpectedRow[expectedRow.length] = 1L;
      expectedRows.set(i, newExpectedRow);
    }

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt2", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "insert into foo1\n"
                         + "select  __time, dim1 , count(*) as cnt, count(*) as cnt2\n"
                         + "from foo\n"
                         + "where dim1 is not null\n"
                         + "group by 1, 2\n"
                         + "PARTITIONED by All")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(QueryContexts.override(context, ROLLUP_CONTEXT_PARAMS))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedRows)
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt2", "cnt2"))
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
  public void testInsertOnFoo1WithTimeAggregator()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + "SELECT MILLIS_TO_TIMESTAMP((SUM(CAST(\"m1\" AS BIGINT)))) AS __time "
                         + "FROM foo "
                         + "PARTITIONED BY DAY"
                     )
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(
                         ImmutableSet.of(
                             SegmentId.of("foo1", Intervals.of("1970-01-01/P1D"), "test", 0)
                         )
                     )
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{21L}
                         )
                     )
                     .verifyResults();

  }

  @Test
  public void testInsertOnFoo1WithTimeAggregatorAndMultipleWorkers()
  {
    Map<String, Object> localContext = new HashMap<>(context);
    localContext.put(MultiStageQueryContext.CTX_TASK_ASSIGNMENT_STRATEGY, WorkerAssignmentStrategy.MAX.name());
    localContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 4);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + "SELECT MILLIS_TO_TIMESTAMP((SUM(CAST(\"m1\" AS BIGINT)))) AS __time "
                         + "FROM foo "
                         + "PARTITIONED BY DAY"
                     )
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(localContext)
                     .setExpectedSegment(
                         ImmutableSet.of(
                             SegmentId.of("foo1", Intervals.of("1970-01-01/P1D"), "test", 0)
                         )
                     )
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{21L}
                         )
                     )
                     .verifyResults();
  }


  @Test
  public void testInsertOnFoo1WithTimePostAggregator()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + "SELECT DATE_TRUNC('DAY', TIMESTAMP '2000-01-01' - INTERVAL '1'DAY) AS __time, SUM(m1) AS sum_m1 "
                         + "FROM foo "
                         + "PARTITIONED BY DAY"
                     )
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(
                         ImmutableSet.of(
                             SegmentId.of("foo1", Intervals.of("1999-12-31T/P1D"), "test", 0)
                         )
                     )
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946598400000L, 21.0}
                         )
                     )
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
  public void testInsertOnFoo1MultiValueDimWithLimitWithoutClusterBy()
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
  public void testInsertOnFoo1MultiValueDimWithLimitWithClusterBy()
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
                     .setExpectedValidationErrorMatcher(
                         invalidSqlContains("Aggregate expression is illegal in GROUP BY clause")
                     )
                     .verifyPlanningErrors();
  }



  @Test
  public void testInsertOnFoo1WithAutoTypeArrayGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING_ARRAY).build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_USE_AUTO_SCHEMAS, true);

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT MV_TO_ARRAY(dim3) as dim3 FROM foo GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(adjustedContext)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(
                         NullHandling.replaceWithDefault() ?
                         ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, new Object[]{"a", "b"}},
                             new Object[]{0L, new Object[]{"b", "c"}},
                             new Object[]{0L, new Object[]{"d"}}
                         ) : ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, new Object[]{"a", "b"}},
                             new Object[]{0L, new Object[]{""}},
                             new Object[]{0L, new Object[]{"b", "c"}},
                             new Object[]{0L, new Object[]{"d"}}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithArrayIngestModeArrayGroupByInsertAsArray()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING_ARRAY).build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT MV_TO_ARRAY(dim3) as dim3 FROM foo GROUP BY 1 PARTITIONED BY ALL TIME"
                     )
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(adjustedContext)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(
                         NullHandling.replaceWithDefault() ?
                         ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, new Object[]{"a", "b"}},
                             new Object[]{0L, new Object[]{"b", "c"}},
                             new Object[]{0L, new Object[]{"d"}}
                         ) : ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, new Object[]{"a", "b"}},
                             new Object[]{0L, new Object[]{""}},
                             new Object[]{0L, new Object[]{"b", "c"}},
                             new Object[]{0L, new Object[]{"d"}}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithArrayIngestModeArrayGroupByInsertAsMvd()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING).build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT ARRAY_TO_MV(MV_TO_ARRAY(dim3)) as dim3 FROM foo GROUP BY MV_TO_ARRAY(dim3) PARTITIONED BY ALL TIME"
                     )
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(adjustedContext)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(
                         NullHandling.replaceWithDefault() ?
                         ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, Arrays.asList("a", "b")},
                             new Object[]{0L, Arrays.asList("b", "c")},
                             new Object[]{0L, "d"}
                         ) : ImmutableList.of(
                             new Object[]{0L, null},
                             new Object[]{0L, ""},
                             new Object[]{0L, Arrays.asList("a", "b")},
                             new Object[]{0L, Arrays.asList("b", "c")},
                             new Object[]{0L, "d"}
                         )
                     )
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
  public void testInsertWithClusteredByDescendingThrowsException()
  {
    // Add a DESC clustered by column, which should not be allowed
    testIngestQuery().setSql("INSERT INTO foo1 "
                             + "SELECT __time, dim1 , count(*) as cnt "
                             + "FROM foo "
                             + "GROUP BY 1, 2"
                             + "PARTITIONED BY DAY "
                             + "CLUSTERED BY dim1 DESC"
                     )
                     .setExpectedValidationErrorMatcher(
                         invalidSqlIs("Invalid CLUSTERED BY clause [`dim1` DESC]: cannot sort in descending order.")
                     )
                     .verifyPlanningErrors();
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
        .setExpectedValidationErrorMatcher(
            new DruidExceptionMatcher(
                DruidException.Persona.USER,
                DruidException.Category.INVALID_INPUT,
                "invalidInput"
            ).expectMessageIs("Field [__time] was the wrong type [VARCHAR], expected TIMESTAMP")
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testIncorrectInsertQuery()
  {
    testIngestQuery()
        .setSql(
            "insert into foo1 select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 clustered by dim1"
        )
        .setExpectedValidationErrorMatcher(invalidSqlContains(
            "CLUSTERED BY found before PARTITIONED BY, CLUSTERED BY must come after the PARTITIONED BY clause"
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
  public void testInsertDuplicateColumnNames()
  {
    testIngestQuery()
        .setSql(" insert into foo1 SELECT\n"
                + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                + " namespace,\n"
                + " \"user\" AS namespace\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [\"ignored\"],\"type\":\"local\"}',\n"
                + "    '{\"type\": \"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"namespace\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}, {\"name\": \"__bucket\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + ") PARTITIONED by day")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            invalidSqlIs("Duplicate field in SELECT: [namespace]")
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testInsertQueryWithInvalidSubtaskCount()
  {
    Map<String, Object> localContext = new HashMap<>(context);
    localContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 1);

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setQueryContext(localContext)
                     .setExpectedExecutionErrorMatcher(
                         new DruidExceptionMatcher(
                             DruidException.Persona.USER,
                             DruidException.Category.INVALID_INPUT,
                             "invalidInput"
                         ).expectMessageIs(
                             "MSQ context maxNumTasks [1] cannot be less than 2, since at least 1 controller "
                             + "and 1 worker is necessary"
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
                     .setExpectedValidationErrorMatcher(
                         invalidSqlContains(
                             "INSERT and REPLACE queries cannot have a LIMIT unless PARTITIONED BY is \"ALL\""
                         )
                     )
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
                     .setExpectedValidationErrorMatcher(
                         invalidSqlContains("INSERT and REPLACE queries cannot have an OFFSET")
                     )
                     .setQueryContext(context)
                     .verifyPlanningErrors();
  }

  @Test
  public void testCorrectNumberOfWorkersUsedAutoModeWithoutBytesLimit() throws IOException
  {
    Map<String, Object> localContext = new HashMap<>(context);
    localContext.put(MultiStageQueryContext.CTX_TASK_ASSIGNMENT_STRATEGY, WorkerAssignmentStrategy.AUTO.name());
    localContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 4);

    final File toRead1 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-1.json");
    final String toReadFileNameAsJson1 = queryFramework().queryJsonMapper().writeValueAsString(toRead1.getAbsolutePath());

    final File toRead2 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-2.json");
    final String toReadFileNameAsJson2 = queryFramework().queryJsonMapper().writeValueAsString(toRead2.getAbsolutePath());

    final File toRead3 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-3.json");
    final String toReadFileNameAsJson3 = queryFramework().queryJsonMapper().writeValueAsString(toRead3.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "insert into foo1 select "
                         + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                         + "  count(*) as cnt\n"
                         + "FROM TABLE(\n"
                         + "  EXTERN(\n"
                         + "    '{ \"files\": [" + toReadFileNameAsJson1 + "," + toReadFileNameAsJson2 + "," + toReadFileNameAsJson3 + "],\"type\":\"local\"}',\n"
                         + "    '{\"type\": \"json\"}',\n"
                         + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                         + "  )\n"
                         + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(localContext)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedWorkerCount(
                         ImmutableMap.of(
                             0, 1
                         ))
                     .verifyResults();

  }

  @Test
  public void testCorrectNumberOfWorkersUsedAutoModeWithBytesLimit() throws IOException
  {
    Map<String, Object> localContext = new HashMap<>(context);
    localContext.put(MultiStageQueryContext.CTX_TASK_ASSIGNMENT_STRATEGY, WorkerAssignmentStrategy.AUTO.name());
    localContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 4);
    localContext.put(MultiStageQueryContext.CTX_MAX_INPUT_BYTES_PER_WORKER, 10);

    final File toRead1 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-1.json");
    final String toReadFileNameAsJson1 = queryFramework().queryJsonMapper().writeValueAsString(toRead1.getAbsolutePath());

    final File toRead2 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-2.json");
    final String toReadFileNameAsJson2 = queryFramework().queryJsonMapper().writeValueAsString(toRead2.getAbsolutePath());

    final File toRead3 = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/multipleFiles/wikipedia-sampled-3.json");
    final String toReadFileNameAsJson3 = queryFramework().queryJsonMapper().writeValueAsString(toRead3.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "insert into foo1 select "
                         + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                         + "  count(*) as cnt\n"
                         + "FROM TABLE(\n"
                         + "  EXTERN(\n"
                         + "    '{ \"files\": [" + toReadFileNameAsJson1 + "," + toReadFileNameAsJson2 + "," + toReadFileNameAsJson3 + "],\"type\":\"local\"}',\n"
                         + "    '{\"type\": \"json\"}',\n"
                         + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                         + "  )\n"
                         + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(localContext)
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedWorkerCount(
                         ImmutableMap.of(
                             0, 3
                         ))
                     .verifyResults();
  }

  @Test
  public void testEmptyInsertQuery()
  {
    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY day"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

  @Test
  public void testEmptyInsertQueryWithAllGranularity()
  {
    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + " SELECT  __time, dim1 , COUNT(*) AS cnt"
                         + " FROM foo WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

  @Test
  public void testEmptyInsertLimitQuery()
  {
    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "INSERT INTO foo1 "
                         + " SELECT  __time, dim1, COUNT(*) AS cnt"
                         + " FROM foo WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " LIMIT 100"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1"
                     )
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

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
