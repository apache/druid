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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@RunWith(Parameterized.class)
public class MSQReplaceTest extends MSQTestBase
{

  private static final String WITH_REPLACE_LOCK = "WITH_REPLACE_LOCK";
  private static final Map<String, Object> QUERY_CONTEXT_WITH_REPLACE_LOCK =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(
                      Tasks.TASK_LOCK_TYPE,
                      StringUtils.toLowerCase(TaskLockType.REPLACE.name())
                  )
                  .build();

  @Parameterized.Parameters(name = "{index}:with context {0}")
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT},
        {WITH_REPLACE_LOCK, QUERY_CONTEXT_WITH_REPLACE_LOCK}
    };
    return Arrays.asList(data);
  }

  @Parameterized.Parameter(0)
  public String contextName;

  @Parameterized.Parameter(1)
  public Map<String, Object> context;

  @Test
  public void testReplaceOnFooWithAll()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    DataSegment existingDataSegment1 = DataSegment.builder()
                                                  .interval(Intervals.of("2001-01-01T/2001-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doCallRealMethod()
           .doReturn(ImmutableSet.of(existingDataSegment0, existingDataSegment1))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.ETERNITY))
           ));

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(
                         ImmutableSet.of(
                             SegmentId.of("foo", Intervals.of("2000-01-01T/P1D"), "test", 0),
                             SegmentId.of("foo", Intervals.of("2000-01-02T/P1D"), "test", 0),
                             SegmentId.of("foo", Intervals.of("2000-01-03T/P1D"), "test", 0),
                             SegmentId.of("foo", Intervals.of("2001-01-01T/P1D"), "test", 0),
                             SegmentId.of("foo", Intervals.of("2001-01-02T/P1D"), "test", 0),
                             SegmentId.of("foo", Intervals.of("2001-01-03T/P1D"), "test", 0)
                         )
                     )
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f},
                             new Object[]{946857600000L, 3.0f},
                             new Object[]{978307200000L, 4.0f},
                             new Object[]{978393600000L, 5.0f},
                             new Object[]{978480000000L, 6.0f}
                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1, 1, 1, 1, 1, 1).frames(1, 1, 1, 1, 1, 1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1, 1, 1, 1, 1, 1).frames(1, 1, 1, 1, 1, 1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(6),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFooWithWhere()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(
                         " REPLACE INTO foo OVERWRITE WHERE __time >= TIMESTAMP '2000-01-02' AND __time < TIMESTAMP '2000-01-03' "
                         + "SELECT __time, m1 "
                         + "FROM foo "
                         + "WHERE __time >= TIMESTAMP '2000-01-02' AND __time < TIMESTAMP '2000-01-03' "
                         + "PARTITIONED by DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of(
                         "2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.000Z")))
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-02T/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{946771200000L, 2.0f}))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
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
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(1),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFoo1WithAllExtern() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG).build();

    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE ALL SELECT "
                             + "  floor(TIME_PARSE(\"timestamp\") to hour) AS __time, "
                             + "  count(*) AS cnt "
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") GROUP BY 1  PARTITIONED BY HOUR ")
                     .setExpectedDataSource("foo1")
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(
                                             SegmentId.of(
                                                 "foo1",
                                                 Intervals.of("2016-06-27T00:00:00.000Z/2016-06-27T01:00:00.000Z"),
                                                 "test",
                                                 0
                                             ),
                                             SegmentId.of(
                                                 "foo1",
                                                 Intervals.of("2016-06-27T01:00:00.000Z/2016-06-27T02:00:00.000Z"),
                                                 "test",
                                                 0
                                             ),
                                             SegmentId.of(
                                                 "foo1",
                                                 Intervals.of("2016-06-27T02:00:00.000Z/2016-06-27T03:00:00.000Z"),
                                                 "test",
                                                 0
                                             )
                                         )
                     )
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{1466985600000L, 10L},
                             new Object[]{1466989200000L, 4L},
                             new Object[]{1466992800000L, 6L}
                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(3).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(3).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(1, 1, 1).frames(1, 1, 1),
                         1, 0, "shuffle"
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFoo1WithWhereExtern() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("user", ColumnType.STRING).build();

    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    testIngestQuery().setSql(
                         " REPLACE INTO foo1 OVERWRITE WHERE __time >= TIMESTAMP '2016-06-27 01:00:00.00' AND __time < TIMESTAMP '2016-06-27 02:00:00.00' "
                         + " SELECT "
                         + "  floor(TIME_PARSE(\"timestamp\") to hour) AS __time, "
                         + "  user "
                         + "FROM TABLE(\n"
                         + "  EXTERN(\n"
                         + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                         + "    '{\"type\": \"json\"}',\n"
                         + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                         + "  )\n"
                         + ") "
                         + "where \"timestamp\" >= TIMESTAMP '2016-06-27 01:00:00.00' AND \"timestamp\" < TIMESTAMP '2016-06-27 02:00:00.00' "
                         + "PARTITIONED BY HOUR ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of(
                         "2016-06-27T01:00:00.000Z/2016-06-27T02:00:00.000Z")))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27T01:00:00.000Z/2016-06-27T02:00:00.000Z"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{1466989200000L, "2001:DA8:207:E132:94DC:BA03:DFDF:8F9F"},
                             new Object[]{1466989200000L, "Ftihikam"},
                             new Object[]{1466989200000L, "Guly600"},
                             new Object[]{1466989200000L, "Kolega2357"}
                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(4).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(4).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(4),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceIncorrectSyntax()
  {
    testIngestQuery()
        .setSql("REPLACE INTO foo1 OVERWRITE SELECT * FROM foo PARTITIONED BY ALL TIME")
        .setExpectedDataSource("foo1")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(invalidSqlContains(
            "Missing time chunk information in OVERWRITE clause for REPLACE. "
            + "Use OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table."
        ))
        .verifyPlanningErrors();
  }

  @Test
  public void testReplaceSegmentEntireTable()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY ALL TIME ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f},
                             new Object[]{946857600000L, 3.0f},
                             new Object[]{978307200000L, 4.0f},
                             new Object[]{978393600000L, 5.0f},
                             new Object[]{978480000000L, 6.0f}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
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
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(6),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceSegmentsRepartitionTable()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();
    DataSegment existingDataSegment1 = DataSegment.builder()
                                                  .interval(Intervals.of("2001-01-01T/2001-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doCallRealMethod()
           .doReturn(ImmutableSet.of(existingDataSegment0, existingDataSegment1))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.ETERNITY))
           ));


    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f},
                             new Object[]{946857600000L, 3.0f},
                             new Object[]{978307200000L, 4.0f},
                             new Object[]{978393600000L, 5.0f},
                             new Object[]{978480000000L, 6.0f}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(
                                             SegmentId.of("foo", Intervals.of("2000-01-01T/P1M"), "test", 0),
                                             SegmentId.of("foo", Intervals.of("2001-01-01T/P1M"), "test", 0)
                                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(3, 3).frames(1, 1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(3, 3).frames(1, 1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(6),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceWithWhereClause()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment0))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.of("2000-01-01/2000-03-01")))
           ));

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-03-01' "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Collections.singletonList(Intervals.of("2000-01-01T/2000-03-01T")))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(2),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceWhereClauseLargerThanData()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    DataSegment existingDataSegment1 = DataSegment.builder()
                                                  .interval(Intervals.of("2001-01-01T/2001-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment0, existingDataSegment1))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.of("2000-01-01/2002-01-01")))
           ));


    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2002-01-01' "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Collections.singletonList(Intervals.of("2000-01-01T/2002-01-01T")))
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.of("2001-01-01T/2001-02-01T")))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f}
                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(2),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceLimitWithPeriodGranularityThrowsException()
  {
    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "LIMIT 50"
                             + "PARTITIONED BY MONTH")
                     .setQueryContext(context)
                     .setExpectedValidationErrorMatcher(invalidSqlContains(
                             "INSERT and REPLACE queries cannot have a LIMIT unless PARTITIONED BY is \"ALL\""
                     ))
                     .verifyPlanningErrors();
  }

  @Test
  public void testReplaceOffsetThrowsException()
  {
    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "LIMIT 50 "
                             + "OFFSET 10"
                             + "PARTITIONED BY ALL TIME")
                     .setExpectedValidationErrorMatcher(invalidSqlContains(
                             "INSERT and REPLACE queries cannot have an OFFSET"
                     ))
                     .setQueryContext(context)
                     .verifyPlanningErrors();
  }

  @Test
  public void testReplaceTimeChunks()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    final DataSegment existingDataSegment = DataSegment.builder()
                                                       .dataSource("foo")
                                                       .interval(Intervals.of("2000-01-01/2000-01-04"))
                                                       .version(MSQTestTaskActionClient.VERSION)
                                                       .size(1)
                                                       .build();
    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.of("2000-01-01/2000-03-01")))
           ));

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-03-01'"
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Collections.singletonList(Intervals.of("2000-01-01T/2000-03-01T")))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceTimeChunksLargerThanData()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();
    DataSegment existingDataSegment1 = DataSegment.builder()
                                                  .interval(Intervals.of("2001-01-01T/2001-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment0, existingDataSegment1))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.of("2000/2002")))
           ));

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2002-01-01'"
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Collections.singletonList(Intervals.of("2000-01-01T/2002-01-01T")))
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.of("2001-01-01T/2001-02-01T")))
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceAllOverEternitySegment()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    // Create a datasegment which lies partially outside the generated segment
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.ETERNITY)
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-03' "
                             + "PARTITIONED BY MONTH")
                     .setExpectedDataSource("foo")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Collections.singletonList(Intervals.ETERNITY))
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("%s/%s", Intervals.ETERNITY.getStart(), "2000-01-01"),
                             Intervals.of("%s/%s", "2000-02-01", Intervals.ETERNITY.getEnd())
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceOnFoo1Range()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(
                         "REPLACE INTO foo1 OVERWRITE ALL "
                         + "select  __time, dim1 , count(*) as cnt from foo  where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedShardSpec(DimensionRangeShardSpec.class)
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testReplaceSegmentsInsertIntoNewTable()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foobar "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY ALL TIME ")
                     .setExpectedDataSource("foobar")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foobar", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f},
                             new Object[]{946771200000L, 2.0f},
                             new Object[]{946857600000L, 3.0f},
                             new Object[]{978307200000L, 4.0f},
                             new Object[]{978393600000L, 5.0f},
                             new Object[]{978480000000L, 6.0f}
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceWithClusteredByDescendingThrowsException()
  {
    // Add a DESC clustered by column, which should not be allowed
    testIngestQuery().setSql(" REPLACE INTO foobar "
                             + "OVERWRITE ALL "
                             + "SELECT __time, m1, m2 "
                             + "FROM foo "
                             + "PARTITIONED BY ALL TIME "
                             + "CLUSTERED BY m2, m1 DESC"
                             )
                     .setExpectedValidationErrorMatcher(
                         invalidSqlIs("Invalid CLUSTERED BY clause [`m1` DESC]: cannot sort in descending order.")
                     )
                     .verifyPlanningErrors();
  }

  @Test
  public void testReplaceUnnestSegmentEntireTable()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.STRING)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, d "
                             + "FROM foo, UNNEST(MV_TO_ARRAY(dim3)) as unnested(d) "
                             + "PARTITIONED BY ALL TIME ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, "a"},
                             new Object[]{946684800000L, "b"},
                             new Object[]{946771200000L, "b"},
                             new Object[]{946771200000L, "c"},
                             new Object[]{946857600000L, "d"},
                             new Object[]{978307200000L, NullHandling.sqlCompatible() ? "" : null},
                             new Object[]{978393600000L, null},
                             new Object[]{978480000000L, null}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(8).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(8).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(8),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceUnnestWithVirtualColumnSegmentEntireTable()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE ALL "
                             + "SELECT __time, d "
                             + "FROM foo, UNNEST(ARRAY[m1, m2]) as unnested(d) "
                             + "PARTITIONED BY ALL TIME ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo",
                         Intervals.of("2000-01-01T/P1M"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(
                         ImmutableList.of(
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
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(12).frames(1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(12).frames(1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(12),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceUnnestSegmentWithTimeFilter()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("d", ColumnType.STRING)
                                            .build();

    DataSegment existingDataSegment0 = DataSegment.builder()
                                                  .interval(Intervals.of("2000-01-01T/2000-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();
    DataSegment existingDataSegment1 = DataSegment.builder()
                                                  .interval(Intervals.of("2001-01-01T/2001-01-04T"))
                                                  .size(50)
                                                  .version(MSQTestTaskActionClient.VERSION)
                                                  .dataSource("foo")
                                                  .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment0, existingDataSegment1))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo"),
               EasyMock.eq(ImmutableList.of(Intervals.of("1999/2002")))
           ));

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '1999-01-01 00:00:00' and __time < TIMESTAMP '2002-01-01 00:00:00'"
                             + "SELECT __time, d "
                             + "FROM foo, UNNEST(MV_TO_ARRAY(dim3)) as unnested(d) "
                             + "PARTITIONED BY DAY CLUSTERED BY d ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of(
                         "1999-01-01T00:00:00.000Z/2002-01-01T00:00:00.000Z")))
                     .setExpectedShardSpec(DimensionRangeShardSpec.class)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, "a"},
                             new Object[]{946684800000L, "b"},
                             new Object[]{946771200000L, "b"},
                             new Object[]{946771200000L, "c"},
                             new Object[]{946857600000L, "d"},
                             new Object[]{978307200000L, NullHandling.sqlCompatible() ? "" : null},
                             new Object[]{978393600000L, null},
                             new Object[]{978480000000L, null}
                         )
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().totalFiles(1),
                         0, 0, "input0"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2, 2, 1, 1, 1, 1).frames(1, 1, 1, 1, 1, 1),
                         0, 0, "shuffle"
                     )
                     .setExpectedCountersForStageWorkerChannel(
                         CounterSnapshotMatcher
                             .with().rows(2, 2, 1, 1, 1, 1).frames(1, 1, 1, 1, 1, 1),
                         1, 0, "input0"
                     )
                     .setExpectedSegmentGenerationProgressCountersForStageWorker(
                         CounterSnapshotMatcher
                             .with().segmentRowsProcessed(8),
                         1, 0
                     )
                     .verifyResults();
  }

  @Test
  public void testReplaceTombstonesOverPartiallyOverlappingSegments()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    // Create a datasegment which lies partially outside the generated segment
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.of("2001-01-01T/2003-01-04T"))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(new RetrieveUsedSegmentsAction(
               EasyMock.eq("foo1"),
               EasyMock.eq(ImmutableList.of(Intervals.of("2000/2002")))
           ));

    List<Object[]> expectedResults;
    if (NullHandling.sqlCompatible()) {
      expectedResults = ImmutableList.of(
          new Object[]{946684800000L, "", 1L},
          new Object[]{946771200000L, "10.1", 1L},
          new Object[]{946857600000L, "2", 1L},
          new Object[]{978307200000L, "1", 1L},
          new Object[]{978393600000L, "def", 1L},
          new Object[]{978480000000L, "abc", 1L}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{946771200000L, "10.1", 1L},
          new Object[]{946857600000L, "2", 1L},
          new Object[]{978307200000L, "1", 1L},
          new Object[]{978393600000L, "def", 1L},
          new Object[]{978480000000L, "abc", 1L}
      );
    }

    testIngestQuery().setSql(
                         "REPLACE INTO foo1 "
                         + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' and __time < TIMESTAMP '2002-01-01 00:00:00'"
                         + "SELECT  __time, dim1 , count(*) as cnt "
                         + "FROM foo "
                         + "WHERE dim1 IS NOT NULL "
                         + "GROUP BY 1, 2 "
                         + "PARTITIONED by TIME_FLOOR(__time, 'P3M') "
                         + "CLUSTERED by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedShardSpec(DimensionRangeShardSpec.class)
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("2001-04-01/P3M"),
                             Intervals.of("2001-07-01/P3M"),
                             Intervals.of("2001-10-01/P3M")
                         )
                     )
                     .setExpectedResultRows(expectedResults)
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceAll()
  {
    // An empty replace all with no used segment should effectively be the same as an empty insert
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceInterval()
  {
    // An empty replace interval with no used segment should effectively be the same as an empty insert
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE WHERE __time >= TIMESTAMP '2016-06-27 01:00:00.00' AND __time < TIMESTAMP '2016-06-27 02:00:00.00'"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY HOUR"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceAllOverExistingSegment()
  {
    Interval existingSegmentInterval = Intervals.of("2001-01-01T/2001-01-02T");
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(existingSegmentInterval)
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(ImmutableSet.of(existingSegmentInterval))
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceIntervalOverPartiallyOverlappingSegment()
  {
    // Create a data segment which lies partially outside the generated segment
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.of("2016-06-27T/2016-06-28T"))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE WHERE __time >= TIMESTAMP '2016-06-27 01:00:00.00' AND __time < TIMESTAMP '2016-06-27 02:00:00.00'"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY HOUR"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("2016-06-27T01:00:00/2016-06-27T02:00:00")
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceIntervalOverPartiallyOverlappingStart()
  {
    // Create a data segment whose start partially lies outside the query's replace interval
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.of("2016-06-01T/2016-07-01T"))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE WHERE __time >= TIMESTAMP '2016-06-29' AND __time < TIMESTAMP '2016-07-03'"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("2016-06-29T/2016-06-30T"),
                             Intervals.of("2016-06-30T/2016-07-01T")
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceIntervalOverPartiallyOverlappingEnd()
  {
    // Create a data segment whose end partially lies outside the query's replace interval
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.of("2016-06-01T/2016-07-01T"))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE WHERE __time >= TIMESTAMP '2016-05-25' AND __time < TIMESTAMP '2016-06-03'"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("2016-06-01T/2016-06-02T"),
                             Intervals.of("2016-06-02T/2016-06-03T")
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceAllOverEternitySegment()
  {
    // Create a data segment spanning eternity
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.ETERNITY)
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.ETERNITY))
                     .verifyResults();
  }


  @Test
  public void testEmptyReplaceAllWithAllGrainOverFiniteIntervalSegment()
  {
    // Create a finite-interval segment
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.of("2016-06-01T/2016-09-01T"))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();
    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.of("2016-06-01T/2016-09-01T")))
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceAllWithAllGrainOverEternitySegment()
  {
    // Create a segment spanning eternity
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.ETERNITY)
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.ETERNITY))
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceAllWithAllGrainOverHalfEternitySegment()
  {
    // Create a segment spanning half-eternity
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(new Interval(DateTimes.of("2000"), DateTimes.MAX))
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();
    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.ETERNITY))
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceLimitQuery()
  {
    // A limit query which results in 0 rows being inserted -- do nothing.
    testIngestQuery().setSql(
                         "REPLACE INTO foo1 "
                         + " OVERWRITE ALL"
                         + " SELECT  __time, dim1, COUNT(*) AS cnt"
                         + " FROM foo WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " LIMIT 100"
                         + " PARTITIONED BY ALL"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of())
                     .verifyResults();
  }

  @Test
  public void testEmptyReplaceIntervalOverEternitySegment()
  {
    // Create a data segment spanning eternity
    DataSegment existingDataSegment = DataSegment.builder()
                                                 .interval(Intervals.ETERNITY)
                                                 .size(50)
                                                 .version(MSQTestTaskActionClient.VERSION)
                                                 .dataSource("foo1")
                                                 .build();

    Mockito.doReturn(ImmutableSet.of(existingDataSegment))
           .when(testTaskActionClient)
           .submit(ArgumentMatchers.isA(RetrieveUsedSegmentsAction.class));

    // Insert with a condition which results in 0 rows being inserted -- do nothing!
    testIngestQuery().setSql(
                         "REPLACE INTO foo1"
                         + " OVERWRITE WHERE __time >= TIMESTAMP '2016-06-01' AND __time < TIMESTAMP '2016-06-03'"
                         + " SELECT  __time, dim1 , count(*) AS cnt"
                         + " FROM foo"
                         + " WHERE dim1 IS NOT NULL AND __time < TIMESTAMP '1971-01-01 00:00:00'"
                         + " GROUP BY 1, 2"
                         + " PARTITIONED BY DAY"
                         + " CLUSTERED BY dim1")
                     .setQueryContext(context)
                     .setExpectedDataSource("foo1")
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedTombstoneIntervals(
                         ImmutableSet.of(
                             Intervals.of("2016-06-01T/2016-06-02T"),
                             Intervals.of("2016-06-02T/2016-06-03T")
                         )
                     )
                     .verifyResults();
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
}
