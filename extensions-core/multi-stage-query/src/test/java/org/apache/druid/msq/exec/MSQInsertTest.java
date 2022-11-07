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
import org.apache.druid.msq.indexing.error.InsertTimeNullFault;
import org.apache.druid.msq.indexing.error.RowTooLargeFault;
import org.apache.druid.msq.test.MSQTestBase;
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
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class MSQInsertTest extends MSQTestBase
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testInsertOnFoo1()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testInsertOnExternalDataSource() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
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
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of(
                         "foo1",
                         Intervals.of("2016-06-27/P1D"),
                         "test",
                         0
                     )))
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
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
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
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
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRowsToArray())
                     .verifyResults();
  }

  @Test
  public void testInsertOnFoo1WithMultiValueDimGroupByWithoutGroupByEnable()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put("groupByEnableMultiValueUnnesting", false)
                                              .build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT dim3, count(*) AS cnt1 FROM foo GROUP BY dim3 PARTITIONED BY ALL TIME")
                     .setQueryContext(context)
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false."))
                     ))
                     .verifyExecutionError();
  }

  @Test
  public void testRolltestRollUpOnFoo1UpOnFoo1()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
                     .verifyResults();

  }

  @Test
  public void testRollUpOnFoo1WithTimeFunction()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();


    testIngestQuery().setSql(
                         "insert into foo1 select  floor(__time to day) as __time , dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedQueryGranularity(Granularities.DAY)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedSegment(expectedFooSegments())
                     .setExpectedResultRows(expectedFooRows())
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
                     .setQueryContext(ROLLUP_CONTEXT)
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
                     .setQueryContext(ROLLUP_CONTEXT)
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
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
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
                     .setQueryContext(ROLLUP_CONTEXT)
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
                     .verifyResults();
  }

  @Test()
  public void testRollUpOnExternalDataSourceWithCompositeKey() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
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
                     .setQueryContext(ROLLUP_CONTEXT)
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
                     .verifyResults();

  }

  @Test
  public void testInsertNullTimestamp()
  {
    final RowSignature rowSignature =
        RowSignature.builder()
                    .add("__time", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();

    testIngestQuery()
        .setSql(
            "INSERT INTO foo1\n"
            + "SELECT TIME_PARSE(dim1) AS __time, dim1 as cnt\n"
            + "FROM foo\n"
            + "PARTITIONED BY DAY\n"
            + "CLUSTERED BY dim1")
        .setExpectedDataSource("foo1")
        .setExpectedRowSignature(rowSignature)
        .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.of("2000-01-01T/P1M"), "test", 0)))
        .setExpectedMSQFault(InsertTimeNullFault.instance())
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
        .setExpectedMSQFault(new ColumnNameRestrictedFault("__bucket"))
        .verifyResults();
  }

  @Test
  public void testInsertQueryWithInvalidSubtaskCount()
  {
    Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                              .putAll(DEFAULT_MSQ_CONTEXT)
                                              .put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 1)
                                              .build();
    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setQueryContext(context)
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
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
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
