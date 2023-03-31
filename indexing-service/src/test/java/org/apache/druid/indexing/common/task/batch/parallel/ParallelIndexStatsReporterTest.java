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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class ParallelIndexStatsReporterTest
{
  static class TestReporter extends ParallelIndexStatsReporter
  {
    @Override
    ParallelIndexStats report(
        ParallelIndexSupervisorTask task,
        Object runner,
        boolean includeUnparseable,
        boolean full
    )
    {
      //noinspection ReturnOfNull
      return null;
    }
  }

  private static final ParseExceptionReport PARSE_EXCEPTION_REPORT = new ParseExceptionReport(
      "one,two,three",
      "some_error_type",
      ImmutableList.of("error details here"),
      123L
  );
  private static final RowIngestionMetersTotals ROW_INGESTION_METERS_TOTALS = new RowIngestionMetersTotals(
      1L,
      5L,
      2L,
      3L,
      4L
  );
  private static final Map<String, Object> UNPARSEABLE_EVENTS = ImmutableMap.of(
      RowIngestionMeters.BUILD_SEGMENTS,
      ImmutableList.of(PARSE_EXCEPTION_REPORT)
  );
  private static final Map<String, Object> ROW_STATS = ImmutableMap.of(
      RowIngestionMeters.BUILD_SEGMENTS,
      ROW_INGESTION_METERS_TOTALS
  );

  @Mock
  private ParallelIndexSupervisorTask supervisorTask;
  private TestReporter reporter;

  @Before
  public void setUp() throws Exception
  {
    reporter = new TestReporter();
  }

  @Test
  public void testStatsEquals()
  {
    ParallelIndexStats stats = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    ParallelIndexStats stats2 = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    Assert.assertEquals(stats, stats2);
  }

  @Test
  public void testStatsEqualsIdentity()
  {
    ParallelIndexStats stats = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    Assert.assertSame(stats, stats);
  }

  @Test
  public void testStatsNotEquals()
  {
    ParallelIndexStats stats = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    ParallelIndexStats stats2 = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "barq"),
        ImmutableSet.of()
    );
    Assert.assertNotEquals(stats, stats2);
  }

  @Test
  public void testStatsNotEqualsNull()
  {
    ParallelIndexStats stats = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    Assert.assertNotEquals(stats, null);
  }

  @Test
  public void testStatsNotEqualsOtherClass()
  {
    ParallelIndexStats stats = new ParallelIndexStats(
        ImmutableMap.of("foo", "bar"),
        ImmutableMap.of("foo", "bar"),
        ImmutableSet.of()
    );
    //noinspection AssertBetweenInconvertibleTypes
    Assert.assertNotEquals(stats, "non-stats");
  }

  @Test
  public void testDataEquals()
  {
    IngestionStatsAndErrorsTaskReportData data = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        0L,
        ImmutableList.of()
    );
    IngestionStatsAndErrorsTaskReportData data2 = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        0L,
        ImmutableList.of()
    );
    Assert.assertEquals(data2, data);
  }

  @Test
  public void testDataNotEquals()
  {
    IngestionStatsAndErrorsTaskReportData data = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        1L,
        ImmutableList.of()
    );
    IngestionStatsAndErrorsTaskReportData data2 = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        0L,
        ImmutableList.of()
    );
    Assert.assertNotEquals(data2, data);
  }

  @Test
  public void testDataNotEqualsIntervals()
  {
    IngestionStatsAndErrorsTaskReportData data = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        1L,
        ImmutableList.of()
    );
    IngestionStatsAndErrorsTaskReportData data2 = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        1L,
        ImmutableList.of(Intervals.of("2020-01-01/2020-02-01"))
    );
    Assert.assertNotEquals(data2, data);
  }

  @Test
  public void testGetBuildSegmentStatsFromTaskReportHappy()
  {
    IngestionStatsAndErrorsTaskReportData data = new IngestionStatsAndErrorsTaskReportData(
        IngestionState.COMPLETED,
        UNPARSEABLE_EVENTS,
        ROW_STATS,
        "error msg",
        true,
        0L,
        ImmutableList.of()
    );
    IngestionStatsAndErrorsTaskReport taskReport = new IngestionStatsAndErrorsTaskReport("myTaskId", data);
    Map<String, TaskReport> taskReports = ImmutableMap.of(IngestionStatsAndErrorsTaskReport.REPORT_KEY, taskReport);

    List<ParseExceptionReport> parseExceptionReports = new ArrayList<>();
    RowIngestionMetersTotals totals = reporter.getBuildSegmentsStatsFromTaskReport(
        taskReports,
        true,
        parseExceptionReports
    );

    Assert.assertEquals(ROW_INGESTION_METERS_TOTALS, totals);
    Assert.assertEquals(1, parseExceptionReports.size());
    Assert.assertEquals(PARSE_EXCEPTION_REPORT, parseExceptionReports.get(0));
  }

  @Test
  public void testCreateStatsAndErrorsReportHappy()
  {
    Pair<Map<String, Object>, Map<String, Object>> report = reporter.createStatsAndErrorsReport(
        ROW_INGESTION_METERS_TOTALS,
        ImmutableList.of(PARSE_EXCEPTION_REPORT)
    );

    Map<String, Object> rowStatsMap = ImmutableMap.of("totals", ROW_STATS);
    Assert.assertEquals(rowStatsMap, report.lhs);
    Assert.assertEquals(UNPARSEABLE_EVENTS, report.rhs);
  }

  @Test
  public void testRunningTasksHappy()
  {
    Mockito.when(supervisorTask.fetchTaskReport(ArgumentMatchers.anyString())).thenReturn(createReportMap());

    RowIngestionMetersTotals totals = reporter.getRowStatsAndUnparseableEventsForRunningTasks(
        supervisorTask,
        ImmutableSet.of("task1", "task2"),
        Collections.emptyList(),
        false
    );

    RowIngestionMetersTotals expected = new RowIngestionMetersTotals(2L, 10L, 4L, 6L, 8L);
    Assert.assertEquals(expected, totals);
  }

  @Test
  public void testRunningTasksNullReport()
  {
    Mockito.when(supervisorTask.fetchTaskReport(ArgumentMatchers.anyString())).thenReturn(createReportMap());
    Mockito.when(supervisorTask.fetchTaskReport("task3")).thenReturn(null);

    RowIngestionMetersTotals totals = reporter.getRowStatsAndUnparseableEventsForRunningTasks(
        supervisorTask,
        ImmutableSet.of("task1", "task2", "task3"),
        Collections.emptyList(),
        false
    );

    RowIngestionMetersTotals expected = new RowIngestionMetersTotals(2L, 10L, 4L, 6L, 8L);
    Assert.assertEquals(expected, totals);
  }

  @Test
  public void testRunningTasksEmptyReport()
  {
    Mockito.when(supervisorTask.fetchTaskReport(ArgumentMatchers.anyString())).thenReturn(createReportMap());
    Mockito.when(supervisorTask.fetchTaskReport("task3")).thenReturn(ImmutableMap.of());

    RowIngestionMetersTotals totals = reporter.getRowStatsAndUnparseableEventsForRunningTasks(
        supervisorTask,
        ImmutableSet.of("task1", "task2", "task3"),
        Collections.emptyList(),
        false
    );

    RowIngestionMetersTotals expected = new RowIngestionMetersTotals(2L, 10L, 4L, 6L, 8L);
    Assert.assertEquals(expected, totals);
  }

  @Test
  public void testRunningTasksIncludeUnparseable()
  {
    Mockito.when(supervisorTask.fetchTaskReport(ArgumentMatchers.anyString())).thenReturn(createReportMap());

    List<ParseExceptionReport> parseExceptionReports = new ArrayList<>();
    RowIngestionMetersTotals totals = reporter.getRowStatsAndUnparseableEventsForRunningTasks(
        supervisorTask,
        ImmutableSet.of("task1", "task2"),
        parseExceptionReports,
        true
    );

    RowIngestionMetersTotals expected = new RowIngestionMetersTotals(2L, 10L, 4L, 6L, 8L);
    Assert.assertEquals(expected, totals);
    Assert.assertEquals(2, parseExceptionReports.size());
  }

  private static Map<String, Object> createReportMap()
  {
    return ImmutableMap.of("ingestionStatsAndErrors", ImmutableMap.of(
        "payload", ImmutableMap.of(
            "rowStats", ImmutableMap.of(
                "totals", ImmutableMap.of(
                    "buildSegments", ImmutableMap.of(
                        "processed", 1L,
                        "processedBytes", 5L,
                        "processedWithError", 2L,
                        "thrownAway", 3L,
                        "unparseable", 4L
                    )
                )
            ),
            "unparseableEvents", ImmutableMap.of(
                "buildSegments", ImmutableList.of(
                    new ParseExceptionReport(
                        "some_input",
                        "bad error type",
                        ImmutableList.of("detail1", "detail2"),
                        System.currentTimeMillis()
                    )
                )
            )
        )
    ));
  }
}
