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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.SegmentLoadStatusFetcher;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.kernel.GlobalSortMaxCountShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class MSQTaskReportTest
{
  private static final String TASK_ID = "mytask";
  private static final String HOST = "example.com:1234";
  public static final QueryDefinition QUERY_DEFINITION =
      QueryDefinition
          .builder(UUID.randomUUID().toString())
          .add(
              StageDefinition
                  .builder(0)
                  .processor(new OffsetLimitStageProcessor(0, 1L))
                  .shuffleSpec(
                      new GlobalSortMaxCountShuffleSpec(
                          new ClusterBy(ImmutableList.of(new KeyColumn("s", KeyOrder.ASCENDING)), 0),
                          2,
                          false,
                          ShuffleSpec.UNLIMITED
                      )
                  )
                  .maxWorkerCount(3)
                  .signature(RowSignature.builder().add("s", ColumnType.STRING).build())
          )
          .build();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSerdeResultsReport() throws Exception
  {
    final List<Object[]> results = ImmutableList.of(
        new Object[]{"foo"},
        new Object[]{"bar"}
    );

    SegmentLoadStatusFetcher.SegmentLoadWaiterStatus status = new SegmentLoadStatusFetcher.SegmentLoadWaiterStatus(
        SegmentLoadStatusFetcher.State.WAITING,
        DateTimes.nowUtc(),
        200L,
        100,
        80,
        30,
        50,
        10,
        0
    );

    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.SUCCESS, null, new ArrayDeque<>(), null, 0, new HashMap<>(), 1, 2, status, null),
            MSQStagesReport.create(
                QUERY_DEFINITION,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            new CounterSnapshotsTree(),
            new MSQResultsReport(
                Collections.singletonList(new MSQResultsReport.ColumnAndType("s", ColumnType.STRING)),
                ImmutableList.of(SqlTypeName.VARCHAR),
                results,
                null
            )
        )
    );

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    final MSQTaskReport report2 = (MSQTaskReport) mapper.readValue(
        mapper.writeValueAsString(report),
        TaskReport.class
    );

    Assert.assertEquals(TASK_ID, report2.getTaskId());
    Assert.assertEquals(report.getPayload().getStatus().getStatus(), report2.getPayload().getStatus().getStatus());
    Assert.assertNull(report2.getPayload().getStatus().getErrorReport());
    Assert.assertEquals(report.getPayload().getStatus().getRunningTasks(), report2.getPayload().getStatus().getRunningTasks());
    Assert.assertEquals(report.getPayload().getStatus().getPendingTasks(), report2.getPayload().getStatus().getPendingTasks());
    Assert.assertEquals(report.getPayload().getStages(), report2.getPayload().getStages());

    final List<Object[]> results2 = report2.getPayload().getResults().getResults();
    Assert.assertEquals(results.size(), results2.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(results.get(i), results2.get(i));
    }
  }

  @Test
  public void testSerdeErrorReport() throws Exception
  {
    SegmentLoadStatusFetcher.SegmentLoadWaiterStatus status = new SegmentLoadStatusFetcher.SegmentLoadWaiterStatus(
        SegmentLoadStatusFetcher.State.FAILED,
        DateTimes.nowUtc(),
        200L,
        100,
        80,
        30,
        50,
        10,
        0
    );

    final MSQErrorReport errorReport = MSQErrorReport.fromFault(TASK_ID, HOST, 0, new TooManyColumnsFault(10, 5));
    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.FAILED, errorReport, new ArrayDeque<>(), null, 0, new HashMap<>(), 1, 2, status, null),
            MSQStagesReport.create(
                QUERY_DEFINITION,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            new CounterSnapshotsTree(),
            null
        )
    );

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    final MSQTaskReport report2 = (MSQTaskReport) mapper.readValue(
        mapper.writeValueAsString(report),
        TaskReport.class
    );

    Assert.assertEquals(TASK_ID, report2.getTaskId());
    Assert.assertEquals(report.getPayload().getStatus().getStatus(), report2.getPayload().getStatus().getStatus());
    Assert.assertEquals(
        report.getPayload().getStatus().getErrorReport(),
        report2.getPayload().getStatus().getErrorReport()
    );
    Assert.assertEquals(report.getPayload().getStages(), report2.getPayload().getStages());
  }

  @Test
  public void testWriteTaskReport() throws Exception
  {
    SegmentLoadStatusFetcher.SegmentLoadWaiterStatus status = new SegmentLoadStatusFetcher.SegmentLoadWaiterStatus(
        SegmentLoadStatusFetcher.State.SUCCESS,
        DateTimes.nowUtc(),
        200L,
        100,
        80,
        30,
        50,
        10,
        0
    );

    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.SUCCESS, null, new ArrayDeque<>(), null, 0, new HashMap<>(), 1, 2, status, null),
            MSQStagesReport.create(
                QUERY_DEFINITION,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            new CounterSnapshotsTree(),
            null
        )
    );

    final File reportFile = temporaryFolder.newFile();
    final SingleFileTaskReportFileWriter writer = new SingleFileTaskReportFileWriter(reportFile);
    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    writer.setObjectMapper(mapper);
    writer.write(TASK_ID, TaskReport.buildTaskReports(report));

    final TaskReport.ReportMap reportMap = mapper.readValue(
        reportFile,
        TaskReport.ReportMap.class
    );

    final MSQTaskReport report2 = (MSQTaskReport) reportMap.get(MSQTaskReport.REPORT_KEY);

    Assert.assertEquals(TASK_ID, report2.getTaskId());
    Assert.assertEquals(report.getPayload().getStatus().getStatus(), report2.getPayload().getStatus().getStatus());
    Assert.assertEquals(report.getPayload().getStages(), report2.getPayload().getStages());
  }
}
