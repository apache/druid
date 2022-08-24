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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.kernel.MaxCountShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.common.OffsetLimitFrameProcessorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MSQTaskReportTest
{
  private static final String TASK_ID = "mytask";
  private static final String HOST = "example.com:1234";
  private static final QueryDefinition QUERY_DEFINITION =
      QueryDefinition
          .builder()
          .add(
              StageDefinition
                  .builder(0)
                  .processorFactory(new OffsetLimitFrameProcessorFactory(0, 1L))
                  .shuffleSpec(
                      new MaxCountShuffleSpec(
                          new ClusterBy(ImmutableList.of(new SortColumn("s", false)), 0),
                          2,
                          false
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

    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.SUCCESS, null, new ArrayDeque<>(), null, 0),
            MSQStagesReport.create(
                QUERY_DEFINITION,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            new CounterSnapshotsTree(),
            new MSQResultsReport(
                RowSignature.builder().add("s", ColumnType.STRING).build(),
                ImmutableList.of("VARCHAR"),
                Yielders.each(Sequences.simple(results))
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
    Assert.assertEquals(report.getPayload().getStages(), report2.getPayload().getStages());

    Yielder<Object[]> yielder = report2.getPayload().getResults().getResultYielder();
    final List<Object[]> results2 = new ArrayList<>();

    while (!yielder.isDone()) {
      results2.add(yielder.get());
      yielder = yielder.next(null);
    }

    Assert.assertEquals(results.size(), results2.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(results.get(i), results2.get(i));
    }
  }

  @Test
  public void testSerdeErrorReport() throws Exception
  {
    final MSQErrorReport errorReport = MSQErrorReport.fromFault(TASK_ID, HOST, 0, new TooManyColumnsFault(10, 5));
    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.FAILED, errorReport, new ArrayDeque<>(), null, 0),
            MSQStagesReport.create(
                QUERY_DEFINITION,
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
  @Ignore("requires https://github.com/apache/druid/pull/12938")
  public void testWriteTaskReport() throws Exception
  {
    final MSQTaskReport report = new MSQTaskReport(
        TASK_ID,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.SUCCESS, null, new ArrayDeque<>(), null, 0),
            MSQStagesReport.create(
                QUERY_DEFINITION,
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

    final Map<String, TaskReport> reportMap = mapper.readValue(
        reportFile,
        new TypeReference<Map<String, TaskReport>>() {}
    );

    final MSQTaskReport report2 = (MSQTaskReport) reportMap.get(MSQTaskReport.REPORT_KEY);

    Assert.assertEquals(TASK_ID, report2.getTaskId());
    Assert.assertEquals(report.getPayload().getStatus().getStatus(), report2.getPayload().getStatus().getStatus());
    Assert.assertEquals(report.getPayload().getStages(), report2.getPayload().getStages());
  }
}
