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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskContextReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskReportQueryListenerTest
{
  private static final String TASK_ID = "mytask";
  private static final Map<String, Object> TASK_CONTEXT = ImmutableMap.of("foo", "bar");
  private static final List<MSQResultsReport.ColumnAndType> SIGNATURE = ImmutableList.of(
      new MSQResultsReport.ColumnAndType("x", ColumnType.STRING)
  );
  private static final List<SqlTypeName> SQL_TYPE_NAMES = ImmutableList.of(SqlTypeName.VARCHAR);
  private static final ObjectMapper JSON_MAPPER =
      TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  @Test
  public void test_taskReportDestination() throws IOException
  {
    final TaskReportQueryListener listener = new TaskReportQueryListener(
        TaskReportMSQDestination.instance(),
        Suppliers.ofInstance(baos)::get,
        JSON_MAPPER,
        TASK_ID,
        TASK_CONTEXT
    );

    Assert.assertTrue(listener.readResults());
    listener.onResultsStart(SIGNATURE, SQL_TYPE_NAMES);
    Assert.assertTrue(listener.onResultRow(new Object[]{"foo"}));
    Assert.assertTrue(listener.onResultRow(new Object[]{"bar"}));
    listener.onResultsComplete();
    listener.onQueryComplete(
        new MSQTaskReportPayload(
            new MSQStatusReport(
                TaskState.SUCCESS,
                null,
                Collections.emptyList(),
                null,
                0,
                new HashMap<>(),
                1,
                2,
                null,
                null
            ),
            MSQStagesReport.create(
                MSQTaskReportTest.QUERY_DEFINITION,
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

    final TaskReport.ReportMap reportMap =
        JSON_MAPPER.readValue(
            baos.toByteArray(),
            new TypeReference<TaskReport.ReportMap>() {}
        );

    Assert.assertEquals(ImmutableSet.of("multiStageQuery", TaskContextReport.REPORT_KEY), reportMap.keySet());
    Assert.assertEquals(TASK_CONTEXT, ((TaskContextReport) reportMap.get(TaskContextReport.REPORT_KEY)).getPayload());

    final MSQTaskReport report = (MSQTaskReport) reportMap.get("multiStageQuery");
    final List<List<Object>> results =
        report.getPayload().getResults().getResults().stream().map(Arrays::asList).collect(Collectors.toList());

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of("foo"),
            ImmutableList.of("bar")
        ),
        results
    );

    Assert.assertFalse(report.getPayload().getResults().isResultsTruncated());
    Assert.assertEquals(TaskState.SUCCESS, report.getPayload().getStatus().getStatus());
  }

  @Test
  public void test_durableDestination() throws IOException
  {
    final TaskReportQueryListener listener = new TaskReportQueryListener(
        DurableStorageMSQDestination.instance(),
        Suppliers.ofInstance(baos)::get,
        JSON_MAPPER,
        TASK_ID,
        TASK_CONTEXT
    );

    Assert.assertTrue(listener.readResults());
    listener.onResultsStart(SIGNATURE, SQL_TYPE_NAMES);
    for (int i = 0; i < Limits.MAX_SELECT_RESULT_ROWS - 1; i++) {
      Assert.assertTrue("row #" + i, listener.onResultRow(new Object[]{"foo"}));
    }
    Assert.assertFalse(listener.onResultRow(new Object[]{"foo"}));
    listener.onQueryComplete(
        new MSQTaskReportPayload(
            new MSQStatusReport(
                TaskState.SUCCESS,
                null,
                Collections.emptyList(),
                null,
                0,
                new HashMap<>(),
                1,
                2,
                null,
                null
            ),
            MSQStagesReport.create(
                MSQTaskReportTest.QUERY_DEFINITION,
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

    final TaskReport.ReportMap reportMap =
        JSON_MAPPER.readValue(
            baos.toByteArray(),
            new TypeReference<TaskReport.ReportMap>() {}
        );

    Assert.assertEquals(ImmutableSet.of("multiStageQuery", TaskContextReport.REPORT_KEY), reportMap.keySet());
    Assert.assertEquals(TASK_CONTEXT, ((TaskContextReport) reportMap.get(TaskContextReport.REPORT_KEY)).getPayload());

    final MSQTaskReport report = (MSQTaskReport) reportMap.get("multiStageQuery");
    final List<List<Object>> results =
        report.getPayload().getResults().getResults().stream().map(Arrays::asList).collect(Collectors.toList());

    Assert.assertEquals(
        IntStream.range(0, (int) Limits.MAX_SELECT_RESULT_ROWS)
                 .mapToObj(i -> ImmutableList.of("foo"))
                 .collect(Collectors.toList()),
        results
    );

    Assert.assertTrue(report.getPayload().getResults().isResultsTruncated());
    Assert.assertEquals(TaskState.SUCCESS, report.getPayload().getStatus().getStatus());
  }
}
