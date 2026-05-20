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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskContextReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaskReportQueryListenerTest
{
  private static final String TASK_ID = "mytask";
  private static final Map<String, Object> TASK_CONTEXT = ImmutableMap.of("foo", "bar");
  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("x", ColumnType.STRING)
                                                            .build();
  private static final List<SqlTypeName> SQL_TYPE_NAMES = ImmutableList.of(SqlTypeName.VARCHAR);
  private static final ObjectMapper JSON_MAPPER =
      TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  /**
   * Creates a Frame containing the given rows using the test signature.
   */
  private Frame createFrame(final List<Map<String, Object>> rows)
  {
    final RowBasedCursorFactory<Map<String, Object>> cursorFactory = new RowBasedCursorFactory<>(
        Sequences.simple(rows),
        new MapRowAdapter(SIGNATURE),
        SIGNATURE
    );

    return FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                               .frameType(FrameType.latestRowBased())
                               .maxRowsPerFrame(Integer.MAX_VALUE)
                               .frames()
                               .toList()
                               .get(0);
  }

  @Test
  public void test_taskReportDestination() throws IOException
  {
    final FrameReader frameReader = FrameReader.create(SIGNATURE);
    final ResultsContext resultsContext = new ResultsContext(SQL_TYPE_NAMES, null);

    final TaskReportQueryListener listener = new TaskReportQueryListener(
        Suppliers.ofInstance(baos)::get,
        JSON_MAPPER,
        TASK_ID,
        TASK_CONTEXT,
        TaskReportMSQDestination.instance().getRowsInTaskReport(),
        ColumnMappings.identity(SIGNATURE),
        resultsContext
    );

    Assert.assertTrue(listener.readResults());
    listener.onResultsStart(frameReader);

    // Create a frame with two rows
    final Frame frame = createFrame(ImmutableList.of(
        ImmutableMap.of("x", "foo"),
        ImmutableMap.of("x", "bar")
    ));
    Assert.assertTrue(listener.onResultBatch(frame.asRAC()));

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
            new TypeReference<>() {}
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
    final FrameReader frameReader = FrameReader.create(SIGNATURE);
    final ResultsContext resultsContext = new ResultsContext(SQL_TYPE_NAMES, null);

    final TaskReportQueryListener listener = new TaskReportQueryListener(
        Suppliers.ofInstance(baos)::get,
        JSON_MAPPER,
        TASK_ID,
        TASK_CONTEXT,
        DurableStorageMSQDestination.instance().getRowsInTaskReport(),
        ColumnMappings.identity(SIGNATURE),
        resultsContext
    );

    Assert.assertTrue(listener.readResults());
    listener.onResultsStart(frameReader);

    // Create frames with rows up to MAX_SELECT_RESULT_ROWS
    final int batchSize = 100;
    int rowsAdded = 0;
    boolean keepGoing = true;

    while (keepGoing && rowsAdded < Limits.MAX_SELECT_RESULT_ROWS) {
      final int rowsToAdd = (int) Math.min(batchSize, Limits.MAX_SELECT_RESULT_ROWS - rowsAdded);
      final List<Map<String, Object>> rows = IntStream.range(0, rowsToAdd)
                                                       .mapToObj(i -> ImmutableMap.<String, Object>of("x", "foo"))
                                                       .collect(Collectors.toList());
      final Frame frame = createFrame(rows);
      keepGoing = listener.onResultBatch(frame.asRAC());
      rowsAdded += rowsToAdd;
    }

    // Should have stopped accepting results after MAX_SELECT_RESULT_ROWS
    Assert.assertFalse(keepGoing);

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
            new TypeReference<>() {}
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

  /**
   * Simple RowAdapter for Map-based rows.
   */
  private static class MapRowAdapter implements RowAdapter<Map<String, Object>>
  {
    private final RowSignature signature;

    MapRowAdapter(final RowSignature signature)
    {
      this.signature = signature;
    }

    @Override
    public ToLongFunction<Map<String, Object>> timestampFunction()
    {
      return row -> 0L;
    }

    @Override
    public Function<Map<String, Object>, Object> columnFunction(final String columnName)
    {
      return row -> row.get(columnName);
    }
  }
}
