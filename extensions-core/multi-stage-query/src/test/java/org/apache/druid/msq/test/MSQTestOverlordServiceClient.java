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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexer.report.TaskReport.ReportMap;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.exec.ResultsContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.sql.MSQTaskQueryKitSpecFactory;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MSQTestOverlordServiceClient extends NoopOverlordClient
{
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final TaskActionClient taskActionClient;
  private final WorkerMemoryParameters workerMemoryParameters;
  private final List<ImmutableSegmentLoadInfo> loadedSegmentMetadata;

  private final Map<String, MSQTestTaskDetails> taskDetailsByTaskId = new HashMap<>();
  private final Map<String, MSQTestTaskDetails> taskDetailsByQueryId = new HashMap<>();

  public static final DateTime CREATED_TIME = DateTimes.of("2023-05-31T12:00Z");
  public static final DateTime QUEUE_INSERTION_TIME = DateTimes.of("2023-05-31T12:01Z");

  public static final long DURATION = 100L;


  public class MSQTestTaskDetails implements AutoCloseable
  {
    private String taskId;
    public MSQControllerTask controllerTask;
    private ControllerImpl controller;
    private TaskStatus taskStatus;
    public ReportMap report;

    MSQTestTaskDetails(String taskId)
    {
      this.taskId = taskId;
    }

    public void addController(ControllerImpl controller)
    {
      if (this.controller != null) {
        throw DruidException.defensive("Attempt to register a second controller!");
      }
      this.controller = controller;
      registerController(controller.queryId(), this);
    }

    public ControllerImpl getController(String queryId)
    {
      if (controller.queryId().equals(queryId)) {
        return controller;
      }
      return null;
    }

    @Override
    public void close()
    {
      taskDetailsByTaskId.remove(taskId);
      taskDetailsByQueryId.remove(controller.queryId());
    }
  }

  public MSQTestOverlordServiceClient(
      ObjectMapper objectMapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegmentMetadata
  )
  {
    this.objectMapper = objectMapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    this.workerMemoryParameters = workerMemoryParameters;
    this.loadedSegmentMetadata = loadedSegmentMetadata;
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    TestQueryListener queryListener = null;
    ControllerImpl controller = null;
    MSQTestControllerContext msqTestControllerContext;
    MSQTestTaskDetails testTaskDetails = registerTestTask(taskId);
    try {
      MSQControllerTask cTask = objectMapper.convertValue(taskObject, MSQControllerTask.class);

      msqTestControllerContext = new MSQTestControllerContext(
          cTask.getId(),
          objectMapper,
          injector,
          taskActionClient,
          workerMemoryParameters,
          loadedSegmentMetadata,
          cTask.getTaskLockType(),
          cTask.getQuerySpec().getContext()
      );

      assertEquals(taskId, cTask.getId());
      testTaskDetails.controllerTask = cTask;

      controller = new ControllerImpl(
          cTask.getId(),
          cTask.getQuerySpec(),
          new ResultsContext(cTask.getSqlTypeNames(), cTask.getSqlResultsContext()),
          msqTestControllerContext,
          injector.getInstance(MSQTaskQueryKitSpecFactory.class)
      );

      testTaskDetails.addController(controller);

      queryListener =
          new TestQueryListener(
              cTask.getId(),
              cTask.getQuerySpec().getDestination()
          );

      try {
        controller.run(queryListener);
        testTaskDetails.taskStatus = queryListener.getStatusReport().toTaskStatus(cTask.getId());
      }
      catch (Exception e) {
        testTaskDetails.taskStatus = TaskStatus.failure(cTask.getId(), e.toString());
      }
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "Unable to run");
    }
    finally {
      if (queryListener != null && queryListener.reportMap != null) {
        testTaskDetails.report = queryListener.getReportMap();
      }
    }
  }

  private static final Logger LOG = new Logger(MSQTestOverlordServiceClient.class);


  private MSQTestTaskDetails registerTestTask(String taskId)
  {
    MSQTestTaskDetails details = taskDetailsByTaskId.get(taskId);
    if (details != null) {
      LOG.warn("There is an un-closed taskId which will be overwritten; closing implicitly");
      details.close();
    }
    details = new MSQTestTaskDetails(taskId);
    taskDetailsByTaskId.put(taskId, details);
    return details;
  }

  private void registerController(String queryId, MSQTestTaskDetails msqTestTaskDetails)
  {
    MSQTestTaskDetails old = taskDetailsByQueryId.get(queryId);
    if (old != null) {
      throw DruidException.defensive("There is an existing queryId {}!", queryId);
    }
    taskDetailsByQueryId.put(queryId, msqTestTaskDetails);
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    getControllerForQueryId(taskId).stop(CancellationReason.TASK_SHUTDOWN);
    return Futures.immediateFuture(null);
  }

  private ControllerImpl getControllerForQueryId(String queryId)
  {
    MSQTestTaskDetails details = taskDetailsByQueryId.get(queryId);
    if (details == null) {
      return null;
    }
    return details.getController(queryId);
  }

  @Override
  public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
  {
    return Futures.immediateFuture(getReportForTask(taskId));
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
  {
    SettableFuture<TaskPayloadResponse> future = SettableFuture.create();
    future.set(new TaskPayloadResponse(taskId, getMSQControllerTask(taskId)));
    return future;
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    SettableFuture<TaskStatusResponse> future = SettableFuture.create();
    MSQTestTaskDetails details = taskDetailsByTaskId.get(taskId);
    TaskStatus taskStatus = details.taskStatus;
    future.set(new TaskStatusResponse(taskId, new TaskStatusPlus(
        taskId,
        null,
        MSQControllerTask.TYPE,
        CREATED_TIME,
        QUEUE_INSERTION_TIME,
        taskStatus.getStatusCode(),
        null,
        DURATION,
        taskStatus.getLocation(),
        null,
        taskStatus.getErrorMsg()
    )));

    return future;
  }

  // hooks to pull stuff out for testing
  @Nullable
  public TaskReport.ReportMap getReportForTask(String id)
  {
    MSQTestTaskDetails details = taskDetailsByQueryId.get(id);
    return details.report;
  }

  @Nullable
  MSQControllerTask getMSQControllerTask(String id)
  {
    MSQTestTaskDetails details = taskDetailsByTaskId.get(id);
    return details.controllerTask;
  }

  /**
   * Listener that captures a report and makes it available through {@link #getReportMap()}.
   */
  static class TestQueryListener implements QueryListener
  {
    private final String taskId;
    private final MSQDestination destination;
    private final List<Object[]> results = new ArrayList<>();

    private List<MSQResultsReport.ColumnAndType> signature;
    private List<SqlTypeName> sqlTypeNames;
    private boolean resultsTruncated = true;
    private TaskReport.ReportMap reportMap;

    public TestQueryListener(final String taskId, final MSQDestination destination)
    {
      this.taskId = taskId;
      this.destination = destination;
    }

    @Override
    public boolean readResults()
    {
      return destination.getRowsInTaskReport() == MSQDestination.UNLIMITED || destination.getRowsInTaskReport() > 0;
    }

    @Override
    public void onResultsStart(List<MSQResultsReport.ColumnAndType> signature, @Nullable List<SqlTypeName> sqlTypeNames)
    {
      this.signature = signature;
      this.sqlTypeNames = sqlTypeNames;
    }

    @Override
    public boolean onResultRow(Object[] row)
    {
      if (destination.getRowsInTaskReport() == MSQDestination.UNLIMITED
          || results.size() < destination.getRowsInTaskReport()) {
        results.add(row);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void onResultsComplete()
    {
      resultsTruncated = false;
    }

    @Override
    public void onQueryComplete(MSQTaskReportPayload report)
    {
      final MSQResultsReport resultsReport;

      if (signature != null) {
        resultsReport = new MSQResultsReport(
            signature,
            sqlTypeNames,
            results,
            resultsTruncated
        );
      } else {
        resultsReport = null;
      }

      final MSQTaskReport taskReport = new MSQTaskReport(
          taskId,
          new MSQTaskReportPayload(
              report.getStatus(),
              report.getStages(),
              report.getCounters(),
              resultsReport
          )
      );

      reportMap = TaskReport.buildTaskReports(taskReport);
    }

    public TaskReport.ReportMap getReportMap()
    {
      return Preconditions.checkNotNull(reportMap, "reportMap");
    }

    public MSQStatusReport getStatusReport()
    {
      final MSQTaskReport taskReport = (MSQTaskReport) Iterables.getOnlyElement(getReportMap().values());
      return taskReport.getPayload().getStatus();
    }
  }

  public void closeTask(String taskId)
  {

    taskDetailsByTaskId.get(taskId).close();
  }
}
