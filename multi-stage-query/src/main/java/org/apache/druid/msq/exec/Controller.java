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

import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.client.ControllerChatHandler;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.run.SqlEngine;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Interface for the controller of a multi-stage query. Each Controller is specific to a particular query.
 *
 * @see WorkerImpl the production implementation
 */
public interface Controller
{
  /**
   * Unique task/query ID for the batch query run by this controller.
   *
   * Controller IDs must be globally unique. For tasks, this is the task ID from {@link MSQControllerTask#getId()}.
   * For Dart, this is {@link QueryContexts#CTX_DART_QUERY_ID}, set by {@link SqlEngine#initContextMap(Map)}.
   */
  String queryId();

  /**
   * Runs the controller logic in the current thread. Surrounding classes provide the execution thread.
   */
  void run(QueryListener listener) throws Exception;

  /**
   * Terminate the controller upon a cancellation request. Causes a concurrently-running {@link #run} method in
   * a separate thread to cancel all outstanding work and exit.
   */
  void stop(CancellationReason reason);

  // Worker-to-controller messages

  /**
   * Accepts a {@link PartialKeyStatisticsInformation} and updates the controller key statistics information. If all key
   * statistics have been gathered, enqueues the task with the {@link WorkerSketchFetcher} to generate partiton boundaries.
   * This is intended to be called by the {@link ControllerChatHandler}.
   *
   * @see ControllerClient#postPartialKeyStatistics(StageId, int, PartialKeyStatisticsInformation)
   */
  void updatePartialKeyStatisticsInformation(
      int stageNumber,
      int workerNumber,
      Object partialKeyStatisticsInformationObject
  );

  /**
   * Sent by workers when they finish reading their input, in cases where they would not otherwise be calling
   * {@link #updatePartialKeyStatisticsInformation(int, int, Object)}.
   *
   * @see ControllerClient#postDoneReadingInput(StageId, int)
   */
  void doneReadingInput(int stageNumber, int workerNumber);

  /**
   * System error reported by a subtask. Note that the errors are organized by
   * taskId, not by query/stage/worker, because system errors are associated
   * with a task rather than a specific query/stage/worker execution context.
   *
   * @see ControllerClient#postWorkerError(MSQErrorReport)
   */
  void workerError(MSQErrorReport errorReport);

  /**
   * System warning reported by a subtask. Indicates that the worker has encountered a non-lethal error. Worker should
   * continue its execution in such a case. If the worker wants to report an error and stop its execution,
   * please use {@link Controller#workerError}
   *
   * @see ControllerClient#postWorkerWarning(List)
   */
  void workerWarning(List<MSQErrorReport> errorReports);

  /**
   * Periodic update of {@link CounterSnapshots} from subtasks.
   *
   * @see ControllerClient#postCounters(String, CounterSnapshotsTree)
   */
  void updateCounters(String taskId, CounterSnapshotsTree snapshotsTree);

  /**
   * Reports that results are ready for a subtask.
   *
   * @see ControllerClient#postResultsComplete(StageId, int, Object)
   */
  void resultsComplete(
      String queryId,
      int stageNumber,
      int workerNumber,
      Object resultObject
  );

  /**
   * Returns the current list of worker IDs, ordered by worker number. The Nth worker has worker number N.
   */
  List<String> getWorkerIds();

  /**
   * Returns whether this controller has a worker with the given ID.
   */
  boolean hasWorker(String workerId);

  @Nullable
  TaskReport.ReportMap liveReports();

  ControllerContext getControllerContext();

  QueryContext getQueryContext();
}
