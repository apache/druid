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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Interface for the controller of a multi-stage query.
 */
public interface Controller
{
  /**
   * POJO for capturing the status of a controller task that is currently running.
   */
  class RunningControllerStatus
  {
    private final String id;

    @JsonCreator
    public RunningControllerStatus(String id)
    {
      this.id = id;
    }

    @JsonProperty("id")
    public String getId()
    {
      return id;
    }
  }

  /**
   * Unique task/query ID for the batch query run by this controller.
   */
  String id();

  /**
   * The task which this controller runs.
   */
  MSQControllerTask task();

  /**
   * Runs the controller logic in the current thread. Surrounding classes provide the execution thread.
   */
  TaskStatus run() throws Exception;

  /**
   * Terminate the query DAG upon a cancellation request.
   */
  void stopGracefully();

  // Worker-to-controller messages

  /**
   * Provide a {@link ClusterByStatisticsSnapshot} for shuffling stages.
   */
  void updateStatus(int stageNumber, int workerNumber, Object keyStatisticsObject);

  /**
   * System error reported by a subtask. Note that the errors are organized by
   * taskId, not by query/stage/worker, because system errors are associated
   * with a task rather than a specific query/stage/worker execution context.
   */
  void workerError(MSQErrorReport errorReport);

  /**
   * System warning reported by a subtask. Indicates that the worker has encountered a non-lethal error. Worker should
   * continue its execution in such a case. If the worker wants to report an error and stop its execution,
   * please use {@link Controller#workerError}
   */
  void workerWarning(List<MSQErrorReport> errorReports);

  /**
   * Periodic update of {@link CounterSnapshots} from subtasks.
   */
  void updateCounters(CounterSnapshotsTree snapshotsTree);

  /**
   * Reports that results are ready for a subtask.
   */
  void resultsComplete(
      String queryId,
      int stageNumber,
      int workerNumber,
      Object resultObject
  );

  /**
   * Returns the current list of task ids, ordered by worker number. The Nth task has worker number N.
   */
  List<String> getTaskIds();

  @Nullable
  Map<String, TaskReport> liveReports();

}
