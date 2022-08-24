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

import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

public interface Worker
{
  /**
   * Unique ID for this worker.
   */
  String id();

  /**
   * The task which this worker runs.
   */
  MSQWorkerTask task();

  /**
   * Runs the worker in the current thread. Surrounding classes provide
   * the execution thread.
   */
  TaskStatus run() throws Exception;

  /**
   * Terminate the worker upon a cancellation request.
   */
  void stopGracefully();

  /**
   * Report that the controller has failed. The worker must cease work immediately. Cleanup then exit.
   * Do not send final messages to the controller: there will be no one home at the other end.
   */
  void controllerFailed();

  // Controller-to-worker, and worker-to-worker messages

  /**
   * Called when the worker chat handler receives a request for a work order. Accepts the work order and schedules it for
   * execution
   */
  void postWorkOrder(WorkOrder workOrder);

  /**
   * Called when the worker chat handler recieves the result partition boundaries for a particular stageNumber
   * and queryId
   */
  boolean postResultPartitionBoundaries(
      ClusterByPartitions stagePartitionBoundaries,
      String queryId,
      int stageNumber
  );

  /**
   * Returns an InputStream of the worker output for a particular queryId, stageNumber and partitionNumber.
   * Offset indicates the number of bytes to skip the channel data, and is used to prevent re-reading the same data
   * during retry in case of a connection error
   *
   * Returns a null if the workerOutput for a particular queryId, stageNumber, and partitionNumber is not found.
   *
   * @throws IOException when the worker output is found but there is an error while reading it.
   */
  @Nullable
  InputStream readChannel(String queryId, int stageNumber, int partitionNumber, long offset) throws IOException;

  /**
   * Returns the snapshot of the worker counters
   */
  CounterSnapshotsTree getCounters();

  /**
   * Called when the worker receives a POST request to clean up the stage with stageId, and is no longer required.
   * This marks the stage as FINISHED in its stage kernel, cleans up the worker output for the stage and optionally
   * frees any resources held on by the worker for the particular stage
   */
  void postCleanupStage(StageId stageId);

  /**
   * Called when the work required for the query has been finished
   */
  void postFinish();
}
