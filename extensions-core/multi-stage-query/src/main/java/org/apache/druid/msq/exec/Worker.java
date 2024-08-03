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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import java.io.InputStream;

/**
 * Interface for a multi-stage query (MSQ) worker. Workers are long-lived and are able to run multiple {@link WorkOrder}
 * prior to exiting.
 *
 * @see WorkerImpl the production implementation
 */
public interface Worker
{
  /**
   * Identifier for this worker. Same as {@link WorkerContext#workerId()}.
   */
  String id();

  /**
   * Runs the worker in the current thread. Surrounding classes provide the execution thread.
   */
  void run();

  /**
   * Terminate the worker upon a cancellation request. Causes a concurrently-running {@link #run()} method in
   * a separate thread to cancel all outstanding work and exit. Does not block. Use {@link #awaitStop()} if you
   * would like to wait for {@link #run()} to finish.
   */
  void stop();

  /**
   * Wait for {@link #run()} to finish.
   */
  void awaitStop();

  /**
   * Report that the controller has failed. The worker must cease work immediately. Cleanup then exit.
   * Do not send final messages to the controller: there will be no one home at the other end.
   */
  void controllerFailed();

  // Controller-to-worker, and worker-to-worker messages

  /**
   * Called when the worker receives a new work order. Accepts the work order and schedules it for execution.
   */
  void postWorkOrder(WorkOrder workOrder);

  /**
   * Returns the statistics snapshot for the given stageId. This is called from {@link WorkerSketchFetcher} under
   * {@link ClusterStatisticsMergeMode#PARALLEL} OR {@link ClusterStatisticsMergeMode#AUTO} modes.
   */
  ClusterByStatisticsSnapshot fetchStatisticsSnapshot(StageId stageId);

  /**
   * Returns the statistics snapshot for the given stageId which contains only the sketch for the specified timeChunk.
   * This is called from {@link WorkerSketchFetcher} under {@link ClusterStatisticsMergeMode#SEQUENTIAL} or
   * {@link ClusterStatisticsMergeMode#AUTO} modes.
   */
  ClusterByStatisticsSnapshot fetchStatisticsSnapshotForTimeChunk(StageId stageId, long timeChunk);

  /**
   * Called when the worker chat handler recieves the result partition boundaries for a particular stageNumber
   * and queryId
   */
  boolean postResultPartitionBoundaries(StageId stageId, ClusterByPartitions stagePartitionBoundaries);

  /**
   * Returns an InputStream of the worker output for a particular queryId, stageNumber and partitionNumber.
   * Offset indicates the number of bytes to skip the channel data, and is used to prevent re-reading the same data
   * during retry in case of a connection error.
   *
   * The returned future resolves when at least one byte of data is available, or when the channel is finished.
   * If the channel is finished, an empty {@link InputStream} is returned.
   *
   * With {@link OutputChannelMode#MEMORY}, once this method is called with a certain offset, workers are free to
   * delete data prior to that offset. (Already-requested offsets will not be re-requested, because
   * {@link OutputChannelMode#MEMORY} requires a single reader.) In this mode, if an already-requested offset is
   * re-requested for some reason, an error future is returned.
   *
   * The returned future resolves to null if stage output for a particular queryId, stageNumber, and
   * partitionNumber is not found.
   *
   * Throws an exception when worker output is found, but there is an error while reading it.
   */
  ListenableFuture<InputStream> readStageOutput(StageId stageId, int partitionNumber, long offset);

  /**
   * Returns a snapshot of counters.
   */
  CounterSnapshotsTree getCounters();

  /**
   * Called when the worker receives a POST request to clean up the stage with stageId, and is no longer required.
   * This marks the stage as FINISHED in its stage kernel, cleans up the worker output for the stage and optionally
   * frees any resources held on by the worker for the particular stage
   */
  void postCleanupStage(StageId stageId);

  /**
   * Called when the worker is no longer needed, and should shut down.
   */
  void postFinish();
}
