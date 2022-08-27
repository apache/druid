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
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import java.io.IOException;

/**
 * Client for multi-stage query workers. Used by the controller task.
 */
public interface WorkerClient extends AutoCloseable
{
  /**
   * Worker's client method to add a {@link WorkOrder} to the worker to work on
   */
  ListenableFuture<Void> postWorkOrder(String workerId, WorkOrder workOrder);

  /**
   * Worker's client method to inform it of the partition boundaries for the given stage. This is usually invoked by the
   * controller after collating the result statistics from all the workers processing the query
   */
  ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  );

  /**
   * Worker's client method to inform that the work has been done, and it can initiate cleanup and shutdown
   */
  ListenableFuture<Void> postFinish(String workerId);

  /**
   * Fetches all the counters gathered by that worker
   */
  ListenableFuture<CounterSnapshotsTree> getCounters(String workerId);

  /**
   * Worker's client method that informs it that the results and resources for the given stage are no longer required
   * and that they can be cleaned up
   */
  ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId);

  /**
   * Fetch some data from a worker and add it to the provided channel. The exact amount of data is determined
   * by the server.
   *
   * Returns a future that resolves to true (no more data left), false (there is more data left), or exception (some
   * kind of unrecoverable exception).
   */
  ListenableFuture<Boolean> fetchChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  );

  @Override
  void close() throws IOException;
}
