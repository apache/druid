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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

public class MSQTestWorkerClient implements WorkerClient
{
  private final Map<String, Worker> inMemoryWorkers;

  public MSQTestWorkerClient(Map<String, Worker> inMemoryWorkers)
  {
    this.inMemoryWorkers = inMemoryWorkers;
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    inMemoryWorkers.get(workerTaskId).postWorkOrder(workOrder);
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    try {
      inMemoryWorkers.get(workerTaskId).postResultPartitionBoundaries(
          partitionBoundaries,
          stageId.getQueryId(),
          stageId.getStageNumber()
      );
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "unable to post result partition boundaries to workers");
    }
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
  {
    inMemoryWorkers.get(workerTaskId).postCleanupStage(stageId);
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> postFinish(String taskId)
  {
    inMemoryWorkers.get(taskId).postFinish();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String taskId)
  {
    return Futures.immediateFuture(inMemoryWorkers.get(taskId).getCounters());
  }

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      final String workerTaskId,
      final StageId stageId,
      final int partitionNumber,
      final long offset,
      final ReadableByteChunksFrameChannel channel
  )
  {
    try (InputStream inputStream = inMemoryWorkers.get(workerTaskId).readChannel(
        stageId.getQueryId(),
        stageId.getStageNumber(),
        partitionNumber,
        offset
    )) {
      byte[] buffer = new byte[8 * 1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        channel.addChunk(Arrays.copyOf(buffer, bytesRead));
      }
      inputStream.close();

      return Futures.immediateFuture(true);
    }
    catch (Exception e) {
      throw new ISE(e, "Error reading frame file channel");
    }

  }

  @Override
  public void close()
  {
    inMemoryWorkers.forEach((k, v) -> v.stopGracefully());
  }
}
