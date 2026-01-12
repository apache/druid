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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerRunRef;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.rpc.SketchEncoding;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MSQTestWorkerClient implements WorkerClient
{
  private static final long WORKER_WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

  protected final Map<String, WorkerRunRef> inMemoryWorkers;
  private final AtomicBoolean closed = new AtomicBoolean();

  public MSQTestWorkerClient(Map<String, WorkerRunRef> inMemoryWorkers)
  {
    this.inMemoryWorkers = inMemoryWorkers;
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    getWorkerFor(workerTaskId).postWorkOrder(workOrder);
    return Futures.immediateFuture(null);
  }

  protected Worker getWorkerFor(String workerTaskId)
  {
    final WorkerRunRef workerRunRef = inMemoryWorkers.computeIfAbsent(workerTaskId, this::newWorker);
    final Stopwatch stopwatch = Stopwatch.createStarted();

    // Wait for the worker to exist
    while (!workerRunRef.hasWorker()) {
      if (stopwatch.millisElapsed() > WORKER_WAIT_TIMEOUT_MS) {
        throw new ISE(
            "Timed out after [%,d]ms waiting for worker[%s] to be registered",
            stopwatch.millisElapsed(),
            workerTaskId
        );
      }

      try {
        Thread.sleep(10);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ISE("Interrupted while waiting for worker[%s]", workerTaskId);
      }
    }

    return workerRunRef.worker();
  }

  protected WorkerRunRef newWorker(String workerId)
  {
    throw new RuntimeException("Not implemented!");
  }

  @Override
  public ListenableFuture<ClusterByStatisticsSnapshot> fetchClusterByStatisticsSnapshot(
      String workerTaskId,
      StageId stageId,
      SketchEncoding sketchEncoding
  )
  {
    return Futures.immediateFuture(getWorkerFor(workerTaskId).fetchStatisticsSnapshot(stageId));
  }

  @Override
  public ListenableFuture<ClusterByStatisticsSnapshot> fetchClusterByStatisticsSnapshotForTimeChunk(
      String workerTaskId,
      StageId stageId,
      long timeChunk,
      SketchEncoding sketchEncoding
  )
  {
    return Futures.immediateFuture(
        getWorkerFor(workerTaskId).fetchStatisticsSnapshotForTimeChunk(stageId, timeChunk)
    );
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      String workerTaskId,
      StageId stageId,
      ClusterByPartitions partitionBoundaries
  )
  {
    try {
      getWorkerFor(workerTaskId).postResultPartitionBoundaries(stageId, partitionBoundaries);
      return Futures.immediateFuture(null);
    }
    catch (Exception e) {
      throw new ISE(e, "unable to post result partition boundaries to workers");
    }
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
  {
    getWorkerFor(workerTaskId).postCleanupStage(stageId);
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> postFinish(String taskId)
  {
    getWorkerFor(taskId).postFinish();
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String taskId)
  {
    return Futures.immediateFuture(getWorkerFor(taskId).getCounters());
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
    return FutureUtils.transform(
        getWorkerFor(workerTaskId).readStageOutput(stageId, partitionNumber, offset),
        inputStream -> {
          try {
            byte[] buffer = new byte[8 * 1024];
            boolean didRead = false;
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
              channel.addChunk(Arrays.copyOf(buffer, bytesRead));
              didRead = true;
            }
            inputStream.close();
            return !didRead;
          }
          catch (Exception e) {
            throw new ISE(e, "Error reading frame file channel");
          }
        }
    );
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      inMemoryWorkers.forEach((k, v) -> v.cancel());
    }
  }
}
