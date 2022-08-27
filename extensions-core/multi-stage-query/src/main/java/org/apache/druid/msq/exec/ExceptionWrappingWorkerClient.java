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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Wrapper around any {@link WorkerClient} that converts exceptions into {@link MSQException}
 * with {@link WorkerRpcFailedFault}. Useful so each implementation of WorkerClient does not need to do this on
 * its own.
 */
public class ExceptionWrappingWorkerClient implements WorkerClient
{
  private final WorkerClient client;

  public ExceptionWrappingWorkerClient(final WorkerClient client)
  {
    this.client = Preconditions.checkNotNull(client, "client");
  }

  @Override
  public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
  {
    return wrap(workerTaskId, client, c -> c.postWorkOrder(workerTaskId, workOrder));
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      final String workerTaskId,
      final StageId stageId,
      final ClusterByPartitions partitionBoundaries
  )
  {
    return wrap(workerTaskId, client, c -> c.postResultPartitionBoundaries(workerTaskId, stageId, partitionBoundaries));
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
  {
    return wrap(workerTaskId, client, c -> c.postCleanupStage(workerTaskId, stageId));
  }

  @Override
  public ListenableFuture<Void> postFinish(String workerTaskId)
  {
    return wrap(workerTaskId, client, c -> c.postFinish(workerTaskId));
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(String workerTaskId)
  {
    return wrap(workerTaskId, client, c -> c.getCounters(workerTaskId));
  }

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      String workerTaskId,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  )
  {
    return wrap(workerTaskId, client, c -> c.fetchChannelData(workerTaskId, stageId, partitionNumber, offset, channel));
  }

  @Override
  public void close() throws IOException
  {
    client.close();
  }

  private static <T> ListenableFuture<T> wrap(
      final String workerTaskId,
      final WorkerClient client,
      final ClientFn<T> clientFn
  )
  {
    final SettableFuture<T> retVal = SettableFuture.create();
    final ListenableFuture<T> clientFuture;

    try {
      clientFuture = clientFn.apply(client);
    }
    catch (Exception e) {
      throw new MSQException(e, new WorkerRpcFailedFault(workerTaskId));
    }

    Futures.addCallback(
        clientFuture,
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(@Nullable T result)
          {
            retVal.set(result);
          }

          @Override
          public void onFailure(Throwable t)
          {
            retVal.setException(new MSQException(t, new WorkerRpcFailedFault(workerTaskId)));
          }
        }
    );

    return retVal;
  }

  private interface ClientFn<T>
  {
    ListenableFuture<T> apply(WorkerClient client);
  }
}
