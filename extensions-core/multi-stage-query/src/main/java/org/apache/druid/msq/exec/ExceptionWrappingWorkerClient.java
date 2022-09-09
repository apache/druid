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
  public ListenableFuture<Void> postWorkOrder(int workerNumber, WorkOrder workOrder)
  {
    return wrap(workerNumber, client, c -> c.postWorkOrder(workerNumber, workOrder));
  }

  @Override
  public ListenableFuture<Void> postResultPartitionBoundaries(
      final int workerNumber,
      final StageId stageId,
      final ClusterByPartitions partitionBoundaries
  )
  {
    return wrap(workerNumber, client, c -> c.postResultPartitionBoundaries(workerNumber, stageId, partitionBoundaries));
  }

  @Override
  public ListenableFuture<Void> postCleanupStage(int workerNumber, StageId stageId)
  {
    return wrap(workerNumber, client, c -> c.postCleanupStage(workerNumber, stageId));
  }

  @Override
  public ListenableFuture<Void> postFinish(int workerNumber)
  {
    return wrap(workerNumber, client, c -> c.postFinish(workerNumber));
  }

  @Override
  public ListenableFuture<CounterSnapshotsTree> getCounters(int workerNumber)
  {
    return wrap(workerNumber, client, c -> c.getCounters(workerNumber));
  }

  @Override
  public ListenableFuture<Boolean> fetchChannelData(
      int workerNumber,
      StageId stageId,
      int partitionNumber,
      long offset,
      ReadableByteChunksFrameChannel channel
  )
  {
    return wrap(workerNumber, client, c -> c.fetchChannelData(workerNumber, stageId, partitionNumber, offset, channel));
  }

  @Override
  public void close() throws IOException
  {
    client.close();
  }

  private static <T> ListenableFuture<T> wrap(
      final int workerNumber,
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
      throw new MSQException(e, new WorkerRpcFailedFault(String.valueOf(workerNumber)));
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
            retVal.setException(new MSQException(t, new WorkerRpcFailedFault(String.valueOf(workerNumber))));
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
