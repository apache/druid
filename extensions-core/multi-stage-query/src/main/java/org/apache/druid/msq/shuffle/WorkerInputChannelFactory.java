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

package org.apache.druid.msq.shuffle;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.ReadableByteChunksFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.kernel.StageId;

import java.util.List;
import java.util.function.Supplier;

/**
 * Provides input channels connected to workers via {@link WorkerClient#fetchChannelData}.
 */
public class WorkerInputChannelFactory implements InputChannelFactory
{
  private final WorkerClient workerClient;
  private final Supplier<List<String>> taskList;

  public WorkerInputChannelFactory(final WorkerClient workerClient, final Supplier<List<String>> taskList)
  {
    this.workerClient = Preconditions.checkNotNull(workerClient, "workerClient");
    this.taskList = Preconditions.checkNotNull(taskList, "taskList");
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber)
  {
    final String taskId = taskList.get().get(workerNumber);
    final ReadableByteChunksFrameChannel channel =
        ReadableByteChunksFrameChannel.create(makeChannelId(taskId, stageId, partitionNumber));
    fetch(taskId, stageId, partitionNumber, 0, channel);
    return channel;
  }

  /**
   * Start a fetch chain for a particular channel starting at a particular offset.
   */
  private void fetch(
      final String taskId,
      final StageId stageId,
      final int partitionNumber,
      final long offset,
      final ReadableByteChunksFrameChannel channel
  )
  {
    final ListenableFuture<Boolean> fetchFuture =
        workerClient.fetchChannelData(taskId, stageId, partitionNumber, offset, channel);

    Futures.addCallback(
        fetchFuture,
        new FutureCallback<Boolean>()
        {
          @Override
          public void onSuccess(final Boolean lastFetch)
          {
            if (lastFetch) {
              channel.doneWriting();
            } else {
              fetch(taskId, stageId, partitionNumber, channel.getBytesAdded(), channel);
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            channel.setError(t);
          }
        }
    );
  }

  private static String makeChannelId(final String workerTaskId, final StageId stageId, final int partitionNumber)
  {
    return StringUtils.format("%s:%s:%s", workerTaskId, stageId, partitionNumber);
  }
}
