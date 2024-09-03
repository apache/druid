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

package org.apache.druid.msq.shuffle.input;

import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.shuffle.output.StageOutputHolder;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * An {@link InputChannelFactory} that loads data locally when possible, and otherwise connects directly to other
 * workers. Used when durable shuffle storage is off.
 */
public class WorkerOrLocalInputChannelFactory implements InputChannelFactory
{
  private final String myId;
  private final Supplier<List<String>> workerIdsSupplier;
  private final InputChannelFactory workerInputChannelFactory;
  private final StageOutputHolderProvider stageOutputHolderProvider;

  public WorkerOrLocalInputChannelFactory(
      final String myId,
      final Supplier<List<String>> workerIdsSupplier,
      final InputChannelFactory workerInputChannelFactory,
      final StageOutputHolderProvider stageOutputHolderProvider
  )
  {
    this.myId = myId;
    this.workerIdsSupplier = workerIdsSupplier;
    this.workerInputChannelFactory = workerInputChannelFactory;
    this.stageOutputHolderProvider = stageOutputHolderProvider;
  }

  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {
    final String taskId = workerIdsSupplier.get().get(workerNumber);
    if (taskId.equals(myId)) {
      return stageOutputHolderProvider.getHolder(stageId, partitionNumber).readLocally();
    } else {
      return workerInputChannelFactory.openChannel(stageId, workerNumber, partitionNumber);
    }
  }

  public interface StageOutputHolderProvider
  {
    StageOutputHolder getHolder(StageId stageId, int partitionNumber);
  }
}
