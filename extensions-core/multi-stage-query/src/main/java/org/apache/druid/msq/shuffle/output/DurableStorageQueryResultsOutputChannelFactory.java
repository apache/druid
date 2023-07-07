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

package org.apache.druid.msq.shuffle.output;

import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;

/**
 * Used to write out select query results to durable storage.
 */
public class DurableStorageQueryResultsOutputChannelFactory extends DurableStorageOutputChannelFactory
{

  public DurableStorageQueryResultsOutputChannelFactory(
      String controllerTaskId,
      int workerNumber,
      int stageNumber,
      String taskId,
      int frameSize,
      StorageConnector storageConnector,
      File tmpDir
  )
  {
    super(controllerTaskId, workerNumber, stageNumber, taskId, frameSize, storageConnector, tmpDir);
  }

  @Override
  public String getSuccessFilePath()
  {
    return DurableStorageUtils.getQueryResultsSuccessFilePath(
        controllerTaskId,
        stageNumber,
        workerNumber
    );
  }

  @Override
  protected String getFileNameWithPathForPartition(int partitionNumber)
  {
    return DurableStorageUtils.getQueryResultsFileNameWithPathForPartition(controllerTaskId,
                                                                           stageNumber,
                                                                           workerNumber,
                                                                           taskId,
                                                                           partitionNumber);
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final String fileName = getFileNameWithPathForPartition(partitionNumber);

    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(storageConnector.write(fileName)),
                null,
                ByteTracker.unboundedTracker()
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> ReadableNilFrameChannel.INSTANCE,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
  {
    throw new UOE("%s does not support this call", DurableStorageQueryResultsOutputChannelFactory.class.getSimpleName());
  }

}
