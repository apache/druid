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
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public class DurableStorageOutputChannelFactory implements OutputChannelFactory
{

  private static final Logger LOG = new Logger(DurableStorageOutputChannelFactory.class);

  private final String controllerTaskId;
  private final int workerNumber;
  private final int stageNumber;
  private final String taskId;
  private final int frameSize;
  private final StorageConnector storageConnector;

  public DurableStorageOutputChannelFactory(
      final String controllerTaskId,
      final int workerNumber,
      final int stageNumber,
      final String taskId,
      final int frameSize,
      final StorageConnector storageConnector
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.workerNumber = workerNumber;
    this.stageNumber = stageNumber;
    this.taskId = taskId;
    this.frameSize = frameSize;
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
  }

  /**
   * Creates an instance that is the standard production implementation. Closeable items are registered with
   * the provided Closer.
   */
  public static DurableStorageOutputChannelFactory createStandardImplementation(
      final String controllerTaskId,
      final int workerNumber,
      final int stageNumber,
      final String taskId,
      final int frameSize,
      final StorageConnector storageConnector
  )
  {
    return new DurableStorageOutputChannelFactory(
        controllerTaskId,
        workerNumber,
        stageNumber,
        taskId,
        frameSize,
        storageConnector
    );
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    final String fileName = DurableStorageUtils.getPartitionOutputsFileNameForPartition(
        controllerTaskId,
        stageNumber,
        workerNumber,
        taskId,
        partitionNumber
    );
    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(storageConnector.write(fileName)),
                null
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> ReadableNilFrameChannel.INSTANCE, // remote reads should happen via the DurableStorageInputChannelFactory
        partitionNumber
    );
  }

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    final String fileName = DurableStorageUtils.getPartitionOutputsFileNameForPartition(
        controllerTaskId,
        stageNumber,
        workerNumber,
        taskId,
        partitionNumber
    );
    // As tasks dependent on output of this partition will forever block if no file is present in RemoteStorage. Hence, writing a dummy frame.
    try {

      FrameFileWriter.open(Channels.newChannel(storageConnector.write(fileName)), null).close();
      return OutputChannel.nil(partitionNumber);
    }
    catch (IOException e) {
      throw new ISE(
          e,
          "Unable to create empty remote output of stage [%d], partition [%d] for worker [%d]",
          stageNumber,
          partitionNumber,
          workerNumber
      );
    }
  }

  /**
   * Creates a file with name __success and adds the worker's id which has successfully written its outputs. While reading
   * this file can be used to find out the worker which has written its outputs completely.
   * Rename operation is not very quick in cloud storage like S3 due to which this alternative
   * route has been taken.
   * If the success file is already present in the location, then this method is a noop
   */
  public void createSuccessFile(String taskId) throws IOException
  {
    String fileName = DurableStorageUtils.getSuccessFilePath(controllerTaskId, stageNumber, workerNumber);
    if (storageConnector.pathExists(fileName)) {
      LOG.warn("Path [%s] already exists. Won't attempt to rewrite on top of it.", fileName);
      return;
    }
    OutputStreamWriter os = new OutputStreamWriter(storageConnector.write(fileName), StandardCharsets.UTF_8);
    os.write(taskId); // Add some dummy content in the file
    os.close();
  }
}
