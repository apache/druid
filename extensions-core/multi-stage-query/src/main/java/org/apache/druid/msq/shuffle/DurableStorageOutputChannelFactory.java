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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Channels;

public class DurableStorageOutputChannelFactory implements OutputChannelFactory
{

  private static final Logger LOG = new Logger(DurableStorageOutputChannelFactory.class);

  public static final String SUCCESS_MARKER_FILENAME = "__success";

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
    final String fileName = getPartitionOutputsFileNameForPartition2(
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
    final String fileName = getPartitionOutputsFileNameForPartition2(
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
          "Unable to create empty remote output of workerTask[%s] stage[%d] partition[%d]",
          workerNumber,
          stageNumber,
          partitionNumber
      );
    }
  }

  /**
   * Creates another file with __success appended at the end which signifies that the output has been written succesfully
   * to the original file. Rename operation is not very quick in cloud storage like S3 due to which this alternative
   * route has been taken
   */
  public void createSuccessFile(String taskId) throws IOException
  {
    String fileName = getSuccessFilePath(controllerTaskId, stageNumber, workerNumber);
    if (storageConnector.pathExists(fileName)) {
      LOG.warn("Path [%s] already exists. Won't attempt to rewrite on top of it.", fileName);
      return;
    }
    PrintStream stream = new PrintStream(storageConnector.write(fileName));
    stream.print(taskId); // Add some dummy content in the file
    stream.close();
  }

  public static String getControllerDirectory(final String controllerTaskId)
  {
    return StringUtils.format("controller_%s", IdUtils.validateId("controller task ID", controllerTaskId));
  }

  public static String getSuccessFilePath(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber
  )
  {
    String folderName = getWorkerOutputFolderName2(
        controllerTaskId,
        stageNumber,
        workerNumber
    );
    String fileName = StringUtils.format("%s/%s", folderName, SUCCESS_MARKER_FILENAME);
    return fileName;
  }

  public static String getWorkerOutputFolderName2(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber
  )
  {
    return StringUtils.format(
        "%s/stage_%d/worker_%d",
        getControllerDirectory(controllerTaskId),
        stageNumber,
        workerNumber
    );
  }

  public static String getTaskIdOutputsFolderName2(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber,
      final String taskId
  )
  {
    return StringUtils.format(
        "%s/taskId_%s",
        getWorkerOutputFolderName2(controllerTaskId, stageNumber, workerNumber),
        taskId
    );
  }

  public static String getPartitionOutputsFileNameForPartition2(
      final String controllerTaskId,
      final int stageNumber,
      final int workerNumber,
      final String taskId,
      final int partitionNumber
  )
  {
    return StringUtils.format(
        "%s/part_%d",
        getTaskIdOutputsFolderName2(controllerTaskId, stageNumber, workerNumber, taskId),
        partitionNumber
    );
  }
}
