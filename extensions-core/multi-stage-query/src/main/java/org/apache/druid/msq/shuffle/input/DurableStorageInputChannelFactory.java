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

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableInputStreamFrameChannel;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.shuffle.output.DurableStorageOutputChannelFactory;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides input channels connected to durable storage.
 */
public abstract class DurableStorageInputChannelFactory implements InputChannelFactory
{

  private static final Logger LOG = new Logger(DurableStorageInputChannelFactory.class);

  private final StorageConnector storageConnector;
  private final ExecutorService remoteInputStreamPool;
  private final String controllerTaskId;

  public DurableStorageInputChannelFactory(
      final String controllerTaskId,
      final StorageConnector storageConnector,
      final ExecutorService remoteInputStreamPool
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.remoteInputStreamPool = Preconditions.checkNotNull(remoteInputStreamPool, "remoteInputStreamPool");
  }

  /**
   * Creates an instance that is the standard production implementation. Closeable items are registered with
   * the provided Closer.
   */
  public static DurableStorageInputChannelFactory createStandardImplementation(
      final String controllerTaskId,
      final StorageConnector storageConnector,
      final Closer closer,
      final boolean isQueryResults
  )
  {
    final String threadNameFormat =
        StringUtils.encodeForFormat(Preconditions.checkNotNull(controllerTaskId, "controllerTaskId"))
        + "-remote-fetcher-%d";
    final ExecutorService remoteInputStreamPool =
        Executors.newCachedThreadPool(Execs.makeThreadFactory(threadNameFormat));
    closer.register(remoteInputStreamPool::shutdownNow);
    if (isQueryResults) {
      return new DurableStorageQueryResultsInputChannelFactory(
          controllerTaskId,
          storageConnector,
          remoteInputStreamPool
      );
    }
    return new DurableStorageStageInputChannelFactory(controllerTaskId, storageConnector, remoteInputStreamPool);
  }


  @Override
  public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException
  {

    try {
      final String remotePartitionPath = findSuccessfulPartitionOutput(
          controllerTaskId,
          workerNumber,
          stageId.getStageNumber(),
          partitionNumber
      );
      LOG.debug(
          "Reading output of stage [%d], partition [%d] for worker [%d] from the file at path [%s]",
          stageId.getStageNumber(),
          partitionNumber,
          workerNumber,
          remotePartitionPath
      );
      if (!storageConnector.pathExists(remotePartitionPath)) {
        throw new ISE(
            "Could not find remote outputs of stage [%d] partition [%d] for worker [%d] at the path [%s]",
            stageId.getStageNumber(),
            partitionNumber,
            workerNumber,
            remotePartitionPath
        );
      }
      final InputStream inputStream = storageConnector.read(remotePartitionPath);

      return ReadableInputStreamFrameChannel.open(
          inputStream,
          remotePartitionPath,
          remoteInputStreamPool,
          false
      );
    }
    catch (Exception e) {
      throw new IOE(
          e,
          "Encountered error while reading the output of stage [%d], partition [%d] for worker [%d]",
          stageId.getStageNumber(),
          partitionNumber,
          workerNumber
      );
    }
  }

  /**
   * Given an input worker number, stage number and the partition number, this method figures out the exact location
   * where the outputs would be present in the durable storage and returns the complete path or throws an exception
   * if no such file exists in the durable storage
   * More information at {@link DurableStorageOutputChannelFactory#createSuccessFile(String)}
   */
  public String findSuccessfulPartitionOutput(
      final String controllerTaskId,
      final int workerNo,
      final int stageNumber,
      final int partitionNumber
  ) throws IOException
  {
    String successfulFilePath = getWorkerOutputSuccessFilePath(controllerTaskId, stageNumber, workerNo);

    if (!storageConnector.pathExists(successfulFilePath)) {
      throw new ISE(
          "No file present at the location [%s]. Unable to read the outputs of stage [%d], partition [%d] for the worker [%d]",
          successfulFilePath,
          stageNumber,
          partitionNumber,
          workerNo
      );
    }

    String successfulTaskId;

    try (InputStream is = storageConnector.read(successfulFilePath)) {
      successfulTaskId = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    if (successfulTaskId == null) {
      throw new ISE("Unable to read the task id from the file: [%s]", successfulFilePath);
    }
    LOG.debug(
        "Reading output of stage [%d], partition [%d] from task id [%s]",
        stageNumber,
        partitionNumber,
        successfulTaskId
    );

    return getPartitionOutputsFileNameWithPathForPartition(
        controllerTaskId,
        stageNumber,
        workerNo,
        partitionNumber,
        successfulTaskId
    );
  }

  /**
   * Get the filePath with filename for the partitioned output of the controller, stage, worker
   */
  public abstract String getPartitionOutputsFileNameWithPathForPartition(
      String controllerTaskId,
      int stageNumber,
      int workerNo,
      int partitionNumber,
      String successfulTaskId
  );

  /**
   * Get the filepath for the success file .
   */

  public abstract String getWorkerOutputSuccessFilePath(String controllerTaskId, int stageNumber, int workerNumber);

}
