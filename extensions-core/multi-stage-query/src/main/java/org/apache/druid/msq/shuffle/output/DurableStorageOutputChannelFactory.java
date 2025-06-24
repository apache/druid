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

import com.google.common.base.Preconditions;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class DurableStorageOutputChannelFactory implements OutputChannelFactory
{
  private static final Logger LOG = new Logger(DurableStorageOutputChannelFactory.class);
  protected final String controllerTaskId;
  protected final int workerNumber;
  protected final int stageNumber;
  protected final String taskId;
  protected final int frameSize;
  protected final StorageConnector storageConnector;
  protected final File tmpDir;
  protected final ExecutorService remoteInputStreamPool;

  public DurableStorageOutputChannelFactory(
      final String controllerTaskId,
      final int workerNumber,
      final int stageNumber,
      final String taskId,
      final int frameSize,
      final StorageConnector storageConnector,
      final File tmpDir
  )
  {
    this.controllerTaskId = Preconditions.checkNotNull(controllerTaskId, "controllerTaskId");
    this.workerNumber = workerNumber;
    this.stageNumber = stageNumber;
    this.taskId = taskId;
    this.frameSize = frameSize;
    this.storageConnector = Preconditions.checkNotNull(storageConnector, "storageConnector");
    this.tmpDir = Preconditions.checkNotNull(tmpDir, "tmpDir is null");
    this.remoteInputStreamPool =
        Executors.newCachedThreadPool(Execs.makeThreadFactory("-remote-fetcher-%d"));
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
      final StorageConnector storageConnector,
      final File tmpDir,
      final boolean isQueryResults
  )
  {
    if (isQueryResults) {
      return new DurableStorageQueryResultsOutputChannelFactory(
          controllerTaskId,
          workerNumber,
          stageNumber,
          taskId,
          frameSize,
          storageConnector,
          tmpDir
      );
    } else {
      return new DurableStorageTaskOutputChannelFactory(
          controllerTaskId,
          workerNumber,
          stageNumber,
          taskId,
          frameSize,
          storageConnector,
          tmpDir
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
    String fileName = getSuccessFilePath();
    if (storageConnector.pathExists(fileName)) {
      LOG.warn("Path [%s] already exists. Won't attempt to rewrite on top of it.", fileName);
      return;
    }
    OutputStreamWriter os = new OutputStreamWriter(storageConnector.write(fileName), StandardCharsets.UTF_8);
    os.write(taskId); // Add some dummy content in the file
    os.close();
  }

  /**
   * Get filepath to write success file in.
   */
  public abstract String getSuccessFilePath();

  @Override
  public OutputChannel openNilChannel(int partitionNumber)
  {
    final String fileName = getFileNameWithPathForPartition(partitionNumber);
    // As tasks dependent on output of this partition will forever block if no file is present in RemoteStorage. Hence, writing a dummy frame.
    try {
      FrameFileWriter.open(Channels.newChannel(storageConnector.write(fileName)), null, ByteTracker.unboundedTracker())
                     .close();
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

  @Override
  public boolean isBuffered()
  {
    return true;
  }

  /**
   * Get fileName with path for partition
   */
  protected abstract String getFileNameWithPathForPartition(int partitionNumber);
}
