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

package org.apache.druid.frame.channel;

import org.apache.druid.frame.file.FrameFileFooter;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class DurableStoragePartitionedReadableFrameChannel implements PartitionedReadableFrameChannel
{
  private final StorageConnector storageConnector;
  private final Supplier<FrameFileFooter> frameFileFooterSupplier;
  private final String frameFileFullPath;
  private final ExecutorService remoteInputStreamPool;
  private final File footerFile;

  public DurableStoragePartitionedReadableFrameChannel(
      StorageConnector storageConnector,
      Supplier<FrameFileFooter> frameFileFooterSupplier,
      String frameFileFullPath,
      ExecutorService remoteInputStreamPool,
      File footerFile
  )
  {
    this.storageConnector = storageConnector;
    this.frameFileFooterSupplier = frameFileFooterSupplier;
    this.frameFileFullPath = frameFileFullPath;
    this.remoteInputStreamPool = remoteInputStreamPool;
    this.footerFile = footerFile;
  }

  @Override
  public ReadableFrameChannel getReadableFrameChannel(int partitionNumber)
  {
    FrameFileFooter frameFileFooter = frameFileFooterSupplier.get();
    // find the range to read for partition
    int startFrame = frameFileFooter.getPartitionStartFrame(partitionNumber);
    int endFrame = frameFileFooter.getPartitionStartFrame(partitionNumber + 1);
    long startByte = startFrame == 0 ? FrameFileWriter.MAGIC.length : frameFileFooter.getFrameEndPosition(startFrame - 1);
    long endByte = endFrame == 0 ? FrameFileWriter.MAGIC.length : frameFileFooter.getFrameEndPosition(endFrame - 1);

    long size = endByte - startByte;
    if (size <= 0) {
      return ReadableNilFrameChannel.INSTANCE;
    }
    try {
      return ReadableInputStreamFrameChannel.open(
          storageConnector.readRange(frameFileFullPath, startByte, endByte - startByte),
          frameFileFullPath,
          remoteInputStreamPool,
          true
      );
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close()
  {
    try {
      storageConnector.deleteFile(frameFileFullPath);
      footerFile.delete();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
