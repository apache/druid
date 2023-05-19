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
import com.google.common.base.Suppliers;
import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.DurableStoragePartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableInputStreamFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFileFooter;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class DurableStorageOutputChannelFactory implements OutputChannelFactory
{
  private static final Logger LOG = new Logger(DurableStorageOutputChannelFactory.class);

  private final String controllerTaskId;
  private final int workerNumber;
  private final int stageNumber;
  private final String taskId;
  private final int frameSize;
  private final StorageConnector storageConnector;
  private final File tmpDir;
  private final ExecutorService remoteInputStreamPool;

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
      final File tmpDir
  )
  {
    return new DurableStorageOutputChannelFactory(
        controllerTaskId,
        workerNumber,
        stageNumber,
        taskId,
        frameSize,
        storageConnector,
        tmpDir
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
                null,
                ByteTracker.unboundedTracker()
            )
        );

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> {
          try {
            if (!storageConnector.pathExists(fileName)) {
              throw new ISE("File does not exist : %s", fileName);
            }
          }
          catch (Exception exception) {
            throw new RuntimeException(exception);
          }
          try {
            return ReadableInputStreamFrameChannel.open(
                storageConnector.read(fileName),
                fileName,
                remoteInputStreamPool,
                false
            );
          }
          catch (IOException e) {
            throw new UncheckedIOException(StringUtils.format("Unable to read file : %s", fileName), e);
          }
        },
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead) throws IOException
  {
    final String fileName = DurableStorageUtils.getOutputsFileNameForPath(
        controllerTaskId,
        stageNumber,
        workerNumber,
        taskId,
        name
    );
    final CountingOutputStream countingOutputStream = new CountingOutputStream(storageConnector.write(fileName));
    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Channels.newChannel(countingOutputStream),
                ByteBuffer.allocate(Frame.compressionBufferSize(frameSize)),
                ByteTracker.unboundedTracker()
            )
        );

    final Supplier<Long> channelSizeSupplier = countingOutputStream::getCount;

    final File footerFile = new File(tmpDir, fileName + "_footer");
    // build supplier for reading the footer of the underlying frame file
    final Supplier<FrameFileFooter> frameFileFooterSupplier = Suppliers.memoize(() -> {
      try {
        // read trailer and find the footer size
        byte[] trailerBytes = new byte[FrameFileWriter.TRAILER_LENGTH];
        long channelSize = channelSizeSupplier.get();
        try (InputStream reader = storageConnector.readRange(
            fileName,
            channelSize - FrameFileWriter.TRAILER_LENGTH,
            FrameFileWriter.TRAILER_LENGTH
        )) {
          int bytesRead = reader.read(trailerBytes, 0, trailerBytes.length);
          if (bytesRead != FrameFileWriter.TRAILER_LENGTH) {
            throw new RuntimeException("Invalid frame file trailer for object : " + fileName);
          }
        }

        Memory trailer = Memory.wrap(trailerBytes);
        int footerLength = trailer.getInt(Integer.BYTES * 2L);

        // read the footer into a file and map it to memory
        FileUtils.mkdirp(footerFile.getParentFile());
        Preconditions.checkState(footerFile.createNewFile(), "Unable to create local footer file");
        try (FileOutputStream footerFileStream = new FileOutputStream(footerFile);
            InputStream footerInputStream =
                storageConnector.readRange(fileName, channelSize - footerLength, footerLength)) {
          IOUtils.copy(footerInputStream, footerFileStream);
        }
        MappedByteBufferHandler mapHandle = FileUtils.map(footerFile);
        Memory footerMemory = Memory.wrap(mapHandle.get(), ByteOrder.LITTLE_ENDIAN);

        // create a frame file footer from the mapper memory
        return new FrameFileFooter(footerMemory, channelSize);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    })::get;

    return PartitionedOutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        () -> new DurableStoragePartitionedReadableFrameChannel(
            storageConnector,
            frameFileFooterSupplier,
            fileName,
            remoteInputStreamPool,
            footerFile
        )
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

      FrameFileWriter.open(Channels.newChannel(storageConnector.write(fileName)), null, ByteTracker.unboundedTracker()).close();
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
