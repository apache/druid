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
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MappedByteBufferHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.function.Supplier;

/**
 * Used to write out intermediate task output files to durable storage. To write final stage output files for select queries use
 * {@link DurableStorageQueryResultsOutputChannelFactory}
 */
public class DurableStorageTaskOutputChannelFactory
    extends DurableStorageOutputChannelFactory
{
  public DurableStorageTaskOutputChannelFactory(
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
    return DurableStorageUtils.getWorkerOutputSuccessFilePath(controllerTaskId, stageNumber, workerNumber);
  }

  @Override
  protected String getFileNameWithPathForPartition(int partitionNumber)
  {
    return DurableStorageUtils.getPartitionOutputsFileNameWithPathForPartition(
        controllerTaskId,
        stageNumber,
        workerNumber,
        taskId,
        partitionNumber
    );
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

}
