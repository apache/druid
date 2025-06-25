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

package org.apache.druid.frame.processor;

import com.google.common.base.Suppliers;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * An {@link OutputChannelFactory} that generates {@link WritableFrameFileChannel} backed by {@link FrameFileWriter}.
 */
public class FileOutputChannelFactory implements OutputChannelFactory
{
  private final File fileChannelsDirectory;
  private final int frameSize;
  private final ByteTracker byteTracker;

  public FileOutputChannelFactory(
      final File fileChannelsDirectory,
      final int frameSize,
      @Nullable final ByteTracker byteTracker
  )
  {
    this.fileChannelsDirectory = fileChannelsDirectory;
    this.frameSize = frameSize;
    this.byteTracker = byteTracker == null ? ByteTracker.unboundedTracker() : byteTracker;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    FileUtils.mkdirp(fileChannelsDirectory);

    final String fileName = StringUtils.format("part_%06d_%s", partitionNumber, UUID.randomUUID().toString());
    final File file = new File(fileChannelsDirectory, fileName);

    final WritableFrameChannel writableChannel =
            new WritableFrameFileChannel(
              FrameFileWriter.open(
                  Files.newByteChannel(
                      file.toPath(),
                      StandardOpenOption.CREATE_NEW,
                      StandardOpenOption.WRITE
                  ),
                  ByteBuffer.allocate(Frame.compressionBufferSize(frameSize)),
                  byteTracker
              )
        );

    final Supplier<ReadableFrameChannel> readableChannelSupplier = Suppliers.memoize(
        () -> {
          try {
            final FrameFile frameFile = FrameFile.open(file, byteTracker, FrameFile.Flag.DELETE_ON_CLOSE);
            return new ReadableFileFrameChannel(frameFile);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    )::get;

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        readableChannelSupplier,
        partitionNumber
    );
  }

  @Override
  public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead) throws IOException
  {
    FileUtils.mkdirp(fileChannelsDirectory);
    final File file = new File(fileChannelsDirectory, name);
    WritableFrameChannel writableFrameFileChannel =
          new WritableFrameFileChannel(
              FrameFileWriter.open(
                  Files.newByteChannel(
                      file.toPath(),
                      StandardOpenOption.CREATE_NEW,
                      StandardOpenOption.WRITE
                  ),
                  ByteBuffer.allocate(Frame.compressionBufferSize(frameSize)),
                  byteTracker
              )
        );
    Supplier<FrameFile> frameFileSupplier = Suppliers.memoize(
        () -> {
          try {
            return deleteAfterRead ? FrameFile.open(file, byteTracker, FrameFile.Flag.DELETE_ON_CLOSE)
                                   : FrameFile.open(file, byteTracker);
          }
          catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
    )::get;
    final Supplier<PartitionedReadableFrameChannel> partitionedReadableFrameChannelSupplier = Suppliers.memoize(
        () -> new PartitionedReadableFrameChannel()
        {
          @Override
          public ReadableFrameChannel getReadableFrameChannel(int partitionNumber)
          {
            FrameFile fileHandle = frameFileSupplier.get();
            fileHandle = fileHandle.newReference();
            return new ReadableFileFrameChannel(
                fileHandle,
                fileHandle.getPartitionStartFrame(partitionNumber),
                fileHandle.getPartitionStartFrame(partitionNumber + 1)
            );
          }

          @Override
          public void close() throws IOException
          {
            frameFileSupplier.get().close();
          }
        }
    )::get;

    return PartitionedOutputChannel.pair(
        writableFrameFileChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        partitionedReadableFrameChannelSupplier
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }

  @Override
  public boolean isBuffered()
  {
    return true;
  }
}
