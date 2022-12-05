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
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.io.IOException;
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

  public FileOutputChannelFactory(final File fileChannelsDirectory, final int frameSize)
  {
    this.fileChannelsDirectory = fileChannelsDirectory;
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    FileUtils.mkdirp(fileChannelsDirectory);

    final String fileName = StringUtils.format("part_%06d_%s", partitionNumber, UUID.randomUUID().toString());
    final File file = new File(fileChannelsDirectory, fileName);

    final WritableFrameFileChannel writableChannel =
        new WritableFrameFileChannel(
            FrameFileWriter.open(
                Files.newByteChannel(
                    file.toPath(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                ),
                ByteBuffer.allocate(Frame.compressionBufferSize(frameSize))
            )
        );

    final Supplier<ReadableFrameChannel> readableChannelSupplier = Suppliers.memoize(
        () -> {
          try {
            final FrameFile frameFile = FrameFile.open(file, FrameFile.Flag.DELETE_ON_CLOSE);
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
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
