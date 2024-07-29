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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.rpc.WorkerResource;
import org.apache.druid.utils.CloseableUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.io.InputStream;

/**
 * Container for a {@link StageOutputReader}, which is used to read the output of a stage.
 */
public class StageOutputHolder implements Closeable
{
  private final SettableFuture<ReadableFrameChannel> channelFuture;
  private final ListenableFuture<StageOutputReader> readerFuture;

  public StageOutputHolder()
  {
    this.channelFuture = SettableFuture.create();
    this.readerFuture = FutureUtils.transform(channelFuture, StageOutputHolder::createReader);
  }

  /**
   * Method for remote reads.
   *
   * Provides the implementation for {@link Worker#readStageOutput(StageId, int, long)}, which is in turn used by
   * {@link WorkerResource#httpGetChannelData(String, int, int, long, HttpServletRequest)}.
   *
   * @see StageOutputReader#readRemotelyFrom(long) for details on behavior
   */
  public ListenableFuture<InputStream> readRemotelyFrom(final long offset)
  {
    return FutureUtils.transformAsync(readerFuture, reader -> reader.readRemotelyFrom(offset));
  }

  /**
   * Method for local reads.
   *
   * Used instead of {@link #readRemotelyFrom(long)} when a worker is reading a channel from itself, to avoid needless
   * HTTP calls to itself.
   *
   * @see StageOutputReader#readLocally() for details on behavior
   */
  public ReadableFrameChannel readLocally()
  {
    return new FutureReadableFrameChannel(FutureUtils.transform(readerFuture, StageOutputReader::readLocally));
  }

  /**
   * Sets the channel that backs {@link #readLocally()} and {@link #readRemotelyFrom(long)}.
   */
  public void setChannel(final ReadableFrameChannel channel)
  {
    if (!channelFuture.set(channel)) {
      if (FutureUtils.getUncheckedImmediately(channelFuture) == null) {
        throw new ISE("Closed");
      } else {
        throw new ISE("Channel already set");
      }
    }
  }

  @Override
  public void close()
  {
    channelFuture.set(null);

    final StageOutputReader reader;

    try {
      reader = FutureUtils.getUnchecked(readerFuture, true);
    }
    catch (Throwable e) {
      // Error creating the reader, nothing to close. Suppress.
      return;
    }

    if (reader != null) {
      CloseableUtils.closeAndWrapExceptions(reader);
    }
  }

  private static StageOutputReader createReader(final ReadableFrameChannel channel)
  {
    if (channel == null) {
      // Happens if close() was called before the channel resolved.
      throw new ISE("Closed");
    }

    if (channel instanceof ReadableNilFrameChannel) {
      return NilStageOutputReader.INSTANCE;
    }

    if (channel instanceof ReadableFileFrameChannel) {
      // Optimized implementation when reading an entire file.
      final ReadableFileFrameChannel fileChannel = (ReadableFileFrameChannel) channel;

      if (fileChannel.isEntireFile()) {
        final FrameFile frameFile = fileChannel.newFrameFileReference();

        // Close original channel, so we don't leak a frame file reference.
        channel.close();

        return new FileStageOutputReader(frameFile);
      }
    }

    // Generic implementation for any other type of channel.
    return new ChannelStageOutputReader(channel);
  }
}
