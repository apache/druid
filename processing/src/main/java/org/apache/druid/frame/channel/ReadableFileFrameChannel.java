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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.java.util.common.IAE;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Channel backed by a {@link FrameFile}.
 *
 * This class is backed by an actual file that is on disk and memory-mapped. For situations where a stream in frame
 * file format is being read over the network, use {@link ReadableByteChunksFrameChannel} or
 * {@link org.apache.druid.frame.file.FrameFileHttpResponseHandler} instead.
 */
public class ReadableFileFrameChannel implements ReadableFrameChannel
{
  private final FrameFile frameFile;
  private final int endFrame;
  private int currentFrame;

  public ReadableFileFrameChannel(final FrameFile frameFile, final int startFrame, final int endFrame)
  {
    this.frameFile = frameFile;
    this.currentFrame = startFrame;
    this.endFrame = endFrame;

    if (startFrame < 0) {
      throw new IAE("startFrame[%,d] < 0", startFrame);
    }

    if (startFrame > endFrame) {
      throw new IAE("startFrame[%,d] > endFrame[%,d]", startFrame, endFrame);
    }

    if (endFrame > frameFile.numFrames()) {
      throw new IAE("endFrame[%,d] > numFrames[%,d]", endFrame, frameFile.numFrames());
    }
  }

  public ReadableFileFrameChannel(final FrameFile frameFile)
  {
    this(frameFile, 0, frameFile.numFrames());
  }

  @Override
  public boolean isFinished()
  {
    return currentFrame == endFrame;
  }

  @Override
  public boolean canRead()
  {
    return !isFinished();
  }

  @Override
  public Frame read()
  {
    if (isFinished()) {
      throw new NoSuchElementException();
    }

    return frameFile.frame(currentFrame++);
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return Futures.immediateFuture(true);
  }

  @Override
  public void close()
  {
    try {
      frameFile.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a new reference to the {@link FrameFile} that this channel is reading from. Callers should close this
   * reference when done reading.
   */
  public FrameFile newFrameFileReference()
  {
    return frameFile.newReference();
  }
}
