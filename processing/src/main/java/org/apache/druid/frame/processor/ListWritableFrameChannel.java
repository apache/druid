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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link WritableFrameChannel} that collects up to {@code maxFrames} frames in a list. It is always writable. Frames
 * written beyond {@code maxFrames} are discarded and mark the channel as overflowed, so the writer can finish without
 * the channel holding more than {@code maxFrames} frames. Partition numbers are ignored.
 * <p>
 * Not thread-safe. Callers read {@link #isOverflowed()} and {@link #getFrames()} only after the writer has finished.
 */
class ListWritableFrameChannel implements WritableFrameChannel
{
  private final int maxFrames;
  private final List<Frame> frames = new ArrayList<>();
  private boolean overflowed;
  private boolean failed;
  private boolean closed;

  ListWritableFrameChannel(final int maxFrames)
  {
    this.maxFrames = maxFrames;
  }

  @Override
  public void write(final RowsAndColumns rac, final int partitionNumber)
  {
    if (closed) {
      throw DruidException.defensive("Channel is closed");
    }

    final Frame frame = rac.as(Frame.class);
    if (frame == null) {
      throw DruidException.defensive("Expected a frame, got[%s]", rac.getClass().getName());
    }

    if (frames.size() < maxFrames) {
      frames.add(frame);
    } else {
      overflowed = true;
    }
  }

  @Override
  public void fail(@Nullable final Throwable cause)
  {
    failed = true;
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return Futures.immediateFuture(null);
  }

  @Override
  public void close()
  {
    closed = true;
  }

  @Override
  public boolean isClosed()
  {
    return closed;
  }

  /**
   * Whether more than {@code maxFrames} frames were written. Only valid after a successful {@link #close()}.
   */
  public boolean isOverflowed()
  {
    checkClosedSuccessfully();
    return overflowed;
  }

  /**
   * Returns the frames written to this channel, in write order. Only valid after a successful {@link #close()}, and
   * if the channel did not overflow.
   */
  public List<Frame> getFrames()
  {
    checkClosedSuccessfully();
    if (overflowed) {
      throw DruidException.defensive("Frames are not available after the channel overflows");
    }

    return frames;
  }

  private void checkClosedSuccessfully()
  {
    if (!closed || failed) {
      throw DruidException.defensive("Channel has not closed successfully");
    }
  }
}
