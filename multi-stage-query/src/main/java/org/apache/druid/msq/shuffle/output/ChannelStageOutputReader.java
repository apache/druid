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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reader for the case where stage output is a generic {@link ReadableFrameChannel}.
 *
 * Because this reader returns an underlying channel directly, it must only be used when it is certain that
 * only a single consumer exists, i.e., when using output mode {@link OutputChannelMode#MEMORY}. See
 * {@link ControllerQueryKernelUtils#canUseMemoryOutput} for the code that ensures that there is only a single
 * consumer in the in-memory case.
 */
public class ChannelStageOutputReader implements StageOutputReader
{
  enum State
  {
    INIT,
    LOCAL,
    REMOTE,
    CLOSED
  }

  private final ReadableFrameChannel channel;
  private final FrameFileWriter writer;

  /**
   * Pair of chunk size + chunk InputStream.
   */
  private final Deque<byte[]> chunks = new ArrayDeque<>();

  /**
   * State of this reader.
   */
  @GuardedBy("this")
  private State state = State.INIT;

  /**
   * Position of {@link #positionWithinFirstChunk} in the first chunk of {@link #chunks}, within the overall stream.
   */
  @GuardedBy("this")
  private long cursor;

  /**
   * Offset of the first chunk in {@link #chunks} which corresponds to {@link #cursor}.
   */
  @GuardedBy("this")
  private int positionWithinFirstChunk;

  /**
   * Whether {@link FrameFileWriter#close()} is called on {@link #writer}.
   */
  @GuardedBy("this")
  private boolean didCloseWriter;

  public ChannelStageOutputReader(final ReadableFrameChannel channel)
  {
    this.channel = channel;
    this.writer = FrameFileWriter.open(new ChunkAcceptor(), null, ByteTracker.unboundedTracker());
  }

  /**
   * Returns an input stream starting at the provided offset.
   *
   * The returned {@link InputStream} is non-blocking, and is slightly buffered (up to one frame). It does not
   * necessarily contain the complete remaining dataset; this means that multiple calls to this method are necessary
   * to fetch the complete dataset.
   *
   * The provided offset must be greater than, or equal to, the offset provided to the prior call.
   *
   * This class supports either remote or local reads, but not both. Calling both this method and {@link #readLocally()}
   * on the same instance of this class is an error.
   *
   * @param offset offset into the stage output stream
   */
  @Override
  public synchronized ListenableFuture<InputStream> readRemotelyFrom(final long offset)
  {
    if (state == State.INIT) {
      state = State.REMOTE;
    } else if (state == State.LOCAL) {
      throw new ISE("Cannot read both remotely and locally");
    } else if (state == State.CLOSED) {
      throw new ISE("Closed");
    }

    if (offset < cursor) {
      return Futures.immediateFailedFuture(
          new ISE("Offset[%,d] no longer available, current cursor is[%,d]", offset, cursor));
    }

    while (chunks.isEmpty() || offset > cursor) {
      // Fetch additional chunks if needed.
      if (chunks.isEmpty()) {
        if (didCloseWriter) {
          if (offset == cursor) {
            return Futures.immediateFuture(new ByteArrayInputStream(ByteArrays.EMPTY_ARRAY));
          } else {
            throw DruidException.defensive(
                "Channel finished but cursor[%,d] does not match requested offset[%,d]",
                cursor,
                offset
            );
          }
        } else if (channel.isFinished()) {
          try {
            writer.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }

          didCloseWriter = true;
          continue;
        } else if (channel.canRead()) {
          try {
            writer.writeFrame(channel.read(), FrameFileWriter.NO_PARTITION);
          }
          catch (Exception e) {
            try {
              writer.abort();
            }
            catch (IOException e2) {
              e.addSuppressed(e2);
            }

            throw new RuntimeException(e);
          }
        } else {
          return FutureUtils.transformAsync(channel.readabilityFuture(), ignored -> readRemotelyFrom(offset));
        }
      }

      // Advance cursor to the provided offset, or the end of the current chunk, whichever is earlier.
      final byte[] chunk = chunks.peek();
      final long amountToAdvance = Math.min(offset - cursor, chunk.length - positionWithinFirstChunk);
      cursor += amountToAdvance;
      positionWithinFirstChunk += Ints.checkedCast(amountToAdvance);

      // Remove first chunk if it is no longer needed. (i.e., if the cursor is at the end of it.)
      if (positionWithinFirstChunk == chunk.length) {
        chunks.poll();
        positionWithinFirstChunk = 0;
      }
    }

    if (chunks.isEmpty() || offset != cursor) {
      throw DruidException.defensive(
          "Expected cursor[%,d] to be caught up to offset[%,d] by this point, and to have nonzero chunks",
          cursor,
          offset
      );
    }

    return Futures.immediateFuture(new ByteChunksInputStream(ImmutableList.copyOf(chunks), positionWithinFirstChunk));
  }

  /**
   * Returns the {@link ReadableFrameChannel} that backs this reader.
   *
   * Callers are responsible for closing the returned channel. Once this method is called, the caller becomes the
   * owner of the channel, and this class's {@link #close()} method will no longer close the channel.
   *
   * Only a single reader is supported. Once this method is called, it cannot be called again.
   *
   * This class supports either remote or local reads, but not both. Calling both this method and
   * {@link #readRemotelyFrom(long)} on the same instance of this class is an error.
   */
  @Override
  public synchronized ReadableFrameChannel readLocally()
  {
    if (state == State.INIT) {
      state = State.LOCAL;
      return channel;
    } else if (state == State.REMOTE) {
      throw new ISE("Cannot read both remotely and locally");
    } else if (state == State.LOCAL) {
      throw new ISE("Cannot read channel multiple times");
    } else {
      assert state == State.CLOSED;
      throw new ISE("Closed");
    }
  }

  /**
   * Closes the {@link ReadableFrameChannel} backing this reader, unless {@link #readLocally()} has been called.
   * In that case, the caller of {@link #readLocally()} is responsible for closing the channel.
   */
  @Override
  public synchronized void close()
  {
    // Call channel.close() unless readLocally() has been called. In that case, we expect the caller to close it.
    if (state != State.LOCAL) {
      state = State.CLOSED;
      channel.close();
    }
  }

  /**
   * Input stream that can have bytes appended to it, and that can have bytes acknowledged.
   */
  private class ChunkAcceptor implements WritableByteChannel
  {
    private boolean open = true;

    @Override
    public int write(final ByteBuffer src) throws IOException
    {
      if (!open) {
        throw new IOException("Closed");
      }

      final int len = src.remaining();
      if (len > 0) {
        final byte[] bytes = new byte[len];
        src.get(bytes);
        chunks.add(bytes);
      }

      return len;
    }

    @Override
    public boolean isOpen()
    {
      return open;
    }

    @Override
    public void close()
    {
      open = false;
    }
  }
}
