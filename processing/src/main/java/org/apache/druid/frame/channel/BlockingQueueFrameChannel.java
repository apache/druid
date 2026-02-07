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
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.objects.ObjectIntPair;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * In-memory channel backed by a limited-capacity {@link java.util.Deque}.
 *
 * Instances of this class provide a {@link ReadableFrameChannel} through {@link #readable()}, and a
 * {@link WritableFrameChannel} through {@link #writable()}. Instances of this class are used by a single writer
 * and single reader. The writer and reader may run concurrently.
 */
public class BlockingQueueFrameChannel
{
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static final Optional<Either<Throwable, ObjectIntPair<RowsAndColumns>>> END_MARKER = Optional.empty();

  private final int maxQueuedFrames;
  private final Object lock = new Object();

  private final Writable writable;
  private final Readable readable;

  /**
   * Queue of items from the writer. Ends with {@link #END_MARKER} if the writable channel is closed.
   * Only ever updated by the writer.
   */
  @GuardedBy("lock")
  private final ArrayDeque<Optional<Either<Throwable, ObjectIntPair<RowsAndColumns>>>> queue;

  /**
   * Whether {@link Readable#close()} has been called.
   */
  @GuardedBy("lock")
  private boolean readerClosed;

  /**
   * Future that is set to null by {@link #notifyWriter()} when the reader has read from {@link #queue}
   * or been closed.
   */
  @GuardedBy("lock")
  private SettableFuture<?> readyForWritingFuture;

  /**
   * Future that is set to null by {@link #notifyReader()} when the writer has written to {@link #queue}.
   */
  @GuardedBy("lock")
  private SettableFuture<?> readyForReadingFuture;

  /**
   * Create a channel with a particular buffer size (expressed in number of frames).
   */
  public BlockingQueueFrameChannel(final int maxQueuedFrames)
  {
    if (maxQueuedFrames < 1 || maxQueuedFrames == Integer.MAX_VALUE) {
      throw new IAE("Cannot handle capacity of [%d]", maxQueuedFrames);
    }

    this.maxQueuedFrames = maxQueuedFrames;
    this.queue = new ArrayDeque<>(maxQueuedFrames + 1); // Plus one to leave space for END_MARKER.
    this.writable = new Writable();
    this.readable = new Readable();
  }

  /**
   * Returns the writable side of this channel.
   */
  public WritableFrameChannel writable()
  {
    return writable;
  }

  /**
   * Returns the readable side of this channel.
   */
  public ReadableFrameChannel readable()
  {
    return readable;
  }

  /**
   * Create a channel that buffers one frame. This is the smallest possible queue size.
   */
  public static BlockingQueueFrameChannel minimal()
  {
    return new BlockingQueueFrameChannel(1);
  }

  @GuardedBy("lock")
  private void notifyWriter()
  {
    if (readyForWritingFuture != null) {
      final SettableFuture<?> tmp = readyForWritingFuture;
      this.readyForWritingFuture = null;
      tmp.set(null);
    }
  }

  @GuardedBy("lock")
  private void notifyReader()
  {
    if (readyForReadingFuture != null) {
      final SettableFuture<?> tmp = readyForReadingFuture;
      this.readyForReadingFuture = null;
      tmp.set(null);
    }
  }

  private class Writable implements WritableFrameChannel
  {
    @Override
    public void write(RowsAndColumns rac, int partitionNumber)
    {
      synchronized (lock) {
        if (isClosed()) {
          throw DruidException.defensive("Channel cannot accept new frames");
        } else if (queue.size() >= maxQueuedFrames) {
          // Caller should have checked if this channel was ready for writing.
          throw DruidException.defensive("Channel has no capacity");
        } else if (!queue.offer(Optional.of(Either.value(ObjectIntPair.of(rac, partitionNumber))))) {
          // If this happens, it's a bug in this class's capacity-counting.
          throw DruidException.defensive("Channel had capacity, but could not add frame");
        }

        notifyReader();
      }
    }

    @Override
    public ListenableFuture<?> writabilityFuture()
    {
      synchronized (lock) {
        if (queue.size() < maxQueuedFrames) {
          return Futures.immediateFuture(null);
        } else if (readyForWritingFuture != null) {
          return readyForWritingFuture;
        } else {
          return (readyForWritingFuture = SettableFuture.create());
        }
      }
    }

    @Override
    public void fail(@Nullable Throwable cause)
    {
      synchronized (lock) {
        queue.clear();

        if (!queue.offer(Optional.of(Either.error(cause != null ? cause : new RuntimeException("Failed"))))) {
          // If this happens, it's a bug, potentially due to incorrectly using this class with multiple writers.
          throw DruidException.defensive("Could not write error to channel");
        }
      }
    }

    @Override
    public void close()
    {
      synchronized (lock) {
        if (isClosed()) {
          throw DruidException.defensive("Already closed");
        }

        if (!queue.offer(END_MARKER)) {
          // If this happens, it's a bug, potentially due to incorrectly using this class with multiple writers.
          throw DruidException.defensive("Channel had capacity, but could not add end marker");
        }

        notifyReader();
      }
    }

    @Override
    public boolean isClosed()
    {
      synchronized (lock) {
        final Optional<Either<Throwable, ObjectIntPair<RowsAndColumns>>> lastElement = queue.peekLast();
        return END_MARKER.equals(lastElement);
      }
    }
  }

  private class Readable implements ReadableFrameChannel
  {
    @Override
    public boolean isFinished()
    {
      synchronized (lock) {
        if (readerClosed) {
          throw DruidException.defensive("Closed, cannot call isFinished()");
        }

        return END_MARKER.equals(queue.peek());
      }
    }

    @Override
    public boolean canRead()
    {
      synchronized (lock) {
        if (readerClosed) {
          throw DruidException.defensive("Closed, cannot call canRead()");
        }

        return !queue.isEmpty() && !isFinished();
      }
    }

    @Override
    public RowsAndColumns read()
    {
      final Optional<Either<Throwable, ObjectIntPair<RowsAndColumns>>> next;

      synchronized (lock) {
        if (readerClosed) {
          throw DruidException.defensive("Closed, cannot call read()");
        }

        if (isFinished()) {
          throw new NoSuchElementException();
        }

        next = queue.poll();

        if (next == null || !next.isPresent()) {
          throw new NoSuchElementException();
        }

        notifyWriter();
      }

      return next.get().valueOrThrow().left();
    }

    @Override
    public ListenableFuture<?> readabilityFuture()
    {
      synchronized (lock) {
        if (!queue.isEmpty()) {
          return Futures.immediateFuture(null);
        } else if (readyForReadingFuture != null) {
          return readyForReadingFuture;
        } else {
          return (readyForReadingFuture = SettableFuture.create());
        }
      }
    }

    @Override
    public void close()
    {
      synchronized (lock) {
        if (readerClosed) {
          // close() should not be called twice.
          throw DruidException.defensive("Already closed");
        }

        readerClosed = true;
        notifyWriter();
      }
    }
  }
}
