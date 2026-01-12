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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.wire.FrameCompression;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;

import javax.annotation.Nullable;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Channel backed by a byte stream that is continuously streamed in using {@link #addChunk}. The byte stream
 * must be in the format of a {@link org.apache.druid.frame.file.FrameFile}.
 *
 * This class is used by {@link org.apache.druid.frame.file.FrameFileHttpResponseHandler} to provide nonblocking
 * reads from a remote http server.
 */
public class ReadableByteChunksFrameChannel implements ReadableFrameChannel
{
  private static final Logger log = new Logger(ReadableByteChunksFrameChannel.class);

  /**
   * Largest supported frame. Limit exists as a safeguard against streams with huge frame sizes. It is not expected
   * that any legitimate frame will be this large: typical usage involves frames an order of magnitude smaller.
   */
  private static final long MAX_FRAME_SIZE_BYTES = 100_000_000;

  private static final int UNKNOWN_LENGTH = -1;
  private static final byte NO_MARKER = 0;
  private static final int FRAME_MARKER_BYTES = Byte.BYTES;
  private static final int FRAME_MARKER_AND_COMPRESSED_ENVELOPE_BYTES =
      Byte.BYTES + FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE;

  private enum StreamPart
  {
    MAGIC,
    FRAMES,
    FOOTER
  }

  private final Object lock = new Object();
  private final String id;
  private final long bytesLimit;

  @GuardedBy("lock")
  private final List<Either<Throwable, byte[]>> chunks = new ArrayList<>();

  @GuardedBy("lock")
  private SettableFuture<?> addChunkBackpressureFuture = null;

  @GuardedBy("lock")
  private SettableFuture<?> readyForReadingFuture = null;

  @GuardedBy("lock")
  private boolean noMoreWrites = false;

  @GuardedBy("lock")
  private int positionInFirstChunk = 0;

  @GuardedBy("lock")
  private long bytesBuffered = 0;

  @GuardedBy("lock")
  private long bytesAdded = 0;

  @GuardedBy("lock")
  private long nextCompressedFrameLength = UNKNOWN_LENGTH;

  /**
   * The marker type of the next frame to be read. Set when {@link #nextCompressedFrameLength} is determined.
   * Will be one of {@link FrameFileWriter#MARKER_FRAME}, {@link FrameFileWriter#MARKER_RAC}, or {@link #NO_MARKER}
   * if no frame is currently ready.
   */
  @GuardedBy("lock")
  private byte nextMarkerType = NO_MARKER;

  @GuardedBy("lock")
  private StreamPart streamPart;

  private final boolean framesOnly;

  @Nullable
  private final WireTransferableContext wtContext;

  private ReadableByteChunksFrameChannel(
      String id,
      long bytesLimit,
      boolean framesOnly,
      @Nullable WireTransferableContext wtContext
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.bytesLimit = bytesLimit;
    this.streamPart = framesOnly ? StreamPart.FRAMES : StreamPart.MAGIC;
    this.framesOnly = framesOnly;
    this.wtContext = wtContext;
  }

  /**
   * Create a channel that aims to limit its memory footprint to one frame. The channel exerts backpressure
   * from {@link #addChunk} immediately once a full frame has been buffered.
   *
   * @param id                      identifier for this channel
   * @param framesOnly              if true, expects stream to include only FRAMES part (no header or footer)
   * @param wireTransferableContext context for wire transfer serde
   */
  public static ReadableByteChunksFrameChannel create(
      final String id,
      boolean framesOnly,
      @Nullable WireTransferableContext wireTransferableContext
  )
  {
    // Set byte limit to 1, so backpressure will be exerted as soon as we have a full frame buffered.
    // (The bytesLimit is soft: it will be exceeded if needed to store a complete frame.)
    return new ReadableByteChunksFrameChannel(id, 1, framesOnly, wireTransferableContext);
  }

  /**
   * Adds a chunk of bytes. If this chunk forms a full frame, it will immediately become available for reading.
   * Otherwise, the bytes will be buffered until a full frame is encountered.
   *
   * Returns a backpressure future if the amount of queued bytes is at or above this channel's limit. If the return
   * future is nonnull, callers are politely requested to wait for the future to resolve before adding additional
   * chunks. (This is not enforced; addChunk will continue to accept new chunks even if the channel is over its limit.)
   *
   * When done adding chunks call {@code doneWriting}.
   *
   * @throws ChannelClosedForWritesException if the channel is closed
   */
  @Nullable
  public ListenableFuture<?> addChunk(final byte[] chunk)
  {
    synchronized (lock) {
      if (noMoreWrites) {
        throw new ChannelClosedForWritesException();
      }

      try {
        if (chunk.length > 0) {
          bytesAdded += chunk.length;

          if (streamPart != StreamPart.FOOTER) {
            // Footer is discarded: it isn't useful when reading frame files as streams. (It contains pointers
            // for random access of frames.)
            chunks.add(Either.value(chunk));
            bytesBuffered += chunk.length;
          }

          updateStreamState();

          if (readyForReadingFuture != null && canReadFrame()) {
            readyForReadingFuture.set(null);
            readyForReadingFuture = null;
          }
        }

        if (addChunkBackpressureFuture == null && bytesBuffered >= bytesLimit && canReadFrame()) {
          addChunkBackpressureFuture = SettableFuture.create();
        }

        return addChunkBackpressureFuture;
      }
      catch (Throwable e) {
        // The channel is in an inconsistent state if any of this logic throws an error. Shut it down.
        setError(e);
        return null;
      }
    }
  }

  /**
   * Clears the channel and replaces it with the given error. After calling this method, no additional chunks will be accepted.
   */
  public void setError(final Throwable t)
  {
    synchronized (lock) {
      // Write error to the channel, unless "noMoreWrites" is set. If that's set, suppress errors, so regular channel
      // shutdown doesn't trigger warnings in the log.
      if (!noMoreWrites) {
        chunks.clear();
        chunks.add(Either.error(t));
        nextCompressedFrameLength = UNKNOWN_LENGTH;
        nextMarkerType = NO_MARKER;
        doneWriting();
      }
    }
  }

  /**
   * Call method when caller is done adding chunks.
   */
  public void doneWriting()
  {
    synchronized (lock) {
      noMoreWrites = true;

      if (readyForReadingFuture != null) {
        readyForReadingFuture.set(null);
        readyForReadingFuture = null;
      }
    }
  }

  @Override
  public boolean isFinished()
  {
    synchronized (lock) {
      return chunks.isEmpty() && noMoreWrites && !canRead();
    }
  }

  @Override
  public boolean canRead()
  {
    synchronized (lock) {
      if (framesOnly) {
        return canReadError() || canReadFrame();
      }
      // The noMoreWrites check is here so read() can throw an error if the last few chunks are an incomplete frame.
      return canReadError() || canReadFrame() || (streamPart != StreamPart.FOOTER && noMoreWrites);
    }
  }

  @Override
  public RowsAndColumns readRAC()
  {
    synchronized (lock) {
      if (canReadError()) {
        // This map will be a no-op since we're guaranteed that the next chunk is an error.
        final Throwable t = chunks.remove(0).map(bytes -> null).error();
        Throwables.propagateIfPossible(t);
        throw new RuntimeException(t);
      } else if (canReadFrame()) {
        return nextRAC();
      } else if (noMoreWrites) {
        // The last few chunks are an incomplete or missing frame.
        chunks.clear();
        nextCompressedFrameLength = UNKNOWN_LENGTH;
        nextMarkerType = NO_MARKER;

        throw new ISE(
            "Incomplete or missing frame at end of stream (id = %s, position = %d)",
            id,
            bytesAdded - bytesBuffered
        );
      } else {
        assert !canRead();

        // This method should not have been called at this time.
        throw new NoSuchElementException();
      }
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    synchronized (lock) {
      if (canRead() || isFinished()) {
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
      chunks.clear();
      nextCompressedFrameLength = UNKNOWN_LENGTH;
      nextMarkerType = NO_MARKER;

      // Setting "noMoreWrites" causes the upstream entity to realize this channel has closed the next time
      // it calls "addChunk".
      noMoreWrites = true;
    }
  }

  public String getId()
  {
    return id;
  }

  public long getBytesAdded()
  {
    synchronized (lock) {
      return bytesAdded;
    }
  }

  public boolean isErrorOrFinished()
  {
    synchronized (lock) {
      return isFinished() || (canRead() && !canReadFrame());
    }
  }

  @VisibleForTesting
  long getBytesBuffered()
  {
    synchronized (lock) {
      return bytesBuffered;
    }
  }

  private RowsAndColumns nextRAC()
  {
    final Memory compressedMemory;
    final byte markerType;

    synchronized (lock) {
      if (!canReadFrame()) {
        throw new ISE("Frame of size [%,d] not yet ready to read", nextCompressedFrameLength);
      }

      if (nextCompressedFrameLength > Integer.MAX_VALUE - FRAME_MARKER_BYTES - FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE) {
        throw new ISE("Cannot read frame of size [%,d] bytes", nextCompressedFrameLength);
      }

      markerType = nextMarkerType;
      final int numBytes = Ints.checkedCast(FRAME_MARKER_AND_COMPRESSED_ENVELOPE_BYTES + nextCompressedFrameLength);
      compressedMemory = copyFromQueuedChunks(numBytes).region(
          FRAME_MARKER_BYTES,
          FRAME_MARKER_AND_COMPRESSED_ENVELOPE_BYTES + nextCompressedFrameLength - FRAME_MARKER_BYTES
      );
      deleteFromQueuedChunks(numBytes);
      updateStreamState();
    }

    final byte[] decompressedBytes =
        FrameCompression.decompress(compressedMemory, 0, compressedMemory.getCapacity());

    switch (markerType) {
      case FrameFileWriter.MARKER_FRAME:
        final Frame frame = Frame.wrap(decompressedBytes);
        log.debug("Read frame with rows[%,d] and bytes[%,d].", frame.numRows(), frame.numBytes());
        return frame.asRAC();

      case FrameFileWriter.MARKER_RAC:
        if (wtContext == null) {
          throw DruidException.defensive("Cannot read RAC, no WireTransferableContext");
        }
        final RowsAndColumns rac = wtContext.deserialize(ByteBufferUtils.wrapLE(decompressedBytes));
        log.debug("Read RAC with rows[%,d] and bytes[%,d].", rac.numRows(), decompressedBytes.length);
        return rac;

      default:
        throw new ISE("Unexpected marker type[%d]", markerType);
    }
  }

  @GuardedBy("lock")
  private void updateStreamState()
  {
    if (streamPart == StreamPart.MAGIC) {
      if (bytesBuffered >= FrameFileWriter.MAGIC.length) {
        final Memory memory = copyFromQueuedChunks(FrameFileWriter.MAGIC.length);

        if (memory.equalTo(0, Memory.wrap(FrameFileWriter.MAGIC), 0, FrameFileWriter.MAGIC.length)) {
          streamPart = StreamPart.FRAMES;
          deleteFromQueuedChunks(FrameFileWriter.MAGIC.length);
        } else {
          throw new ISE("Invalid stream header (id = %s, position = %d)", id, bytesAdded - bytesBuffered);
        }
      }
    }

    if (streamPart == StreamPart.FRAMES) {
      if (bytesBuffered >= Byte.BYTES) {
        final Memory memory = copyFromQueuedChunks(1);
        final byte markerByte = memory.getByte(0);

        if (markerByte == FrameFileWriter.MARKER_FRAME || markerByte == FrameFileWriter.MARKER_RAC) {
          // Read nextFrameLength if needed; otherwise do nothing.
          // Both MARKER_FRAME and MARKER_RAC use the same compression envelope format.
          final int bytesRequiredToReadLength = FRAME_MARKER_BYTES + FrameCompression.COMPRESSED_DATA_HEADER_SIZE;

          if (nextCompressedFrameLength == UNKNOWN_LENGTH && bytesBuffered >= bytesRequiredToReadLength) {
            nextMarkerType = markerByte;
            nextCompressedFrameLength = copyFromQueuedChunks(bytesRequiredToReadLength)
                .getLong(FRAME_MARKER_BYTES + Byte.BYTES /* Compression strategy byte */);

            if (nextCompressedFrameLength <= 0 || nextCompressedFrameLength >= MAX_FRAME_SIZE_BYTES) {
              throw new ISE("Invalid frame size (size = %,d B)", nextCompressedFrameLength);
            }
          }
        } else if (markerByte == FrameFileWriter.MARKER_NO_MORE_FRAMES) {
          streamPart = StreamPart.FOOTER;
          nextCompressedFrameLength = UNKNOWN_LENGTH;
          nextMarkerType = NO_MARKER;
        } else {
          throw new ISE("Invalid midstream marker (id = %s, position = %d)", id, bytesAdded - bytesBuffered);
        }
      }
    }

    if (streamPart == StreamPart.FOOTER) {
      if (bytesBuffered > 0) {
        // Footer is discarded: it isn't useful when reading frame files as streams. (It contains pointers
        // for random access of frames.)
        deleteFromQueuedChunks(bytesBuffered);
      }

      assert bytesBuffered == 0 && chunks.isEmpty() && nextCompressedFrameLength == UNKNOWN_LENGTH;
    }

    if (addChunkBackpressureFuture != null && (bytesBuffered < bytesLimit || !canReadFrame())) {
      // Release backpressure.
      addChunkBackpressureFuture.set(null);
      addChunkBackpressureFuture = null;
    }
  }

  @GuardedBy("lock")
  private boolean canReadError()
  {
    return chunks.size() > 0 && chunks.get(0).isError();
  }

  @GuardedBy("lock")
  private boolean canReadFrame()
  {
    return nextCompressedFrameLength != UNKNOWN_LENGTH
           && bytesBuffered >= FRAME_MARKER_AND_COMPRESSED_ENVELOPE_BYTES + nextCompressedFrameLength;
  }

  @GuardedBy("lock")
  private Memory copyFromQueuedChunks(final int numBytes)
  {
    if (bytesBuffered < numBytes) {
      throw new IAE("Cannot copy [%,d] bytes, only have [%,d] buffered", numBytes, bytesBuffered);
    }

    final WritableMemory buf = WritableMemory.allocate(numBytes, ByteOrder.LITTLE_ENDIAN);

    int bufPos = 0;
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      final byte[] chunk = chunks.get(chunkNumber).valueOrThrow();
      final int chunkPosition = chunkNumber == 0 ? positionInFirstChunk : 0;
      final int len = Math.min(chunk.length - chunkPosition, numBytes - bufPos);

      buf.putByteArray(bufPos, chunk, chunkPosition, len);
      bufPos += len;

      if (bufPos == numBytes) {
        break;
      }
    }

    return buf;
  }

  @GuardedBy("lock")
  private void deleteFromQueuedChunks(final long numBytes)
  {
    if (bytesBuffered < numBytes) {
      throw new IAE("Cannot delete [%,d] bytes, only have [%,d] buffered", numBytes, bytesBuffered);
    }

    long toDelete = numBytes;

    while (toDelete > 0) {
      final byte[] chunk = chunks.get(0).valueOrThrow();
      final int bytesRemainingInChunk = chunk.length - positionInFirstChunk;

      if (toDelete >= bytesRemainingInChunk) {
        toDelete -= bytesRemainingInChunk;
        positionInFirstChunk = 0;
        chunks.remove(0);
      } else {
        positionInFirstChunk = Ints.checkedCast(positionInFirstChunk + toDelete);
        toDelete = 0;
      }
    }

    bytesBuffered -= numBytes;

    // Clear nextFrameLength and nextMarkerType; they won't be accurate anymore after deleting bytes.
    nextCompressedFrameLength = UNKNOWN_LENGTH;
    nextMarkerType = NO_MARKER;
  }
}
