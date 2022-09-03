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

package org.apache.druid.frame.file;

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Writer for {@link FrameFile}. See that class for format information.
 */
public class FrameFileWriter implements Closeable
{
  public static final byte[] MAGIC = {(byte) 0xff, 0x01};
  public static final byte MARKER_FRAME = (byte) 0x01;
  public static final byte MARKER_NO_MORE_FRAMES = (byte) 0x02;
  public static final int TRAILER_LENGTH = Integer.BYTES * 4;
  public static final int CHECKSUM_SEED = 0;
  public static final int NO_PARTITION = -1;

  private final WritableByteChannel channel;
  private final AppendableMemory tableOfContents;
  private final AppendableMemory partitions;
  private long bytesWritten = 0;
  private int numFrames = 0;
  private boolean usePartitions = true;
  private ByteBuffer compressionBuffer;
  private boolean closed = false;

  private FrameFileWriter(
      final WritableByteChannel channel,
      @Nullable final ByteBuffer compressionBuffer,
      final AppendableMemory tableOfContents,
      final AppendableMemory partitions
  )
  {
    this.channel = channel;
    this.compressionBuffer = compressionBuffer;
    this.tableOfContents = tableOfContents;
    this.partitions = partitions;
  }

  /**
   * Opens a writer for a particular channel.
   *
   * @param channel           destination channel
   * @param compressionBuffer result of {@link Frame#compressionBufferSize} for the largest possible frame size that
   *                          will be written to this file, or null to allocate buffers dynamically.
   *                          Providing an explicit buffer here, if possible, improves performance.
   */
  public static FrameFileWriter open(final WritableByteChannel channel, @Nullable final ByteBuffer compressionBuffer)
  {
    // Unlimited allocator is for convenience. Only a few bytes per frame will be allocated.
    final HeapMemoryAllocator allocator = HeapMemoryAllocator.unlimited();
    return new FrameFileWriter(
        channel,
        compressionBuffer,
        AppendableMemory.create(allocator),
        AppendableMemory.create(allocator)
    );
  }

  /**
   * Write a frame.
   *
   * @param frame     the frame
   * @param partition partition number for a partitioned frame file, or {@link #NO_PARTITION} for an unpartitioned file.
   *                  Must be monotonically increasing.
   */
  public void writeFrame(final Frame frame, final int partition) throws IOException
  {
    if (numFrames == Integer.MAX_VALUE) {
      throw new ISE("Too many frames");
    }

    if (partition < 0 && numFrames == 0) {
      usePartitions = false;
    }

    if (partition >= 0 != usePartitions) {
      throw new ISE("Cannot mix partitioned and non-partitioned data");
    }

    if (!tableOfContents.reserveAdditional(Long.BYTES)) {
      // Not likely to happen due to allocator limit of Long.MAX_VALUE.
      throw new ISE("Too many frames");
    }

    writeMagicIfNeeded();

    Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{MARKER_FRAME}));
    bytesWritten++;
    bytesWritten += frame.writeTo(channel, true, getCompressionBuffer(frame.numBytes()));

    // Write *end* of frame to tableOfContents.
    final MemoryRange<WritableMemory> tocCursor = tableOfContents.cursor();
    tocCursor.memory().putLong(tocCursor.start(), bytesWritten);
    tableOfContents.advanceCursor(Long.BYTES);

    if (usePartitions) {
      // Write new partition if needed.
      int highestPartitionWritten = Ints.checkedCast(partitions.size() / Integer.BYTES) - 1;

      if (partition < highestPartitionWritten) {
        // Partition number cannot go backwards.
        throw new ISE("Partition [%,d] < highest partition [%,d]", partition, highestPartitionWritten);
      }

      while (partition > highestPartitionWritten) {
        if (!partitions.reserveAdditional(Integer.BYTES)) {
          // Not likely to happen due to allocator limit of Long.MAX_VALUE. But, if this happens, the file is corrupt.
          // Throw an error so the caller knows it is bad.
          throw new ISE("Too many partitions");
        }

        final MemoryRange<WritableMemory> partitionCursor = partitions.cursor();
        highestPartitionWritten++;
        partitionCursor.memory().putInt(partitionCursor.start(), numFrames);
        partitions.advanceCursor(Integer.BYTES);
      }
    }

    numFrames++;
  }

  /**
   * Stops writing this file and closes early. Readers will be able to detect that the file is truncated due to the
   * lack of {@link #MARKER_NO_MORE_FRAMES}.
   *
   * After calling this method, {@link #close()} does nothing.
   */
  public void abort() throws IOException
  {
    if (!closed) {
      partitions.close();
      tableOfContents.close();
      channel.close();
      compressionBuffer = null;
      closed = true;
    }
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      // Already closed in abort() or a prior call to close().
      return;
    }

    writeMagicIfNeeded();

    if (!tableOfContents.reserveAdditional(Integer.BYTES * 3)) {
      throw new ISE("Can't finish table of contents");
    }

    final MemoryRange<WritableMemory> tocCursor = tableOfContents.cursor();
    final int numPartitions = Ints.checkedCast(partitions.size() / Integer.BYTES);

    tocCursor.memory().putInt(tocCursor.start(), numFrames);
    tocCursor.memory().putInt(tocCursor.start() + Integer.BYTES, numPartitions);
    tocCursor.memory().putInt(tocCursor.start() + Integer.BYTES * 2L, footerLength(numFrames, numPartitions));
    tableOfContents.advanceCursor(Integer.BYTES * 3);

    // Buffer up the footer so we can compute its checksum.
    final ByteBuffer footerBuf = ByteBuffer.allocate(footerLength(numFrames, numPartitions));
    final WritableMemory footerMemory = WritableMemory.writableWrap(footerBuf, ByteOrder.LITTLE_ENDIAN);
    assert Byte.BYTES + partitions.size() + tableOfContents.size() + Integer.BYTES == footerMemory.getCapacity();
    long p = Byte.BYTES;
    footerMemory.putByte(0, MARKER_NO_MORE_FRAMES);
    p += partitions.writeTo(footerMemory, p);
    partitions.close();
    p += tableOfContents.writeTo(footerMemory, p);
    tableOfContents.close();
    final int checksum = (int) footerMemory.xxHash64(0, p, CHECKSUM_SEED);
    footerMemory.putInt(p, checksum);

    // Write footer to the channel.
    Channels.writeFully(channel, footerBuf);
    channel.close();
    compressionBuffer = null;
    closed = true;
  }

  private void writeMagicIfNeeded() throws IOException
  {
    if (numFrames == 0) {
      Channels.writeFully(channel, ByteBuffer.wrap(MAGIC));
      bytesWritten += MAGIC.length;
    }
  }

  private ByteBuffer getCompressionBuffer(final long frameSize)
  {
    final int requiredSize = Frame.compressionBufferSize(frameSize);

    if (compressionBuffer == null || compressionBuffer.capacity() < requiredSize) {
      // Re-allocate a larger buffer.
      compressionBuffer = ByteBuffer.allocate(requiredSize);
    }

    return compressionBuffer;
  }

  /**
   * Length of the footer: everything from MARKER_NO_MORE_FRAMES to EOF. See class-level javadoc from {@link FrameFile}
   * for details on the format.
   */
  static int footerLength(final int numFrames, final int numPartitions)
  {
    return Ints.checkedCast(
        Byte.BYTES // MARKER_NO_MORE_FRAMES
        + (long) Integer.BYTES * numPartitions
        + (long) Long.BYTES * numFrames
        + TRAILER_LENGTH
    );
  }
}
