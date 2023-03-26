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

package org.apache.druid.segment.data;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.CompressedPools;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.function.Supplier;

/**
 * Reader for a virtual contiguous address range backed by compressed blocks of data.
 *
 * Format:
 * | version (byte) | compression (byte) | block size (int) | num blocks (int) | end offsets | compressed data |
 *
 * This mechanism supports two modes of use, the first where callers may ask for a range of data from the underlying
 * blocks, provided by {@link #getRange(long, int)}. The {@link ByteBuffer} provided by this method may or may not
 * be valid after additional calls to {@link #getRange(long, int)} or calls to {@link #seekBlock(int)}.
 *
 * For fixed width values which are aligned with the block size, callers may also use the method
 * {@link #getDecompressedDataBuffer()} to have direct access to the current uncompressed block, and use the methods
 * {@link #loadBlock(long)} to load the correct block and translate a virtual offset into the relative offset, or
 * {@link #seekBlock(int)} to change which block is currently loaded.
 *
 * {@link #getRange(long, int)} uses these same mechanisms internally to supply data.
 *
 * @see CompressedBlockSerializer for writer
 */
public final class CompressedBlockReader implements Closeable
{
  private static final ByteBuffer NULL_VALUE = ByteBuffer.wrap(new byte[0]);
  public static final byte VERSION = 0x01;

  public static Supplier<CompressedBlockReader> fromByteBuffer(ByteBuffer buffer, ByteOrder byteOrder)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final CompressionStrategy compression = CompressionStrategy.forId(buffer.get());
      final int blockSize = buffer.getInt();
      assert CompressedPools.BUFFER_SIZE == blockSize;
      Preconditions.checkState(
          blockSize <= CompressedPools.BUFFER_SIZE,
          "Maximum block size must be less than " + CompressedPools.BUFFER_SIZE
      );
      final int numBlocks = buffer.getInt();
      final int offsetsSize = numBlocks * Integer.BYTES;
      // buffer is at start of ending offsets
      final ByteBuffer offsets = buffer.asReadOnlyBuffer().order(byteOrder);
      offsets.limit(offsets.position() + offsetsSize);
      final IntBuffer offsetView = offsets.slice().order(byteOrder).asIntBuffer();
      final int compressedSize = offsetView.get(numBlocks - 1);

      // move to start of compressed data
      buffer.position(buffer.position() + offsetsSize);
      final ByteBuffer compressedData = buffer.asReadOnlyBuffer().order(byteOrder);
      compressedData.limit(compressedData.position() + compressedSize);
      buffer.position(buffer.position() + compressedSize);

      final ByteBuffer compressedDataView = compressedData.slice().order(byteOrder);
      return () -> new CompressedBlockReader(
          compression,
          numBlocks,
          blockSize,
          offsetView.asReadOnlyBuffer(),
          compressedDataView.asReadOnlyBuffer().order(byteOrder),
          byteOrder
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final CompressionStrategy.Decompressor decompressor;

  private final int numBlocks;
  private final int div;
  private final int rem;
  private final IntBuffer endOffsetsBuffer;
  private final ByteBuffer compressedDataBuffer;

  private final ResourceHolder<ByteBuffer> decompressedDataBufferHolder;
  private final ByteBuffer decompressedDataBuffer;

  private final ByteOrder byteOrder;
  private final Closer closer;
  private int currentBlockNumber = -1;

  public CompressedBlockReader(
      CompressionStrategy compressionStrategy,
      int numBlocks,
      int blockSize,
      IntBuffer endOffsetsBuffer,
      ByteBuffer compressedDataBuffer,
      ByteOrder byteOrder
  )
  {
    this.decompressor = compressionStrategy.getDecompressor();
    this.numBlocks = numBlocks;
    this.div = Integer.numberOfTrailingZeros(blockSize);
    this.rem = blockSize - 1;
    this.endOffsetsBuffer = endOffsetsBuffer;
    this.compressedDataBuffer = compressedDataBuffer;
    this.closer = Closer.create();
    this.decompressedDataBufferHolder = CompressedPools.getByteBuf(byteOrder);
    closer.register(decompressedDataBufferHolder);
    this.decompressedDataBuffer = decompressedDataBufferHolder.get();
    this.decompressedDataBuffer.clear();
    this.byteOrder = byteOrder;
  }

  /**
   * Get size in bytes of virtual contiguous buffer
   */
  public long getSize()
  {
    return endOffsetsBuffer.get(numBlocks - 1);
  }

  /**
   * Get current block number of data loaded in {@link #decompressedDataBuffer}
   */
  @SuppressWarnings("unused")
  public int getCurrentBlockNumber()
  {
    return currentBlockNumber;
  }

  /**
   * Current decompressed data buffer of the data located in {@link #currentBlockNumber}
   */
  public ByteBuffer getDecompressedDataBuffer()
  {
    return decompressedDataBuffer;
  }

  /**
   * Get {@link ByteBuffer} containing data from starting offset of contiguous virtual address range of the specified
   * size. If this data spans more than a single block, it will be copied on heap, but if not, will be a view into
   * {@link #decompressedDataBuffer}. The data returned by this method is not guaranteed to still be readable after
   * another call to {@link #getRange(long, int)} or a call to {@link #seekBlock(int)}.
   */
  public ByteBuffer getRange(long startOffset, int size)
  {
    if (size == 0) {
      return NULL_VALUE;
    }

    final int startBlockOffset = loadBlock(startOffset);
    final int startBlockNumber = currentBlockNumber;
    decompressedDataBuffer.position(startBlockOffset);
    // patch together value from n underlying compressed pages
    if (size < decompressedDataBuffer.remaining()) {
      // sweet, same buffer, we can slice out a view directly to the value
      final ByteBuffer dupe = decompressedDataBuffer.duplicate().order(byteOrder);
      dupe.position(startBlockOffset).limit(startBlockOffset + size);
      return dupe.slice().order(byteOrder);
    } else {
      // spans multiple blocks, copy on heap
      final byte[] bytes = new byte[size];
      int bytesRead = 0;
      int block = startBlockNumber;
      int blockOffset = startBlockOffset;
      do {
        seekBlock(block);
        decompressedDataBuffer.position(blockOffset);
        final int readSizeBytes = Math.min(size - bytesRead, decompressedDataBuffer.remaining());
        decompressedDataBuffer.get(bytes, bytesRead, readSizeBytes);
        bytesRead += readSizeBytes;
        block++;
        blockOffset = 0;
      } while (bytesRead < size);

      return ByteBuffer.wrap(bytes).order(byteOrder);
    }
  }

  /**
   * Load the block for the specified virtual offset, returning the relative offset into {@link #decompressedDataBuffer}
   * of the {@link #currentBlockNumber}.
   */
  public int loadBlock(long startOffset)
  {
    final int startBlockNumber = (int) (startOffset >> div);
    final int startBlockOffset = (int) (startOffset & rem);
    if (startBlockNumber != currentBlockNumber) {
      seekBlock(startBlockNumber);
    }
    return startBlockOffset;
  }

  /**
   * Swap the current data in {@link #decompressedDataBuffer} to the specified block
   */
  public void seekBlock(int block)
  {
    if (block == currentBlockNumber) {
      // the call is coming from inside the house
      return;
    }

    final int blockStartOffset;
    final int blockEndOffset;
    if (block == 0) {
      blockStartOffset = 0;
      blockEndOffset = endOffsetsBuffer.get(0);
    } else {
      blockStartOffset = endOffsetsBuffer.get(block - 1);
      blockEndOffset = endOffsetsBuffer.get(block);
    }
    decompressedDataBuffer.clear();
    compressedDataBuffer.limit(blockEndOffset);
    compressedDataBuffer.position(blockStartOffset);

    decompressor.decompress(compressedDataBuffer, blockEndOffset - blockStartOffset, decompressedDataBuffer);
    decompressedDataBuffer.limit(decompressedDataBuffer.capacity());

    currentBlockNumber = block;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
