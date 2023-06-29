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

package org.apache.druid.segment.serde.cell;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockCompressedPayloadReader implements Closeable
{
  private static final ByteBuffer NULL_CELL = ByteBuffer.wrap(new byte[0]);
  private final IntIndexView blockIndexView;
  private final ByteBuffer compressedBlocksByteBuffer;
  private final ByteBuffer uncompressedByteBuffer;
  private final Closer closer;
  private final int blockSize;
  private final long maxValidUncompressedOffset;
  private final CompressionStrategy.Decompressor decompressor;

  private int currentUncompressedBlockNumber = -1;

  private BlockCompressedPayloadReader(
      IntIndexView blockIndexView,
      ByteBuffer compressedBlocksByteBuffer,
      ByteBuffer uncompressedByteBuffer,
      CompressionStrategy.Decompressor decompressor,
      Closer closer
  )
  {
    this.blockIndexView = blockIndexView;
    this.compressedBlocksByteBuffer = compressedBlocksByteBuffer;
    this.uncompressedByteBuffer = uncompressedByteBuffer;
    this.closer = closer;
    uncompressedByteBuffer.clear();
    blockSize = uncompressedByteBuffer.remaining();
    maxValidUncompressedOffset = Integer.MAX_VALUE * (long) blockSize;
    this.decompressor = decompressor;
  }

  /**
   * @param originalByteBuffer - buffer as written byte {@link BlockCompressedPayloadWriter}. Not modified.
   * @param byteBufferProvider - should be native ordered ByteBuffer
   * @param decompressor       - decompressor for block compression
   * @return BlockCompressedPayloadReader
   */
  public static BlockCompressedPayloadReader create(
      ByteBuffer originalByteBuffer,
      ByteBufferProvider byteBufferProvider,
      CompressionStrategy.Decompressor decompressor
  )
  {
    ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

    int blockIndexSize = masterByteBuffer.getInt();
    ByteBuffer blockIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
    blockIndexBuffer.limit(blockIndexBuffer.position() + blockIndexSize);

    masterByteBuffer.position(masterByteBuffer.position() + blockIndexSize);

    int dataStreamSize = masterByteBuffer.getInt();
    ByteBuffer compressedBlockStreamByteBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
    compressedBlockStreamByteBuffer.limit(compressedBlockStreamByteBuffer.position() + dataStreamSize);

    Closer closer = Closer.create();
    ResourceHolder<ByteBuffer> byteBufferResourceHolder = byteBufferProvider.get();

    closer.register(byteBufferResourceHolder);

    return new BlockCompressedPayloadReader(
        new IntIndexView(blockIndexBuffer),
        compressedBlockStreamByteBuffer,
        byteBufferResourceHolder.get(),
        decompressor,
        closer
    );
  }

  public ByteBuffer read(long uncompressedStart, int size)
  {
    if (size == 0) {
      return NULL_CELL;
    }

    Preconditions.checkArgument(uncompressedStart + size < maxValidUncompressedOffset);

    int blockNumber = (int) (uncompressedStart / blockSize);
    int blockOffset = (int) (uncompressedStart % blockSize);
    ByteBuffer currentUncompressedBlock = getUncompressedBlock(blockNumber);

    currentUncompressedBlock.position(blockOffset);

    if (size <= currentUncompressedBlock.remaining()) {
      ByteBuffer resultByteBuffer = currentUncompressedBlock.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

      resultByteBuffer.limit(blockOffset + size);

      return resultByteBuffer;
    } else {
      byte[] payload = readMultiBlock(size, blockNumber, blockOffset);

      return ByteBuffer.wrap(payload).order(ByteOrder.nativeOrder());
    }
  }

  @Nonnull
  private byte[] readMultiBlock(int size, int blockNumber, int blockOffset)
  {
    byte[] payload = new byte[size];
    int bytesRead = 0;

    do {
      ByteBuffer currentUncompressedBlock = getUncompressedBlock(blockNumber);

      currentUncompressedBlock.position(blockOffset);

      int readSizeBytes = Math.min(size - bytesRead, currentUncompressedBlock.remaining());

      currentUncompressedBlock.get(payload, bytesRead, readSizeBytes);
      bytesRead += readSizeBytes;
      blockNumber++;
      blockOffset = 0;
    } while (bytesRead < size);

    return payload;
  }

  private ByteBuffer getUncompressedBlock(int blockNumber)
  {
    if (currentUncompressedBlockNumber != blockNumber) {
      IntIndexView.EntrySpan span = blockIndexView.getEntrySpan(blockNumber);
      ByteBuffer compressedBlock = compressedBlocksByteBuffer.asReadOnlyBuffer()
                                                             .order(compressedBlocksByteBuffer.order());
      compressedBlock.position(compressedBlock.position() + span.getStart());
      compressedBlock.limit(compressedBlock.position() + span.getSize());
      uncompressedByteBuffer.clear();

      decompressor.decompress(compressedBlock, span.getSize(), uncompressedByteBuffer);
      currentUncompressedBlockNumber = blockNumber;
    }

    return uncompressedByteBuffer;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
