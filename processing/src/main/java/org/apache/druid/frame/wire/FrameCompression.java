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

package org.apache.druid.frame.wire;

import com.google.common.primitives.Ints;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.data.CompressionStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for compressing and decompressing data using LZ4 compression with a specific envelope format.
 *
 * Compressed format:
 * - 1 byte: compression type: {@link CompressionStrategy#getId()}. Currently, only LZ4 is supported.
 * - 8 bytes: compressed data length, little-endian long
 * - 8 bytes: uncompressed data length, little-endian long
 * - NNN bytes: LZ4-compressed data
 * - 8 bytes: 64-bit xxhash checksum of prior content, including 17-byte header and compressed data, little-endian long
 */
public class FrameCompression
{
  public static final int COMPRESSED_DATA_HEADER_SIZE = Byte.BYTES + Long.BYTES * 2;
  public static final int COMPRESSED_DATA_TRAILER_SIZE = Long.BYTES; // Checksum
  public static final int COMPRESSED_DATA_ENVELOPE_SIZE = COMPRESSED_DATA_HEADER_SIZE + COMPRESSED_DATA_TRAILER_SIZE;

  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4SafeDecompressor LZ4_DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

  private static final int CHECKSUM_SEED = 0;

  private FrameCompression()
  {
    // No instantiation.
  }

  /**
   * Minimum size of compression buffer required by {@link #compress}.
   */
  public static int compressionBufferSize(final long dataBytes)
  {
    return COMPRESSED_DATA_ENVELOPE_SIZE + LZ4_COMPRESSOR.maxCompressedLength(Ints.checkedCast(dataBytes));
  }

  /**
   * Compresses data from Memory into the provided compression buffer.
   *
   * @param data               the memory region to compress
   * @param dataLength         the number of bytes to compress from memory
   * @param compressionBuffer  buffer to hold compressed output; must be at least {@link #compressionBufferSize} bytes
   * @return the compression buffer with position 0 and limit set to the number of bytes written
   */
  public static ByteBuffer compress(final Memory data, final long dataLength, final ByteBuffer compressionBuffer)
  {
    final ByteBuffer dataBuffer;

    if (data.hasByteBuffer()) {
      // Optimized path when Memory is backed by ByteBuffer.
      final ByteBuffer underlying = data.getByteBuffer();
      final int offset = Ints.checkedCast(data.getRegionOffset());
      dataBuffer = underlying.duplicate();
      dataBuffer.position(offset).limit(offset + Ints.checkedCast(dataLength));
    } else {
      // Copy to byte array first, then compress.
      final byte[] dataBytes = new byte[Ints.checkedCast(dataLength)];
      data.getByteArray(0, dataBytes, 0, Ints.checkedCast(dataLength));
      dataBuffer = ByteBuffer.wrap(dataBytes);
    }

    return compress(dataBuffer, compressionBuffer);
  }

  /**
   * Compresses data into the provided compression buffer.
   *
   * @param data               the data to compress (position and limit are used)
   * @param compressionBuffer  buffer to hold compressed output; must be at least {@link #compressionBufferSize} bytes
   * @return the compression buffer with position 0 and limit set to the number of bytes written
   */
  public static ByteBuffer compress(final ByteBuffer data, final ByteBuffer compressionBuffer)
  {
    final int dataLength = data.remaining();
    final int requiredSize = compressionBufferSize(dataLength);

    if (compressionBuffer.capacity() < requiredSize) {
      throw new ISE(
          "Compression buffer too small: expected [%,d] bytes but got [%,d] bytes",
          requiredSize,
          compressionBuffer.capacity()
      );
    }

    final int compressedDataLength = LZ4_COMPRESSOR.compress(
        data,
        data.position(),
        dataLength,
        compressionBuffer,
        COMPRESSED_DATA_HEADER_SIZE,
        compressionBuffer.capacity() - COMPRESSED_DATA_ENVELOPE_SIZE
    );

    compressionBuffer.order(ByteOrder.LITTLE_ENDIAN)
                     .limit(COMPRESSED_DATA_ENVELOPE_SIZE + compressedDataLength)
                     .position(0);

    compressionBuffer.put(0, CompressionStrategy.LZ4.getId())
                     .putLong(Byte.BYTES, compressedDataLength)
                     .putLong(Byte.BYTES + Long.BYTES, dataLength);

    final long checksum = Memory.wrap(compressionBuffer)
                                .xxHash64(0, COMPRESSED_DATA_HEADER_SIZE + compressedDataLength, CHECKSUM_SEED);

    compressionBuffer.putLong(COMPRESSED_DATA_HEADER_SIZE + compressedDataLength, checksum);

    return compressionBuffer;
  }

  /**
   * Decompresses the provided memory and returns a byte array with the decompressed data. This operation is
   * safe even on corrupt data: it validates position, length, and checksum prior to decompressing.
   *
   * This operation allocates memory on-heap to store the decompressed data.
   *
   * @param memory   the memory containing compressed data
   * @param position the position in memory where compressed data starts
   * @param length   the length of compressed data
   * @return a byte array containing the decompressed data
   */
  public static byte[] decompress(final Memory memory, final long position, final long length)
  {
    if (memory.getCapacity() < position + length) {
      throw new ISE("Provided position, length is out of bounds");
    }

    if (length < COMPRESSED_DATA_ENVELOPE_SIZE) {
      throw new ISE("Region too short");
    }

    // Verify checksum.
    final long expectedChecksum = memory.getLong(position + length - COMPRESSED_DATA_TRAILER_SIZE);
    final long actualChecksum = memory.xxHash64(position, length - COMPRESSED_DATA_TRAILER_SIZE, CHECKSUM_SEED);

    if (expectedChecksum != actualChecksum) {
      throw new ISE("Checksum mismatch");
    }

    final byte compressionTypeId = memory.getByte(position);
    final CompressionStrategy compressionStrategy = CompressionStrategy.forId(compressionTypeId);

    if (compressionStrategy != CompressionStrategy.LZ4) {
      throw new ISE("Unsupported compression strategy [%s]", compressionStrategy);
    }

    final int compressedDataLength = Ints.checkedCast(memory.getLong(position + Byte.BYTES));
    final int uncompressedDataLength = Ints.checkedCast(memory.getLong(position + Byte.BYTES + Long.BYTES));
    final int compressedDataLengthFromRegionLength = Ints.checkedCast(length - COMPRESSED_DATA_ENVELOPE_SIZE);
    final long dataStart = position + COMPRESSED_DATA_HEADER_SIZE;

    // Verify length.
    if (compressedDataLength != compressedDataLengthFromRegionLength) {
      throw new ISE(
          "Compressed sizes disagree: [%d] (embedded) vs [%d] (region length)",
          compressedDataLength,
          compressedDataLengthFromRegionLength
      );
    }

    if (memory.hasByteBuffer()) {
      // Decompress directly out of the ByteBuffer.
      final ByteBuffer srcBuffer = memory.getByteBuffer();
      final ByteBuffer dstBuffer = ByteBuffer.allocate(uncompressedDataLength);
      final int numBytesDecompressed =
          LZ4_DECOMPRESSOR.decompress(
              srcBuffer,
              Ints.checkedCast(memory.getRegionOffset() + dataStart),
              compressedDataLength,
              dstBuffer,
              0,
              uncompressedDataLength
          );

      // Sanity check.
      if (numBytesDecompressed != uncompressedDataLength) {
        throw new ISE(
            "Expected to decompress [%d] bytes but got [%d] bytes",
            uncompressedDataLength,
            numBytesDecompressed
        );
      }

      return dstBuffer.array();
    } else {
      // Copy first, then decompress.
      final byte[] compressedData = new byte[compressedDataLength];
      memory.getByteArray(dataStart, compressedData, 0, compressedDataLength);
      return LZ4_DECOMPRESSOR.decompress(compressedData, uncompressedDataLength);
    }
  }
}
