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

package org.apache.druid.frame;

import com.google.common.primitives.Ints;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.data.CompressionStrategy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * A data frame.
 *
 * Frames are split into contiguous "regions". With columnar frames ({@link FrameType#COLUMNAR}) each region
 * is a column. With row-based frames ({@link FrameType#ROW_BASED}) there are always two regions: row offsets
 * and row data.
 *
 * This object is lightweight. It has constant overhead regardless of the number of rows or regions.
 *
 * Frames wrap some {@link Memory}. If the memory is backed by a resource that requires explicit releasing, such as
 * direct off-heap memory or a memory-mapped file, the creator of the Memory is responsible for releasing that resource
 * when the frame is no longer needed.
 *
 * Frames are written with {@link org.apache.druid.frame.write.FrameWriter} and read with
 * {@link org.apache.druid.frame.read.FrameReader}.
 *
 * Frame format:
 *
 * - 1 byte: {@link FrameType#version()}
 * - 8 bytes: size in bytes of the frame, little-endian long
 * - 4 bytes: number of rows, little-endian int
 * - 4 bytes: number of regions, little-endian int
 * - 1 byte: 0 if frame is nonpermuted, 1 if frame is permuted
 * - 4 bytes x numRows: permutation section; only present for permuted frames. Array of little-endian ints mapping
 * logical row numbers to physical row numbers.
 * - 8 bytes x numRegions: region offsets. Array of end offsets of each region (exclusive), relative to start of frame,
 * as little-endian longs.
 * - NNN bytes: regions, back-to-back.
 *
 * There is also a compressed frame format. Compressed frames are written by {@link #writeTo} when "compress" is
 * true, and decompressed by {@link #decompress}. Format:
 *
 * - 1 byte: compression type: {@link CompressionStrategy#getId()}. Currently, only LZ4 is supported.
 * - 8 bytes: compressed frame length, little-endian long
 * - 8 bytes: uncompressed frame length (numBytes), little-endian long
 * - NNN bytes: LZ4-compressed frame
 * - 8 bytes: 64-bit xxhash checksum of prior content, including 16-byte header and compressed frame, little-endian long
 *
 * Note to developers: if we end up needing to add more fields here, consider introducing a Smile (or Protobuf, etc)
 * header to make it simpler to add more fields.
 */
public class Frame
{
  public static final long HEADER_SIZE =
      Byte.BYTES /* version */ +
      Long.BYTES /* total size */ +
      Integer.BYTES /* number of rows */ +
      Integer.BYTES /* number of columns */ +
      Byte.BYTES /* permuted flag */;

  // Compression type, compressed length, uncompressed length
  public static final int COMPRESSED_FRAME_HEADER_SIZE = Byte.BYTES + Long.BYTES * 2;
  public static final int COMPRESSED_FRAME_TRAILER_SIZE = Long.BYTES; // Checksum
  public static final int COMPRESSED_FRAME_ENVELOPE_SIZE = COMPRESSED_FRAME_HEADER_SIZE
                                                           + COMPRESSED_FRAME_TRAILER_SIZE;

  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4SafeDecompressor LZ4_DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

  private static final int CHECKSUM_SEED = 0;

  private final Memory memory;
  private final FrameType frameType;
  private final long numBytes;
  private final int numRows;
  private final int numRegions;
  private final boolean permuted;

  private Frame(Memory memory, FrameType frameType, long numBytes, int numRows, int numRegions, boolean permuted)
  {
    this.memory = memory;
    this.frameType = frameType;
    this.numBytes = numBytes;
    this.numRows = numRows;
    this.numRegions = numRegions;
    this.permuted = permuted;
  }

  /**
   * Returns a frame backed by the provided Memory. This operation does not do any copies or allocations.
   *
   * The Memory must be in little-endian byte order.
   *
   * Behavior is undefined if the memory is modified anytime during the lifetime of the Frame object.
   */
  public static Frame wrap(final Memory memory)
  {
    if (memory.getTypeByteOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new IAE("Memory must be little-endian");
    }

    if (memory.getCapacity() < HEADER_SIZE) {
      throw new IAE("Memory too short for a header");
    }

    final byte version = memory.getByte(0);
    final FrameType frameType = FrameType.forVersion(version);

    if (frameType == null) {
      throw new IAE("Unexpected byte [%s] at start of frame", version);
    }

    final long numBytes = memory.getLong(Byte.BYTES);
    final int numRows = memory.getInt(Byte.BYTES + Long.BYTES);
    final int numRegions = memory.getInt(Byte.BYTES + Long.BYTES + Integer.BYTES);
    final boolean permuted = memory.getByte(Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES) != 0;

    validate(memory, numBytes, numRows, numRegions, permuted);
    return new Frame(memory, frameType, numBytes, numRows, numRegions, permuted);
  }

  /**
   * Returns a frame backed by the provided ByteBuffer. This operation does not do any copies or allocations.
   *
   * The position and limit of the buffer are ignored. If you need them to be respected, call
   * {@link ByteBuffer#slice()} first, or use {@link #wrap(Memory)} to wrap a particular region.
   */
  public static Frame wrap(final ByteBuffer buffer)
  {
    return wrap(Memory.wrap(buffer, ByteOrder.LITTLE_ENDIAN));
  }

  /**
   * Returns a frame backed by the provided byte array. This operation does not do any copies or allocations.
   *
   * The position and limit of the buffer are ignored. If you need them to be respected, call
   * {@link ByteBuffer#slice()} first, or use {@link #wrap(Memory)} to wrap a particular region.
   */
  public static Frame wrap(final byte[] bytes)
  {
    // Wrap using ByteBuffer, not Memory. Even though it's seemingly unnecessary, because ByteBuffers are re-wrapped
    // with Memory anyway, this is beneficial because it enables zero-copy optimizations (search for "hasByteBuffer").
    return wrap(ByteBuffer.wrap(bytes));
  }

  /**
   * Decompresses the provided memory and returns a frame backed by that decompressed memory. This operation is
   * safe even on corrupt data: it validates position, length, and checksum prior to decompressing.
   *
   * This operation allocates memory on-heap to store the decompressed frame.
   */
  public static Frame decompress(final Memory memory, final long position, final long length)
  {
    if (memory.getCapacity() < position + length) {
      throw new ISE("Provided position, length is out of bounds");
    }

    if (length < COMPRESSED_FRAME_ENVELOPE_SIZE) {
      throw new ISE("Region too short");
    }

    // Verify checksum.
    final long expectedChecksum = memory.getLong(position + length - COMPRESSED_FRAME_TRAILER_SIZE);
    final long actualChecksum = memory.xxHash64(position, length - COMPRESSED_FRAME_TRAILER_SIZE, CHECKSUM_SEED);

    if (expectedChecksum != actualChecksum) {
      throw new ISE("Checksum mismatch");
    }

    final byte compressionTypeId = memory.getByte(position);
    final CompressionStrategy compressionStrategy = CompressionStrategy.forId(compressionTypeId);

    if (compressionStrategy != CompressionStrategy.LZ4) {
      throw new ISE("Unsupported compression strategy [%s]", compressionStrategy);
    }

    final int compressedFrameLength = Ints.checkedCast(memory.getLong(position + Byte.BYTES));
    final int uncompressedFrameLength = Ints.checkedCast(memory.getLong(position + Byte.BYTES + Long.BYTES));
    final int compressedFrameLengthFromRegionLength = Ints.checkedCast(length - COMPRESSED_FRAME_ENVELOPE_SIZE);
    final long frameStart = position + COMPRESSED_FRAME_HEADER_SIZE;

    // Verify length.
    if (compressedFrameLength != compressedFrameLengthFromRegionLength) {
      throw new ISE(
          "Compressed sizes disagree: [%d] (embedded) vs [%d] (region length)",
          compressedFrameLength,
          compressedFrameLengthFromRegionLength
      );
    }

    if (memory.hasByteBuffer()) {
      // Decompress directly out of the ByteBuffer.
      final ByteBuffer srcBuffer = memory.getByteBuffer();
      final ByteBuffer dstBuffer = ByteBuffer.allocate(uncompressedFrameLength);
      final int numBytesDecompressed =
          LZ4_DECOMPRESSOR.decompress(
              srcBuffer,
              Ints.checkedCast(memory.getRegionOffset() + frameStart),
              compressedFrameLength,
              dstBuffer,
              0,
              uncompressedFrameLength
          );

      // Sanity check.
      if (numBytesDecompressed != uncompressedFrameLength) {
        throw new ISE(
            "Expected to decompress [%d] bytes but got [%d] bytes",
            uncompressedFrameLength,
            numBytesDecompressed
        );
      }

      return Frame.wrap(dstBuffer);
    } else {
      // Copy first, then decompress.
      final byte[] compressedFrame = new byte[compressedFrameLength];
      memory.getByteArray(frameStart, compressedFrame, 0, compressedFrameLength);
      return Frame.wrap(LZ4_DECOMPRESSOR.decompress(compressedFrame, uncompressedFrameLength));
    }
  }

  /**
   * Minimum size of compression buffer required by {@link #writeTo} when "compress" is true.
   */
  public static int compressionBufferSize(final long frameBytes)
  {
    return COMPRESSED_FRAME_ENVELOPE_SIZE + LZ4_COMPRESSOR.maxCompressedLength(Ints.checkedCast(frameBytes));
  }

  public FrameType type()
  {
    return frameType;
  }

  public long numBytes()
  {
    return numBytes;
  }

  public int numRows()
  {
    return numRows;
  }

  public int numRegions()
  {
    return numRegions;
  }

  public boolean isPermuted()
  {
    return permuted;
  }

  /**
   * Maps a logical row number to a physical row number. If the frame is non-permuted, these are the same. If the frame
   * is permuted, this uses the sorted-row mappings to remap the row number.
   *
   * @throws IllegalArgumentException if "logicalRow" is out of bounds
   */
  public int physicalRow(final int logicalRow)
  {
    if (logicalRow < 0 || logicalRow >= numRows) {
      throw new IAE("Row [%,d] out of bounds", logicalRow);
    }

    if (permuted) {
      final int rowPosition = memory.getInt(HEADER_SIZE + (long) Integer.BYTES * logicalRow);

      if (rowPosition < 0 || rowPosition >= numRows) {
        throw new ISE("Invalid physical row position for logical row [%,d]. Corrupt frame?", logicalRow);
      }

      return rowPosition;
    } else {
      return logicalRow;
    }
  }

  /**
   * Returns memory corresponding to a particular region of this frame.
   */
  public Memory region(final int regionNumber)
  {
    if (regionNumber < 0 || regionNumber >= numRegions) {
      throw new IAE("Region [%,d] out of bounds", regionNumber);
    }

    final long regionEndPositionSectionStart =
        HEADER_SIZE + (permuted ? (long) numRows * Integer.BYTES : 0);

    final long regionStartPosition;
    final long regionEndPosition = memory.getLong(regionEndPositionSectionStart + (long) regionNumber * Long.BYTES);

    if (regionNumber == 0) {
      regionStartPosition = regionEndPositionSectionStart + (long) numRegions * Long.BYTES;
    } else {
      regionStartPosition = memory.getLong(regionEndPositionSectionStart + (long) (regionNumber - 1) * Long.BYTES);
    }

    return memory.region(regionStartPosition, regionEndPosition - regionStartPosition);
  }

  /**
   * Direct, writable access to this frame's memory. Used by operations that modify the frame in-place, like
   * {@link org.apache.druid.frame.write.FrameSort}.
   *
   * Most callers should use {@link #region} and {@link #physicalRow}, rather than this direct-access method.
   *
   * @throws IllegalStateException if this frame wraps non-writable memory
   */
  public WritableMemory writableMemory()
  {
    if (memory instanceof WritableMemory) {
      return (WritableMemory) memory;
    } else {
      throw new ISE("Frame memory is not writable");
    }
  }

  /**
   * Writes this frame to a channel, optionally compressing it as well. Returns the number of bytes written.
   *
   * The provided {@code compressionBuffer} is used to hold compressed data temporarily, prior to writing it
   * to the channel. It must be at least as large as {@code compressionBufferSize(numBytes())}, or else an
   * {@link IllegalStateException} is thrown. It may be null if "compress" is false.
   */
  public long writeTo(
      final WritableByteChannel channel,
      final boolean compress,
      @Nullable final ByteBuffer compressionBuffer
  ) throws IOException
  {
    if (compress) {
      if (compressionBuffer == null) {
        throw new NullPointerException("compression buffer");
      } else if (compressionBuffer.capacity() < compressionBufferSize(numBytes)) {
        throw new ISE(
            "Compression buffer too small: expected [%,d] bytes but got [%,d] bytes",
            compressionBufferSize(numBytes),
            compressionBuffer.capacity()
        );
      }

      final ByteBuffer frameBuffer;
      final int frameBufferPosition;
      final int compressedFrameLength;

      if (memory.hasByteBuffer()) {
        // Optimized path when Memory is backed by ByteBuffer.
        frameBuffer = memory.getByteBuffer();
        frameBufferPosition = Ints.checkedCast(memory.getRegionOffset());
      } else {
        // Copy to byte array first, then decompress.
        final byte[] frameBytes = new byte[Ints.checkedCast(numBytes)];
        memory.getByteArray(0, frameBytes, 0, Ints.checkedCast(numBytes));
        frameBuffer = ByteBuffer.wrap(frameBytes);
        frameBufferPosition = 0;
      }

      compressedFrameLength = LZ4_COMPRESSOR.compress(
          frameBuffer,
          frameBufferPosition,
          Ints.checkedCast(numBytes),
          compressionBuffer,
          COMPRESSED_FRAME_HEADER_SIZE,
          compressionBuffer.capacity() - COMPRESSED_FRAME_ENVELOPE_SIZE
      );

      compressionBuffer.order(ByteOrder.LITTLE_ENDIAN)
                       .limit(COMPRESSED_FRAME_ENVELOPE_SIZE + compressedFrameLength)
                       .position(0);

      compressionBuffer.put(0, CompressionStrategy.LZ4.getId())
                       .putLong(Byte.BYTES, compressedFrameLength)
                       .putLong(Byte.BYTES + Long.BYTES, numBytes);

      final long checksum = Memory.wrap(compressionBuffer)
                                  .xxHash64(0, COMPRESSED_FRAME_HEADER_SIZE + compressedFrameLength, CHECKSUM_SEED);

      compressionBuffer.putLong(COMPRESSED_FRAME_HEADER_SIZE + compressedFrameLength, checksum);
      Channels.writeFully(channel, compressionBuffer);

      return COMPRESSED_FRAME_ENVELOPE_SIZE + compressedFrameLength;
    } else {
      memory.writeTo(0, numBytes, channel);
      return numBytes;
    }
  }

  /**
   * Perform basic frame validations, ensuring that the length of frame and region locations are correct.
   */
  private static void validate(
      final Memory memory,
      final long numBytes,
      final int numRows,
      final int numRegions,
      final boolean permuted
  )
  {
    if (numBytes != memory.getCapacity()) {
      throw new IAE("Declared size [%,d] does not match actual size [%,d]", numBytes, memory.getCapacity());
    }

    // Size of permuted row indices.
    final long rowOrderSize = (permuted ? (long) numRows * Integer.BYTES : 0);

    // Size of region ending positions.
    final long regionEndSize = (long) numRegions * Long.BYTES;

    final long expectedSizeForPreamble = HEADER_SIZE + rowOrderSize + regionEndSize;

    if (numBytes < expectedSizeForPreamble) {
      throw new IAE("Memory too short for preamble");
    }

    // Verify each region is wholly contained within this buffer.
    long regionStart = expectedSizeForPreamble; // First region starts immediately after preamble.
    long regionEnd;

    for (int regionNumber = 0; regionNumber < numRegions; regionNumber++) {
      regionEnd = memory.getLong(HEADER_SIZE + rowOrderSize + (long) regionNumber * Long.BYTES);

      if (regionEnd < regionStart || regionEnd > numBytes) {
        throw new IAE(
            "Region [%d] invalid: end [%,d] out of range [%,d -> %,d]",
            regionNumber,
            regionEnd,
            expectedSizeForPreamble,
            numBytes
        );
      }

      if (regionNumber > 0) {
        regionStart = memory.getLong(HEADER_SIZE + rowOrderSize + (long) (regionNumber - 1) * Long.BYTES);
      }

      if (regionStart < expectedSizeForPreamble || regionStart > numBytes) {
        throw new IAE(
            "Region [%d] invalid: start [%,d] out of range [%,d -> %,d]",
            regionNumber,
            regionStart,
            expectedSizeForPreamble,
            numBytes
        );
      }
    }
  }
}
