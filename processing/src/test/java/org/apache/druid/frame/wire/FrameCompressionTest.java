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

import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class FrameCompressionTest
{
  private static final int SEED = 12345;

  @Test
  public void test_compressionBufferSize()
  {
    // Verify buffer size is at least envelope size plus some room for data.
    final int size = FrameCompression.compressionBufferSize(100);
    Assert.assertTrue(size >= FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE + 100);
  }

  @Test
  public void test_compressionBufferSize_zero()
  {
    final int size = FrameCompression.compressionBufferSize(0);
    Assert.assertTrue(size >= FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE);
  }

  @Test
  public void test_compressAndDecompress_byteBuffer_smallData()
  {
    final byte[] data = "Hello, World!".getBytes();
    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_byteBuffer_emptyData()
  {
    final byte[] data = new byte[0];
    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_byteBuffer_largeData()
  {
    final Random random = new Random(SEED);
    final byte[] data = new byte[100_000];
    random.nextBytes(data);

    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_byteBuffer_highlyCompressibleData()
  {
    // Highly compressible data (all zeros).
    final byte[] data = new byte[10_000];
    Arrays.fill(data, (byte) 0);

    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);
    // Compressed data should be much smaller than original for highly compressible data.
    Assert.assertTrue(compressed.limit() < data.length);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_memory()
  {
    final byte[] data = "Test data for Memory compression".getBytes();
    final Memory dataMemory = Memory.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataMemory, data.length, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_memory_partialRegion()
  {
    // Create a buffer with padding at the start.
    final byte[] expected = "Test data".getBytes();
    final byte[] paddedData = new byte[100 + expected.length + 100];
    System.arraycopy(expected, 0, paddedData, 100, expected.length);

    // Create a Memory region that points only to the actual data.
    final Memory fullMemory = Memory.wrap(paddedData);
    final Memory dataRegion = fullMemory.region(100, expected.length);

    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(expected.length));
    final ByteBuffer compressed = FrameCompression.compress(dataRegion, expected.length, compressionBuffer);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(expected, decompressed);
  }

  @Test
  public void test_compress_bufferTooSmall()
  {
    final byte[] data = "Test data".getBytes();
    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer tooSmallBuffer = ByteBuffer.allocate(10);

    final ISE exception = Assert.assertThrows(ISE.class, () ->
        FrameCompression.compress(dataBuffer, tooSmallBuffer)
    );
    Assert.assertTrue(exception.getMessage().contains("Compression buffer too small"));
  }

  @Test
  public void test_decompress_regionTooShort()
  {
    final ByteBuffer tooShort = ByteBuffer.allocate(FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE - 1);
    final Memory memory = Memory.wrap(tooShort);

    final ISE exception = Assert.assertThrows(ISE.class, () ->
        FrameCompression.decompress(memory, 0, tooShort.capacity())
    );
    Assert.assertTrue(exception.getMessage().contains("Region too short"));
  }

  @Test
  public void test_decompress_outOfBounds()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(100);
    final Memory memory = Memory.wrap(buffer);

    final ISE exception = Assert.assertThrows(ISE.class, () ->
        FrameCompression.decompress(memory, 50, 100)
    );
    Assert.assertTrue(exception.getMessage().contains("out of bounds"));
  }

  @Test
  public void test_decompress_checksumMismatch()
  {
    // First, create valid compressed data.
    final byte[] data = "Test data for checksum test".getBytes();
    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    final int compressedLength = compressed.limit();

    // Corrupt the checksum (last 8 bytes).
    final byte[] corruptedData = new byte[compressedLength];
    compressed.get(corruptedData);
    corruptedData[compressedLength - 1] ^= 0xFF;

    final Memory corruptedMemory = Memory.wrap(corruptedData);

    final ISE exception = Assert.assertThrows(ISE.class, () ->
        FrameCompression.decompress(corruptedMemory, 0, compressedLength)
    );
    Assert.assertTrue(exception.getMessage().contains("Checksum mismatch"));
  }

  @Test
  public void test_decompress_atOffset()
  {
    // Create compressed data.
    final byte[] data = "Test data at offset".getBytes();
    final ByteBuffer dataBuffer = ByteBuffer.wrap(data);
    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(dataBuffer, compressionBuffer);
    final int compressedLength = compressed.limit();

    // Put the compressed data at an offset in a larger buffer.
    final int offset = 50;
    final byte[] paddedBuffer = new byte[offset + compressedLength + 50];
    compressed.get(paddedBuffer, offset, compressedLength);

    final Memory memory = Memory.wrap(paddedBuffer);
    final byte[] decompressed = FrameCompression.decompress(memory, offset, compressedLength);

    Assert.assertArrayEquals(data, decompressed);
  }

  @Test
  public void test_compressAndDecompress_byteBuffer_withSlice()
  {
    // Test with a sliced ByteBuffer to verify position handling.
    final byte[] fullData = new byte[200];
    final Random random = new Random(SEED);
    random.nextBytes(fullData);

    final ByteBuffer fullBuffer = ByteBuffer.wrap(fullData);
    fullBuffer.position(50);
    fullBuffer.limit(150);
    final ByteBuffer slicedBuffer = fullBuffer.slice();

    final ByteBuffer compressionBuffer = ByteBuffer.allocate(FrameCompression.compressionBufferSize(100));
    final ByteBuffer compressed = FrameCompression.compress(slicedBuffer, compressionBuffer);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    final byte[] expected = Arrays.copyOfRange(fullData, 50, 150);
    Assert.assertArrayEquals(expected, decompressed);
  }

  @Test
  public void test_constants()
  {
    // Verify constants are as expected per the javadoc.
    // Header: 1 byte compression type + 8 bytes compressed length + 8 bytes uncompressed length = 17 bytes
    Assert.assertEquals(17, FrameCompression.COMPRESSED_DATA_HEADER_SIZE);
    // Trailer: 8 bytes checksum
    Assert.assertEquals(8, FrameCompression.COMPRESSED_DATA_TRAILER_SIZE);
    // Envelope: header + trailer
    Assert.assertEquals(25, FrameCompression.COMPRESSED_DATA_ENVELOPE_SIZE);
  }

  @Test
  public void test_compressAndDecompress_directByteBuffer()
  {
    final byte[] data = "Direct buffer test".getBytes();
    final ByteBuffer directDataBuffer = ByteBuffer.allocateDirect(data.length);
    directDataBuffer.put(data);
    directDataBuffer.flip();

    final ByteBuffer compressionBuffer = ByteBuffer.allocateDirect(FrameCompression.compressionBufferSize(data.length));

    final ByteBuffer compressed = FrameCompression.compress(directDataBuffer, compressionBuffer);
    Assert.assertEquals(0, compressed.position());
    Assert.assertTrue(compressed.limit() > 0);

    final Memory compressedMemory = Memory.wrap(compressed);
    final byte[] decompressed = FrameCompression.decompress(compressedMemory, 0, compressed.limit());

    Assert.assertArrayEquals(data, decompressed);
  }
}
