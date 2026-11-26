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

import org.apache.druid.java.util.common.io.Closer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class CompressionLevelTest
{
  @Test
  public void testLz4LevelsRoundTrip() throws Exception
  {
    for (int level : new int[]{0, 1, 4, 9, 17}) {
      assertRoundTrip(CompressionStrategy.LZ4, level);
    }
  }

  @Test
  public void testZstdLevelsRoundTrip() throws Exception
  {
    for (int level : new int[]{0, 1, 3, 22}) {
      assertRoundTrip(CompressionStrategy.ZSTD, level);
    }
  }

  @Test
  public void testUnspecifiedLevelPreservesDefaultCompressor()
  {
    Assertions.assertSame(
        CompressionStrategy.LZ4.getCompressor(),
        CompressionStrategy.LZ4.getCompressor(null)
    );
    Assertions.assertSame(
        CompressionStrategy.ZSTD.getCompressor(),
        CompressionStrategy.ZSTD.getCompressor(null)
    );
    Assertions.assertEquals((byte) 0x1, CompressionStrategy.LZ4.getId());
    Assertions.assertEquals((byte) 0x2, CompressionStrategy.ZSTD.getId());
  }

  @Test
  public void testInvalidLevelIsRejected()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressionStrategy.LZ4.getCompressor(-1)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressionStrategy.LZ4.getCompressor(18)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressionStrategy.ZSTD.getCompressor(-1)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressionStrategy.ZSTD.getCompressor(23)
    );
  }

  @Test
  public void testUnlevelledStrategiesIgnoreLevel()
  {
    Assertions.assertSame(
        CompressionStrategy.LZF.getCompressor(),
        CompressionStrategy.LZF.getCompressor(5)
    );
    Assertions.assertSame(
        CompressionStrategy.UNCOMPRESSED.getCompressor(),
        CompressionStrategy.UNCOMPRESSED.getCompressor(5)
    );
  }

  private static void assertRoundTrip(CompressionStrategy strategy, int level) throws Exception
  {
    final byte[] input = new byte[32 * 1024];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) (i % 11);
    }

    try (Closer closer = Closer.create()) {
      final CompressionStrategy.Compressor compressor = strategy.getCompressor(level);
      final ByteBuffer inputBuffer = compressor.allocateInBuffer(input.length, closer);
      final ByteBuffer outputBuffer = compressor.allocateOutBuffer(input.length, closer);
      inputBuffer.put(input);
      inputBuffer.flip();

      final int originalPosition = inputBuffer.position();
      final ByteBuffer compressed = compressor.compress(inputBuffer, outputBuffer);
      Assertions.assertEquals(originalPosition, inputBuffer.position());

      final ByteBuffer restored = ByteBuffer.allocateDirect(input.length);
      strategy.getDecompressor().decompress(compressed.duplicate(), compressed.remaining(), restored);

      final byte[] result = new byte[input.length];
      restored.get(result);
      Assertions.assertArrayEquals(input, result);
    }
  }
}
