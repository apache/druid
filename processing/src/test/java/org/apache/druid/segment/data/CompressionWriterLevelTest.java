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
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class CompressionWriterLevelTest
{
  @Test
  public void testGenericIndexedWriterCompressionLevels() throws Exception
  {
    verifyCompressedBufferRoundTrip(CompressionStrategy.LZ4, null);
    verifyCompressedBufferRoundTrip(CompressionStrategy.LZ4, 0);
    verifyCompressedBufferRoundTrip(CompressionStrategy.LZ4, 9);
    verifyCompressedBufferRoundTrip(CompressionStrategy.ZSTD, null);
    verifyCompressedBufferRoundTrip(CompressionStrategy.ZSTD, 3);
  }

  @Test
  public void testCompressedBlockSerializerCompressionLevels() throws Exception
  {
    for (CompressionStrategy codec : new CompressionStrategy[]{CompressionStrategy.LZ4, CompressionStrategy.ZSTD}) {
      try (Closer closer = Closer.create();
           OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium()) {
        final CompressedBlockSerializer legacy = new CompressedBlockSerializer(
            medium,
            codec,
            64 * 1024,
            closer
        );
        legacy.open();
        legacy.addValue(new byte[8192]);
        Assertions.assertTrue(legacy.getSerializedSize() > 0);
      }
    }

    for (CompressionStrategy codec : new CompressionStrategy[]{CompressionStrategy.LZ4, CompressionStrategy.ZSTD}) {
      try (Closer closer = Closer.create();
           OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium()) {
        final int level = codec == CompressionStrategy.LZ4 ? 0 : 3;
        final CompressedBlockSerializer configured = new CompressedBlockSerializer(
            medium,
            codec,
            level,
            64 * 1024,
            closer
        );
        configured.open();
        configured.addValue(new byte[8192]);
        Assertions.assertTrue(configured.getSerializedSize() > 0);
      }
    }
  }

  private static void verifyCompressedBufferRoundTrip(
      CompressionStrategy codec,
      Integer level
  ) throws Exception
  {
    final byte[] expected = new byte[16 * 1024];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) (i % 17);
    }

    try (Closer closer = Closer.create();
         OnHeapMemorySegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium()) {
      final ObjectStrategy<ByteBuffer> writerStrategy = level == null
                                                        ? GenericIndexedWriter.compressedByteBuffersWriteObjectStrategy(
                                                            codec,
                                                            expected.length,
                                                            closer
                                                        )
                                                        : GenericIndexedWriter.compressedByteBuffersWriteObjectStrategy(
                                                            codec,
                                                            level,
                                                            expected.length,
                                                            closer
                                                        );
      final ByteBuffer input = ByteBuffer.allocateDirect(expected.length);
      input.put(expected);
      input.flip();

      final WriteOutBytes bytes = medium.makeWriteOutBytes();
      writerStrategy.writeTo(input, bytes);

      final ByteBuffer compressed = ByteBuffer.allocateDirect((int) bytes.size());
      bytes.readFully(0, compressed);
      compressed.flip();

      final ByteBuffer restored = ByteBuffer.allocateDirect(expected.length);
      codec.getDecompressor().decompress(compressed, compressed.remaining(), restored);
      final byte[] actual = new byte[expected.length];
      restored.get(actual);
      Assertions.assertArrayEquals(expected, actual);
    }
  }
}
