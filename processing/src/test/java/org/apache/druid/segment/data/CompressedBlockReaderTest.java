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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedBlockReaderTest
{
  private static ByteBuffer header(int blockSize, int numBlocks)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.nativeOrder());
    buffer.put(CompressedBlockReader.VERSION);
    buffer.put(CompressionStrategy.UNCOMPRESSED.getId());
    buffer.putInt(blockSize);
    buffer.putInt(numBlocks);
    buffer.flip();
    return buffer;
  }

  @Test
  public void testNumBlocksZeroRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressedBlockReader.fromByteBuffer(
            header(64, 0), ByteOrder.nativeOrder(), ByteOrder.nativeOrder(), false
        )
    );
    Assertions.assertTrue(e.getMessage().contains("Number of blocks[0] must be positive"), e.getMessage());
  }

  @Test
  public void testNumBlocksNegativeRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressedBlockReader.fromByteBuffer(
            header(64, -5), ByteOrder.nativeOrder(), ByteOrder.nativeOrder(), false
        )
    );
    Assertions.assertTrue(e.getMessage().contains("Number of blocks[-5] must be positive"), e.getMessage());
  }

  @Test
  public void testBlockSizeZeroRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressedBlockReader.fromByteBuffer(
            header(0, 1), ByteOrder.nativeOrder(), ByteOrder.nativeOrder(), false
        )
    );
    Assertions.assertTrue(e.getMessage().contains("Block size[0] must be positive"), e.getMessage());
  }

  @Test
  public void testNumBlocksBeyondBufferRejected()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CompressedBlockReader.fromByteBuffer(
            header(64, 32), ByteOrder.nativeOrder(), ByteOrder.nativeOrder(), false
        )
    );
    Assertions.assertTrue(e.getMessage().contains("exceeds the available buffer"), e.getMessage());
  }

  @Test
  public void testValidHeaderAccepted()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.nativeOrder());
    buffer.put(CompressedBlockReader.VERSION);
    buffer.put(CompressionStrategy.UNCOMPRESSED.getId());
    buffer.putInt(64); // blockSize
    buffer.putInt(2);  // numBlocks
    buffer.putInt(4);  // offsets
    buffer.putInt(8);
    buffer.putInt(0);  // compressed bytes
    buffer.putInt(0);
    buffer.flip();

    CompressedBlockReader.fromByteBuffer(buffer, ByteOrder.nativeOrder(), ByteOrder.nativeOrder(), false);
    Assertions.assertTrue(true);
  }
}
