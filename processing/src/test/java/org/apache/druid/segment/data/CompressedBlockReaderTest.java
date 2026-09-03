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

import org.apache.druid.segment.CompressedPools;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedBlockReaderTest
{
  @Test
  public void testRejectsCompressedSizeBeyondBufferWithoutAdvancingPastOffsets()
  {
    final int headerSize = 2 * Byte.BYTES + 2 * Integer.BYTES;
    final ByteBuffer buffer = ByteBuffer.allocate(headerSize + Integer.BYTES + Byte.BYTES)
                                        .order(ByteOrder.BIG_ENDIAN);
    buffer.put(CompressedBlockReader.VERSION);
    buffer.put(CompressionStrategy.LZ4.getId());
    buffer.putInt(CompressedPools.BUFFER_SIZE);
    buffer.putInt(1);
    buffer.putInt(2);
    buffer.put((byte) 0);
    buffer.flip();

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> CompressedBlockReader.fromByteBuffer(
            buffer,
            ByteOrder.BIG_ENDIAN,
            ByteOrder.BIG_ENDIAN,
            false
        )
    );
    Assertions.assertEquals(headerSize, buffer.position());
  }
}
