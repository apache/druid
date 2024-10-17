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

package org.apache.druid.frame.write;

import org.apache.datasketches.memory.WritableMemory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FrameWriterUtilsTest
{

  private static final byte[] INPUT_BYTE_ARRAY = new byte[]{0x0A, (byte) 0xA4, 0x00, 0x53};
  private static final ByteBuffer INPUT_BYTE_BUFFER = ByteBuffer.wrap(INPUT_BYTE_ARRAY);
  private static final WritableMemory WRITABLE_MEMORY = WritableMemory.allocate(10);

  @Test
  public void test_copyByteBufferToMemory_withAllowNullBytesOnArrayBackedBuffer()
  {
    int originalPosition = INPUT_BYTE_BUFFER.position();
    FrameWriterUtils.copyByteBufferToMemoryAllowingNullBytes(INPUT_BYTE_BUFFER, WRITABLE_MEMORY, 0, 4);
    byte[] outputArray = new byte[4];
    WRITABLE_MEMORY.getByteArray(0, outputArray, 0, 4);
    Assert.assertArrayEquals(INPUT_BYTE_ARRAY, outputArray);
    Assert.assertEquals(originalPosition, INPUT_BYTE_BUFFER.position());
  }

  @Test
  public void test_copyByteBufferToMemory_withAllowNullBytes()
  {
    int originalPosition = INPUT_BYTE_BUFFER.position();
    ByteBuffer inputBuffer = ByteBuffer.allocateDirect(10);
    inputBuffer.put(INPUT_BYTE_ARRAY, 0, 4);
    inputBuffer.rewind();
    FrameWriterUtils.copyByteBufferToMemoryAllowingNullBytes(inputBuffer, WRITABLE_MEMORY, 0, 4);
    byte[] outputArray = new byte[4];
    WRITABLE_MEMORY.getByteArray(0, outputArray, 0, 4);
    Assert.assertArrayEquals(INPUT_BYTE_ARRAY, outputArray);
    Assert.assertEquals(originalPosition, INPUT_BYTE_BUFFER.position());
  }

  @Test
  public void test_copyByteBufferToMemory_withRemoveNullBytes()
  {
    int originalPosition = INPUT_BYTE_BUFFER.position();
    FrameWriterUtils.copyByteBufferToMemoryDisallowingNullBytes(INPUT_BYTE_BUFFER, WRITABLE_MEMORY, 0, 4, true);
    byte[] outputArray = new byte[3];
    WRITABLE_MEMORY.getByteArray(0, outputArray, 0, 3);
    Assert.assertArrayEquals(new byte[]{0x0A, (byte) 0xA4, 0x53}, outputArray);
    Assert.assertEquals(originalPosition, INPUT_BYTE_BUFFER.position());
  }

  @Test
  public void test_copyByteBufferToMemory_withDisallowedNullBytes()
  {
    Assert.assertThrows(
        InvalidNullByteException.class,
        () -> FrameWriterUtils.copyByteBufferToMemoryDisallowingNullBytes(
            INPUT_BYTE_BUFFER,
            WRITABLE_MEMORY,
            0,
            4,
            false
        )
    );
  }
}
