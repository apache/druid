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

package org.apache.druid.segment.writeout;

import org.apache.druid.java.util.common.io.Closer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazilyAllocatingHeapWriteOutBytesTest
{
  private LazilyAllocatingHeapWriteOutBytes target;
  private Closer closer;
  private HeapByteBufferWriteOutBytes heapByteBufferWriteOutBytes;

  @BeforeEach
  public void setUp()
  {
    closer = Closer.create();
    heapByteBufferWriteOutBytes = new HeapByteBufferWriteOutBytes();
    target = new LazilyAllocatingHeapWriteOutBytes(
        () -> heapByteBufferWriteOutBytes,
        closer
    );
  }

  @Test
  public void testWritingToBuffer() throws IOException
  {
    Assertions.assertNull(target.getTmpBuffer());

    target.write(ByteBuffer.allocate(512));
    Assertions.assertNotNull(target.getTmpBuffer());
    Assertions.assertEquals(4096, target.getTmpBuffer().limit());
    Assertions.assertNull(target.getDelegate());

    target.write(ByteBuffer.allocate(16385));
    Assertions.assertNull(target.getTmpBuffer());
    Assertions.assertNotNull(target.getDelegate());
    Assertions.assertEquals(16385 + 512, target.getDelegate().size());
  }

  @Test
  public void testClosingWriteOutBytes() throws IOException
  {
    Assertions.assertNull(target.getTmpBuffer());

    target.writeInt(5);
    Assertions.assertNotNull(target.getTmpBuffer());
    Assertions.assertEquals(128, target.getTmpBuffer().limit());
    Assertions.assertNull(target.getDelegate());

    closer.close();

    Assertions.assertNull(target.getTmpBuffer());
  }
}
