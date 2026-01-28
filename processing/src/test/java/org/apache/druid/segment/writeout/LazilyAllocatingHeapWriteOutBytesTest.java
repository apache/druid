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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazilyAllocatingHeapWriteOutBytesTest
{
  private LazilyAllocatingHeapWriteOutBytes target;
  private Closer closer;
  private HeapByteBufferWriteOutBytes heapByteBufferWriteOutBytes;

  @Before
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
    Assert.assertNull(target.getTmpBuffer());

    target.write(ByteBuffer.allocate(512));
    Assert.assertNotNull(target.getTmpBuffer());
    Assert.assertEquals(4096, target.getTmpBuffer().limit());
    Assert.assertNull(target.getDelegate());

    target.write(ByteBuffer.allocate(16385));
    Assert.assertNull(target.getTmpBuffer());
    Assert.assertNotNull(target.getDelegate());
    Assert.assertEquals(16385 + 512, target.getDelegate().size());
  }

  @Test
  public void testClosingWriteOutBytes() throws IOException
  {
    Assert.assertNull(target.getTmpBuffer());

    target.writeInt(5);
    Assert.assertNotNull(target.getTmpBuffer());
    Assert.assertEquals(128, target.getTmpBuffer().limit());
    Assert.assertNull(target.getDelegate());

    closer.close();

    Assert.assertNull(target.getTmpBuffer());
  }
}
