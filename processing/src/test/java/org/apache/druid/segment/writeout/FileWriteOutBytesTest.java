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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileWriteOutBytesTest
{
  private FileWriteOutBytes fileWriteOutBytes;
  private FileChannel mockFileChannel;

  @Before
  public void setUp()
  {
    mockFileChannel = EasyMock.mock(FileChannel.class);
    fileWriteOutBytes = new FileWriteOutBytes(EasyMock.mock(File.class), mockFileChannel);
  }

  @Test
  public void write4KBIntsShouldNotFlush() throws IOException
  {
    // Write 4KB of ints and expect the write operation of the file channel will be triggered only once.
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              int remaining = buffer.remaining();
              buffer.position(remaining);
              return remaining;
            }).times(1);
    EasyMock.replay(mockFileChannel);
    final int writeBytes = 4096;
    final int numOfInt = writeBytes / Integer.BYTES;
    for (int i = 0; i < numOfInt; i++) {
      fileWriteOutBytes.writeInt(i);
    }
    // no need to flush up to 4KB
    // the first byte after 4KB will cause a flush
    fileWriteOutBytes.write(1);
    EasyMock.verify(mockFileChannel);
  }

  @Test
  public void writeShouldIncrementSize() throws IOException
  {
    fileWriteOutBytes.write(1);
    Assert.assertEquals(1, fileWriteOutBytes.size());
  }

  @Test
  public void writeIntShouldIncrementSize() throws IOException
  {
    fileWriteOutBytes.writeInt(1);
    Assert.assertEquals(4, fileWriteOutBytes.size());
  }

  @Test
  public void writeBufferLargerThanCapacityShouldIncrementSizeCorrectly() throws IOException
  {
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              int remaining = buffer.remaining();
              buffer.position(remaining);
              return remaining;
            }).times(1);
    EasyMock.replay(mockFileChannel);
    ByteBuffer src = ByteBuffer.allocate(4096 + 1);
    fileWriteOutBytes.write(src);
    Assert.assertEquals(src.capacity(), fileWriteOutBytes.size());
    EasyMock.verify(mockFileChannel);
  }

  @Test
  public void writeBufferLargerThanCapacityThrowsIOEInTheMiddleShouldIncrementSizeCorrectly() throws IOException
  {
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              int remaining = buffer.remaining();
              buffer.position(remaining);
              return remaining;
            }).once();
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andThrow(new IOException())
            .once();
    EasyMock.replay(mockFileChannel);
    ByteBuffer src = ByteBuffer.allocate(4096 * 2 + 1);
    try {
      fileWriteOutBytes.write(src);
      Assert.fail("IOException should have been thrown.");
    }
    catch (IOException e) {
      // The second invocation to flush bytes fails. So the size should count what has already been put successfully
      Assert.assertEquals(4096 * 2, fileWriteOutBytes.size());
    }
  }

  @Test
  public void writeBufferSmallerThanCapacityShouldIncrementSizeCorrectly() throws IOException
  {
    ByteBuffer src = ByteBuffer.allocate(4096);
    fileWriteOutBytes.write(src);
    Assert.assertEquals(src.capacity(), fileWriteOutBytes.size());
  }
  @Test
  public void sizeDoesNotFlush() throws IOException
  {
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andThrow(new AssertionError("file channel should not have been written to."));
    EasyMock.replay(mockFileChannel);
    long size = fileWriteOutBytes.size();
    Assert.assertEquals(0, size);
    fileWriteOutBytes.writeInt(10);
    size = fileWriteOutBytes.size();
    Assert.assertEquals(4, size);
  }
}
