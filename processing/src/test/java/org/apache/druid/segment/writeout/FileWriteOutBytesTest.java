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

import org.apache.druid.java.util.common.IAE;
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
  private File mockFile;

  @Before
  public void setUp()
  {
    mockFileChannel = EasyMock.mock(FileChannel.class);
    mockFile = EasyMock.mock(File.class);
    fileWriteOutBytes = new FileWriteOutBytes(mockFile, mockFileChannel);
  }

  @Test
  public void write4KiBIntsShouldNotFlush() throws IOException
  {
    // Write 4KiB of ints and expect the write operation of the file channel will be triggered only once.
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
    // no need to flush up to 4KiB
    // the first byte after 4KiB will cause a flush
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

  @Test
  public void testReadFullyWorks() throws IOException
  {
    int fileSize = 4096;
    int numOfInt = fileSize / Integer.BYTES;
    ByteBuffer destination = ByteBuffer.allocate(Integer.BYTES);
    ByteBuffer underlying = ByteBuffer.allocate(fileSize);
    // Write 4KiB of ints and expect the write operation of the file channel will be triggered only once.
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              underlying.position(0);
              underlying.put(buffer);
              return 0;
            }).times(1);
    EasyMock.expect(mockFileChannel.read(EasyMock.eq(destination), EasyMock.eq(100L * Integer.BYTES)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              long pos = (long) EasyMock.getCurrentArguments()[1];
              buffer.putInt(underlying.getInt((int) pos));
              return Integer.BYTES;
            }).times(1);
    EasyMock.replay(mockFileChannel);
    for (int i = 0; i < numOfInt; i++) {
      fileWriteOutBytes.writeInt(i);
    }
    Assert.assertEquals(underlying.capacity(), fileWriteOutBytes.size());

    destination.position(0);
    fileWriteOutBytes.readFully(100L * Integer.BYTES, destination);
    destination.position(0);
    Assert.assertEquals(100, destination.getInt());
    EasyMock.verify(mockFileChannel);
  }

  @Test
  public void testReadFullyOutOfBoundsDoesnt() throws IOException
  {
    int fileSize = 4096;
    int numOfInt = fileSize / Integer.BYTES;
    ByteBuffer destination = ByteBuffer.allocate(Integer.BYTES);
    EasyMock.replay(mockFileChannel);
    for (int i = 0; i < numOfInt; i++) {
      fileWriteOutBytes.writeInt(i);
    }
    Assert.assertEquals(fileSize, fileWriteOutBytes.size());

    destination.position(0);
    Assert.assertThrows(IAE.class, () -> fileWriteOutBytes.readFully(5000, destination));
    EasyMock.verify(mockFileChannel);
  }

  @Test
  public void testIOExceptionHasFileInfo() throws Exception
  {
    IOException cause = new IOException("Too many bytes");
    EasyMock.expect(mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class))).andThrow(cause);
    EasyMock.expect(mockFile.getAbsolutePath()).andReturn("/tmp/file");
    EasyMock.replay(mockFileChannel, mockFile);
    fileWriteOutBytes.writeInt(10);
    fileWriteOutBytes.write(new byte[30]);
    IOException actual = Assert.assertThrows(IOException.class, () -> fileWriteOutBytes.flush());
    Assert.assertEquals(String.valueOf(actual.getCause()), actual.getCause(), cause);
    Assert.assertEquals(actual.getMessage(), actual.getMessage(), "Failed to write to file: /tmp/file. Current size of file: 34");
  }
}
