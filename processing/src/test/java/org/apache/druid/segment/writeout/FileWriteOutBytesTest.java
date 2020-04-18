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
    this.mockFileChannel = EasyMock.mock(FileChannel.class);
    this.fileWriteOutBytes = new FileWriteOutBytes(EasyMock.mock(File.class), mockFileChannel);
  }

  @Test
  public void testWrite4KBInts() throws IOException
  {
    // Write 4KB of ints and expect the write operation of the file channel will be triggered only once.
    EasyMock.expect(this.mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              int remaining = buffer.remaining();
              buffer.position(remaining);
              return remaining;
            }).times(1);
    EasyMock.replay(this.mockFileChannel);
    final int writeBytes = 4096;
    final int numOfInt = writeBytes / Integer.BYTES;
    for (int i = 0; i < numOfInt; i++) {
      this.fileWriteOutBytes.writeInt(i);
    }
    this.fileWriteOutBytes.flush();
    EasyMock.verify(this.mockFileChannel);
  }
}
