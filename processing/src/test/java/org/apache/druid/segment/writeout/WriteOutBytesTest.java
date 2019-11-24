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

import com.google.common.primitives.Ints;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WriteOutBytesTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[] {new TmpFileSegmentWriteOutMedium(FileUtils.createTempDir())},
        new Object[] {new OffHeapMemorySegmentWriteOutMedium()},
        new Object[] {new OnHeapMemorySegmentWriteOutMedium()}
    );
  }

  private final SegmentWriteOutMedium segmentWriteOutMedium;

  public WriteOutBytesTest(SegmentWriteOutMedium segmentWriteOutMedium)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
  }

  @Test
  public void testWriteOutBytes() throws IOException
  {
    WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();

    writeOutBytes.write('1');
    verifyContents(writeOutBytes, "1");

    writeOutBytes.writeInt(Ints.fromBytes((byte) '2', (byte) '3', (byte) '4', (byte) '5'));
    verifyContents(writeOutBytes, "12345");

    writeOutBytes.write(new byte[] {'a'});
    verifyContents(writeOutBytes, "12345a");

    writeOutBytes.write(new byte[] {'a', 'b', 'c'}, 1, 1);
    verifyContents(writeOutBytes, "12345ab");

    ByteBuffer bb = ByteBuffer.wrap(new byte[]{'a', 'b', 'c'});
    bb.position(2);
    writeOutBytes.write(bb);
    Assert.assertEquals(3, bb.position());
    verifyContents(writeOutBytes, "12345abc");
  }

  private void verifyContents(WriteOutBytes writeOutBytes, String expected) throws IOException
  {
    Assert.assertEquals(expected, IOUtils.toString(writeOutBytes.asInputStream(), StandardCharsets.US_ASCII));
    ByteBuffer bb = ByteBuffer.allocate((int) writeOutBytes.size());
    writeOutBytes.readFully(0, bb);
    bb.flip();
    Assert.assertEquals(expected, StringUtils.fromUtf8(bb));
  }

  @Test
  public void testCrossBufferRandomAccess() throws IOException
  {
    WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    for (int i = 0; i < ByteBufferWriteOutBytes.BUFFER_SIZE; i++) {
      writeOutBytes.write('0');
    }
    writeOutBytes.write('1');
    writeOutBytes.write('2');
    writeOutBytes.write('3');
    ByteBuffer bb = ByteBuffer.allocate(4);
    writeOutBytes.readFully(ByteBufferWriteOutBytes.BUFFER_SIZE - 1, bb);
    bb.flip();
    Assert.assertEquals("0123", StringUtils.fromUtf8(bb));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testReadFullyUnderflow() throws IOException
  {
    WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    writeOutBytes.write('1');
    writeOutBytes.readFully(0, ByteBuffer.allocate(2));
  }

  @Test
  public void testReadFullyEmptyAtTheEnd() throws IOException
  {
    WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    writeOutBytes.write('1');
    writeOutBytes.readFully(1, ByteBuffer.allocate(0));
  }
}
