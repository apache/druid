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

import com.google.common.io.ByteStreams;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

final class FileWriteOutBytes extends WriteOutBytes
{
  private final File file;
  private final FileChannel ch;
  private long writeOutBytes;

  /** Purposely big-endian, for {@link #writeInt(int)} implementation */
  private final ByteBuffer buffer = ByteBuffer.allocate(4096); // 4K page sized buffer

  FileWriteOutBytes(File file, FileChannel ch)
  {
    this.file = file;
    this.ch = ch;
    this.writeOutBytes = 0L;
  }
  
  private void flushIfNeeded(int bytesNeeded) throws IOException
  {
    if (buffer.remaining() < bytesNeeded) {
      flush();
    }
  }

  @Override
  public void flush() throws IOException
  {
    buffer.flip();
    Channels.writeFully(ch, buffer);
    buffer.clear();
  }

  @Override
  public void write(int b) throws IOException
  {
    flushIfNeeded(1);
    buffer.put((byte) b);
    writeOutBytes++;
  }

  @Override
  public void writeInt(int v) throws IOException
  {
    flushIfNeeded(Integer.BYTES);
    buffer.putInt(v);
    writeOutBytes += Integer.BYTES;
  }

  @Override
  public int write(ByteBuffer src) throws IOException
  {
    int len = src.remaining();
    flushIfNeeded(len);
    while (src.remaining() > buffer.capacity()) {
      int srcLimit = src.limit();
      try {
        src.limit(src.position() + buffer.capacity());
        buffer.put(src);
        writeOutBytes += buffer.capacity();
        flush();
      }
      finally {
        // IOException may occur in flush(), reset src limit to the original
        src.limit(srcLimit);
      }
    }
    int remaining = src.remaining();
    buffer.put(src);
    writeOutBytes += remaining;
    return len;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    write(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public long size()
  {
    return writeOutBytes;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    flush();
    ch.position(0);
    try {
      ByteStreams.copy(ch, channel);
    }
    finally {
      ch.position(ch.size());
    }
  }

  @Override
  public void readFully(long pos, ByteBuffer buffer) throws IOException
  {
    flush();
    if (pos < 0 || pos > ch.size()) {
      throw new IAE("pos %d out of range [%d, %d]", pos, 0, ch.size());
    }
    ch.read(buffer, pos);
    if (buffer.remaining() > 0) {
      throw new BufferUnderflowException();
    }
  }

  @Override
  public InputStream asInputStream() throws IOException
  {
    flush();
    return new FileInputStream(file);
  }

  @Override
  public boolean isOpen()
  {
    return ch.isOpen();
  }
}
