/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.io;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class OutputBytes extends OutputStream implements WritableByteChannel
{
  private static final int BUFFER_SIZE = 64 * 1024;
  private final ArrayList<ByteBuffer> buffers = new ArrayList<>();
  private int headBufferIndex;
  private ByteBuffer headBuffer;
  private long size;
  private long capacity;

  public OutputBytes()
  {
    headBufferIndex = 0;
    headBuffer = allocateBuffer();
    buffers.add(headBuffer);
    size = 0;
    capacity = BUFFER_SIZE;
  }

  public long size()
  {
    return size;
  }

  private ByteBuffer allocateBuffer()
  {
    return ByteBuffer.allocate(BUFFER_SIZE);
  }

  private void ensureCapacity(int len) {
    long remaining = capacity - size;
    for (long toAllocate = len - remaining; toAllocate >= 0; toAllocate -= BUFFER_SIZE) {
      buffers.add(allocateBuffer());
      capacity += BUFFER_SIZE;
    }
    if (headBuffer.remaining() == 0) {
      nextHead();
    }
  }

  private void nextHead()
  {
    headBufferIndex++;
    headBuffer = buffers.get(headBufferIndex);
  }

  @Override
  public void write(int b)
  {
    if (headBuffer.remaining() == 0) {
      ensureCapacity(1);
    }
    headBuffer.put((byte) b);
    size += 1;
  }

  public void writeInt(int v)
  {
    if (headBuffer.remaining() >= Ints.BYTES) {
      headBuffer.putInt(v);
      size += Ints.BYTES;
    } else {
      ensureCapacity(Ints.BYTES);
      if (headBuffer.remaining() >= Ints.BYTES) {
        headBuffer.putInt(v);
        size += Ints.BYTES;
      } else {
        write(v >> 24);
        write(v >> 16);
        write(v >> 8);
        write(v);
      }
    }
  }

  @Override
  public void write(byte[] b) throws IOException
  {
    write0(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    Preconditions.checkPositionIndexes(off, off + len, b.length);
    write0(b, off, len);
  }

  private void write0(byte[] b, int off, int len)
  {
    if (headBuffer.remaining() < len) {
      ensureCapacity(len);
    }
    int headRemaining = headBuffer.remaining();
    if (len <= headRemaining) {
      headBuffer.put(b, off, len);
    } else {
      headBuffer.put(b, off, headRemaining);
      int bytesLeft = len - headRemaining;
      off += headRemaining;
      for (; bytesLeft > 0; bytesLeft -= BUFFER_SIZE, off += BUFFER_SIZE) {
        nextHead();
        headBuffer.put(b, off, Math.min(BUFFER_SIZE, bytesLeft));
      }
    }
    size += len;
  }

  @Override
  public int write(ByteBuffer src)
  {
    int len = src.remaining();
    if (headBuffer.remaining() < len) {
      ensureCapacity(len);
    }
    int headRemaining = headBuffer.remaining();
    if (len <= headRemaining) {
      headBuffer.put(src);
    } else {
      src.limit(src.position() + headRemaining);
      headBuffer.put(src);
      int bytesLeft = len - headRemaining;
      for (; bytesLeft > 0; bytesLeft -= BUFFER_SIZE) {
        nextHead();
        src.limit(src.position() + Math.min(BUFFER_SIZE, bytesLeft));
        headBuffer.put(src);
      }
    }
    size += len;
    return len;
  }

  @Override
  public boolean isOpen()
  {
    return true;
  }

  public void writeTo(WritableByteChannel channel) throws IOException
  {
    for (int i = 0; i <= headBufferIndex; i++) {
      ByteBuffer buffer = buffers.get(i);
      buffer.flip();
      Channels.writeFully(channel, buffer);
      // switch back to the initial state
      buffer.limit(buffer.capacity());
    }
  }

  public void writeTo(ByteBuffer out)
  {
    for (int i = 0; i <= headBufferIndex; i++) {
      ByteBuffer buffer = buffers.get(i);
      buffer.flip();
      out.put(buffer);
      // switch back to the initial state
      buffer.limit(buffer.capacity());
    }
  }

  public InputStream asInputStream() throws IOException
  {
    Function<ByteBuffer, ByteSource> byteBufferToByteSource = buf -> new ByteSource()
    {
      @Override
      public InputStream openStream()
      {
        return new ByteArrayInputStream(buf.array(), 0, buf.position());
      }
    };
    return ByteSource.concat(buffers.stream().map(byteBufferToByteSource).collect(Collectors.toList())).openStream();
  }
}
