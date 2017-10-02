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

package io.druid.output;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;
import io.druid.io.ByteBufferInputStream;
import io.druid.io.Channels;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class ByteBufferOutputBytes extends OutputBytes
{
  /**
   * There is no reason why 64K is chosen. Other power of 2 between 4K and 64K could be more reasonable.
   */
  static final int BUFFER_SIZE = 64 * 1024;

  final ArrayList<ByteBuffer> buffers = new ArrayList<>();
  int headBufferIndex;
  ByteBuffer headBuffer;
  long size;
  long capacity;

  ByteBufferOutputBytes()
  {
    size = 0;
    headBufferIndex = 0;
    headBuffer = allocateBuffer();
    buffers.add(headBuffer);
    capacity = BUFFER_SIZE;
  }

  @Override
  public long size()
  {
    return size;
  }

  protected abstract ByteBuffer allocateBuffer();

  private void ensureCapacity(int len)
  {
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
    checkOpen();
    if (headBuffer.remaining() == 0) {
      ensureCapacity(1);
    }
    headBuffer.put((byte) b);
    size += 1;
  }

  @Override
  public void writeInt(int v)
  {
    checkOpen();
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
    checkOpen();
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
    checkOpen();
    int len = src.remaining();
    if (headBuffer.remaining() < len) {
      ensureCapacity(len);
    }
    int headRemaining = headBuffer.remaining();
    src.limit(src.position() + Math.min(headRemaining, len));
    headBuffer.put(src);
    for (int bytesLeft = len - headRemaining; bytesLeft > 0; bytesLeft -= BUFFER_SIZE) {
      nextHead();
      src.limit(src.position() + Math.min(BUFFER_SIZE, bytesLeft));
      headBuffer.put(src);
    }
    size += len;
    return len;
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    checkOpen();
    for (int i = 0; i <= headBufferIndex; i++) {
      ByteBuffer buffer = buffers.get(i);
      buffer.flip();
      Channels.writeFully(channel, buffer);
      // switch back to the initial state
      buffer.limit(buffer.capacity());
    }
  }

  /**
   * Takes all bytes that are written to this OutputBytes so far and writes them into the given ByteBuffer. This method
   * changes the position of the out buffer by the {@link #size()} of this OutputBytes.
   *
   * @throws java.nio.BufferOverflowException if the {@link ByteBuffer#remaining()} capacity of the given buffer is
   * smaller than the size of this OutputBytes
   */
  public void writeTo(ByteBuffer out)
  {
    checkOpen();
    for (int i = 0; i <= headBufferIndex; i++) {
      ByteBuffer buffer = buffers.get(i);
      buffer.flip();
      out.put(buffer);
      // switch back to the initial state
      buffer.limit(buffer.capacity());
    }
  }

  @Override
  public InputStream asInputStream() throws IOException
  {
    checkOpen();
    Function<ByteBuffer, ByteSource> byteBufferToByteSource = buf -> new ByteSource()
    {
      @Override
      public InputStream openStream()
      {
        ByteBuffer inputBuf = buf.duplicate();
        inputBuf.flip();
        return new ByteBufferInputStream(inputBuf);
      }
    };
    return ByteSource.concat(buffers.stream().map(byteBufferToByteSource).collect(Collectors.toList())).openStream();
  }

  @Override
  public boolean isOpen()
  {
    return true;
  }

  private void checkOpen()
  {
    if (!isOpen()) {
      throw new IllegalStateException();
    }
  }
}
