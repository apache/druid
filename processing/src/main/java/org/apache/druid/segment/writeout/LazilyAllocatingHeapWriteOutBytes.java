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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.input.NullInputStream;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Supplier;

/**
 * Lazily decides to use a tmpBuffer to act as WriteOutBytes, till more than certain threshold is reached. Once this is
 * met, it switches to delegating all calls to a {@link WriteOutBytes} created by delegateSupplier.
 * <p>
 * This is useful if the data stored in the {@link WriteOutBytes} is small enough that buffering the changes in memory
 * would be faster than creating some {@link WriteOutBytes}.
 */
public class LazilyAllocatingHeapWriteOutBytes extends WriteOutBytes
{
  private final Supplier<WriteOutBytes> delegateSupplier;

  private ByteBuffer tmpBuffer = null;
  private WriteOutBytes delegate = null;
  private boolean open = true;

  public LazilyAllocatingHeapWriteOutBytes(Supplier<WriteOutBytes> delegateSupplier, Closer closer)
  {
    this.delegateSupplier = delegateSupplier;
    closer.register(() -> {
      open = false;
      tmpBuffer = null;
      delegate = null;
    });
  }

  @Override
  public void writeInt(int v) throws IOException
  {
    checkOpen();
    final boolean useBuffer = ensureBytes(Integer.BYTES);
    if (useBuffer) {
      tmpBuffer.putInt(v);
      return;
    }

    delegate.writeInt(v);
  }

  @Override
  public long size()
  {
    if (delegate == null) {
      return tmpBuffer == null ? 0 : tmpBuffer.position();
    } else {
      return delegate.size();
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    checkOpen();
    if (delegate == null) {
      if (tmpBuffer == null) {
        return;
      }

      final ByteBuffer tmpBufCopy = tmpBuffer.asReadOnlyBuffer();
      tmpBufCopy.flip();
      channel.write(tmpBufCopy);
    } else {
      delegate.writeTo(channel);
    }
  }

  @Override
  public InputStream asInputStream() throws IOException
  {
    checkOpen();
    if (delegate == null) {
      if (tmpBuffer == null) {
        return new NullInputStream();
      }
      final ByteBuffer tmpBufCopy = tmpBuffer.asReadOnlyBuffer();
      tmpBufCopy.flip();
      return new ByteBufferInputStream(tmpBufCopy);
    } else {
      return delegate.asInputStream();
    }
  }

  @Override
  public void readFully(long pos, ByteBuffer buffer) throws IOException
  {
    checkOpen();
    if (delegate == null) {
      if (tmpBuffer == null) {
        return;
      }

      if (pos <= tmpBuffer.position()) {
        final ByteBuffer tmpBufCopy = tmpBuffer.asReadOnlyBuffer();
        tmpBufCopy.flip().position((int) pos);
        if (tmpBufCopy.remaining() < buffer.remaining()) {
          throw new BufferUnderflowException();
        }
        tmpBufCopy.limit(tmpBufCopy.position() + buffer.remaining());
        buffer.put(tmpBufCopy);
      } else {
        throw new BufferOverflowException();
      }
    } else {
      delegate.readFully(pos, buffer);
    }
  }

  @Override
  public void write(int b) throws IOException
  {
    checkOpen();
    final boolean useBuffer = ensureBytes(1);
    if (useBuffer) {
      tmpBuffer.put((byte) b);
      return;
    }

    delegate.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    write(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public int write(ByteBuffer src) throws IOException
  {
    checkOpen();
    final int numToWrite = src.remaining();
    final boolean useBuffer = ensureBytes(numToWrite);
    if (useBuffer) {
      tmpBuffer.put(src);
      return numToWrite;
    }

    return delegate.write(src);
  }

  @Override
  public boolean isOpen()
  {
    return open;
  }

  private void checkOpen()
  {
    if (!isOpen()) {
      throw DruidException.defensive("WriteOutBytes is already closed.");
    }
  }

  /**
   * Ensures bytes are available, returns a boolean indicating if the buffer should be used or the delegate
   *
   * @param numBytes the number of bytes that need writing
   * @return boolean true if the buffer should be used, false if the delegate should be used
   * @throws IOException if an issue with IO occurs (can primarily happen when copying buffers)
   */
  private boolean ensureBytes(int numBytes) throws IOException
  {
    if (tmpBuffer == null) {
      if (delegate == null) {
        if (numBytes < 128) {
          tmpBuffer = ByteBuffer.allocate(128);
        } else if (numBytes < 4096) {
          tmpBuffer = ByteBuffer.allocate(4096);
        } else {
          // We are likely dealing with something that's already buffering stuff, so just switch delegate immediately
          delegate = delegateSupplier.get();
          return false;
        }
        return true;
      } else {
        return false;
      }
    }

    if (numBytes < tmpBuffer.remaining()) {
      return true;
    }

    final ByteBuffer newBuf;
    switch (tmpBuffer.capacity()) {
      case 128:
        if (numBytes < 4096 - tmpBuffer.position()) {
          newBuf = ByteBuffer.allocate(4096);
          break;
        }
      case 4096:
        if (numBytes < 16384 - tmpBuffer.position()) {
          newBuf = ByteBuffer.allocate(16384);
          break;
        }
      default:
        newBuf = null;
    }

    if (newBuf == null) {
      delegate = delegateSupplier.get();
      tmpBuffer.flip();
      delegate.write(tmpBuffer);
      tmpBuffer = null;
      return false;
    } else {
      tmpBuffer.flip();
      newBuf.put(tmpBuffer);
      tmpBuffer = newBuf;
      return true;
    }
  }

  @VisibleForTesting
  ByteBuffer getTmpBuffer()
  {
    return tmpBuffer;
  }

  @VisibleForTesting
  WriteOutBytes getDelegate()
  {
    return delegate;
  }
}
