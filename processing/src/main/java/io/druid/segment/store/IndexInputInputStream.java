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
package io.druid.segment.store;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class IndexInputInputStream extends InputStream
{

  private IndexInput indexInput;

  public IndexInputInputStream(IndexInput indexInput)
  {
    this.indexInput = indexInput;
  }

  @Override
  public int read() throws IOException
  {
    return indexInput.readByte();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException
  {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    int remaining = (int) indexInput.remaining();
    if (remaining <= 0) {
      return -1;
    }
    if (len <= 0) {
      return 0;
    }
    if (len >= remaining) {
      len = remaining;
    }
    indexInput.readBytes(b, off, len);
    return len;
  }

  @Override
  public void close() throws IOException
  {
    indexInput.close();
  }

  /**
   * Returns an estimate of the number of bytes that can be read (or
   * skipped over) from this input stream without blocking by the next
   * invocation of a method for this input stream. The next invocation
   * might be the same thread or another thread.  A single read or skip of this
   * many bytes will not block, but may read or skip fewer bytes.
   * <p>
   * <p> Note that while some implementations of {@code InputStream} will return
   * the total number of bytes in the stream, many will not.  It is
   * never correct to use the return value of this method to allocate
   * a buffer intended to hold all data in this stream.
   * <p>
   * <p> A subclass' implementation of this method may choose to throw an
   * {@link IOException} if this input stream has been closed by
   * invoking the {@link #close()} method.
   * <p>
   * <p> The {@code available} method for class {@code InputStream} always
   * returns {@code 0}.
   * <p>
   * <p> This method should be overridden by subclasses.
   *
   * @return an estimate of the number of bytes that can be read (or skipped
   * over) from this input stream without blocking or {@code 0} when
   * it reaches the end of the input stream.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public int available() throws IOException
  {
    return (int) indexInput.remaining();
  }

  public IndexInput getIndexInput()
  {
    return indexInput;
  }
}
