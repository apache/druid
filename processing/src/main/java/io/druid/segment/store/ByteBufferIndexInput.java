/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.segment.store;


import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * this class use ByteBuffer to act as an IndexInput
 * Expert: as Druid index has an file size of Interger.MAX_SIZE,
 * we now can safety transfer long to int ,though it's a tricky
 */
public class ByteBufferIndexInput extends IndexInput
{

  protected ByteBuffer byteBuffer;
  //indicate wherer this IndexInput is cloned ,default is false, the slice and duplicate operations will affect its value
  protected boolean isClone = false;

  public ByteBufferIndexInput(
      ByteBuffer buffer
  )
  {
    this.byteBuffer = buffer;
  }


  @Override
  public final byte readByte() throws IOException
  {
    return byteBuffer.get();
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException
  {
    byteBuffer.get(b, offset, len);
  }

  @Override
  public final short readShort() throws IOException
  {
    return byteBuffer.getShort();

  }

  @Override
  public final int readInt() throws IOException
  {
    return byteBuffer.getInt();
  }

  @Override
  public final long readLong() throws IOException
  {
    return byteBuffer.getLong();
  }

  @Override
  public long getFilePointer() throws IOException
  {
    return byteBuffer.position();
  }

  @Override
  public void seek(long pos) throws IOException
  {
    byteBuffer.position((int) pos);
  }


  @Override
  public final long length() throws IOException
  {
    return byteBuffer.capacity();
  }

  @Override
  public boolean hasRemaining() throws IOException
  {
    return byteBuffer.hasRemaining();
  }


  /**
   * Creates a slice of this index input, with the given offset, and length.
   * The sliced part will be independent to the origin one.
   *
   * @param offset file point where to slice the input
   * @param length number of bytes to be sliced to the new IndexInput
   */
  @Override
  public final ByteBufferIndexInput slice(long offset, long length) throws IOException
  {
    long size = this.length();
    if (offset < 0 || length < 0 || offset + length > size) {
      throw new IllegalArgumentException("slice() "
                                         + " out of bounds: offset="
                                         + offset
                                         + ",length="
                                         + length
                                         + ",fileLength="
                                         + size
                                         + ": "
                                         + this);
    }

    return buildSlice(offset, length);
  }

  /**
   * Creats a copy of this index input,which have the same content 、file point position .
   * but the file point is independent
   * somehow
   *
   * @return
   */
  @Override
  public IndexInput duplicate() throws IOException
  {
    ByteBuffer duplicated = byteBuffer.duplicate();
    return new ByteBufferIndexInput(duplicated);
  }

  /**
   * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
   **/
  protected ByteBufferIndexInput buildSlice(long offset, long length)
  {
    int truncatedOffset = (int) offset;
    int truncatedLength = (int) length;
    ByteBuffer duplicated = byteBuffer.duplicate();
    duplicated.position(truncatedOffset);
    duplicated.limit(truncatedOffset + truncatedLength);
    return new ByteBufferIndexInput(duplicated);
  }


  @Override
  public final void close() throws IOException
  {
    if (isClone) {
      return;
    }
    if (byteBuffer == null) {
      return;
    }
    if (byteBuffer instanceof DirectBuffer) {
      DirectBuffer directBuffer = (DirectBuffer) byteBuffer;
      Cleaner cleaner = directBuffer.cleaner();
      cleaner.clean();
    }
    byteBuffer = null;
  }


}
