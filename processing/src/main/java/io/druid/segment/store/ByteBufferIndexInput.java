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


import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * this class use ByteBuffer to act as an IndexInput
 * Expert: as Druid index has an file size of Interger.MAX_SIZE,
 * we now can safety transfer long to int ,though it's a tricky
 */
public class ByteBufferIndexInput extends IndexInput
{

  private final long length;
  private final long limit;
  protected ByteBuffer byteBuffer;
  //indicate wherer this IndexInput is cloned ,default is false, the slice and duplicate operations will affect its value
  protected boolean isClone = false;

  public ByteBufferIndexInput(
      ByteBuffer buffer
  )
  {
    this.byteBuffer = buffer;
    this.length = buffer.capacity();
    this.limit = buffer.limit();
  }

  public ByteBufferIndexInput(
      ByteBuffer buffer, long length
  )
  {
    this.byteBuffer = buffer;
    this.length = length;
    this.limit = this.getFilePointer() + length;
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
  public long getFilePointer()
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
    return length;
  }

  @Override
  public boolean hasRemaining() throws IOException

  {
    return remaining() > 0;
  }

  @Override
  public long remaining() throws IOException
  {
    return limit - getFilePointer();
  }


  @Override
  public final ByteBufferIndexInput slice(long offset, long length) throws IOException
  {

    long size = this.length();
    if (offset < 0 || length < 0 || length > size) {
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


    ByteBufferIndexInput byteBufferIndexInput = buildSlice(offset, length);
    byteBufferIndexInput.isClone = true;
    return byteBufferIndexInput;
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
    ByteBufferIndexInput byteBufferIndexInput = new ByteBufferIndexInput(duplicated);
    byteBufferIndexInput.isClone = true;
    return byteBufferIndexInput;
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
    return new ByteBufferIndexInput(duplicated, truncatedLength);
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
      if (cleaner != null) {
        cleaner.clean();
      }
    }
    byteBuffer = null;
  }

  /**
   * override for performance
   *
   * @return
   *
   * @throws IOException
   */
  @Override
  public RandomAccessInput randomAccess() throws IOException
  {
    final ByteBuffer duplicate = this.byteBuffer.duplicate();
    //to be consistent with DataInput
    duplicate.order(ByteOrder.BIG_ENDIAN);
    RandomAccessInput randomAccessInput = new RandomAccessInput()
    {
      /**
       * Reads a byte at the given position in the file
       * @see DataInput#readByte
       */
      public byte readByte(long pos) throws IOException
      {
        return duplicate.get((int) pos);
      }

      /**
       * Reads a short at the given position in the file
       * @see DataInput#readShort
       */
      public short readShort(long pos) throws IOException
      {
        return duplicate.getShort((int) pos);
      }

      /**
       * Reads an integer at the given position in the file
       * @see DataInput#readInt
       */
      public int readInt(long pos) throws IOException
      {
        return duplicate.getInt((int) pos);
      }

      /**
       * Reads a long at the given position in the file
       * @see DataInput#readLong
       */
      public long readLong(long pos) throws IOException
      {
        return duplicate.getLong((int) pos);
      }
    };
    return randomAccessInput;
  }


}
