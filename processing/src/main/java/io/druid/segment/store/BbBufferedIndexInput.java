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
 * bytebuffer based IndexInput utilize BufferedIndexInput
 */
public class BbBufferedIndexInput extends BufferedIndexInput
{

  private final long length;
  private final long limit;
  protected ByteBuffer byteBuffer;
  //indicate wherer this IndexInput is cloned ,default is false, the slice and duplicate operations will affect its value
  protected boolean isClone = false;

  public BbBufferedIndexInput(
      ByteBuffer buffer
  )
  {
    this.byteBuffer = buffer;
    this.length = buffer.capacity();
    this.limit = buffer.limit();
  }

  public BbBufferedIndexInput(
      ByteBuffer buffer, long length
  ) throws IOException
  {
    super(buffer.position(), BufferedIndexInput.BUFFER_SIZE);
    this.byteBuffer = buffer;
    this.length = length;
    this.limit = super.getFilePointer() + length;
  }


  @Override
  protected void seekInternal(long pos) throws IOException
  {
    this.byteBuffer.position((int)pos);
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
  public final BbBufferedIndexInput slice(long offset, long length) throws IOException
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


    BbBufferedIndexInput byteBufferIndexInput = buildSlice(offset, length);
    byteBufferIndexInput.isClone = true;
    return byteBufferIndexInput;
  }

  /**
   * duplicate according to the assigned 、fixed start position
   * @param startPosition
   * @return
   * @throws IOException
   */
  public IndexInput duplicateWithFixedPosition(long startPosition) throws IOException
  {
    long currentFp = startPosition;
    ByteBuffer duplicated = byteBuffer.duplicate();
    //to avoid the refill affect ,we got the real current file pointer
    duplicated.position((int)currentFp);
    BbBufferedIndexInput byteBufferIndexInput = new BbBufferedIndexInput(duplicated, duplicated.remaining());
    byteBufferIndexInput.isClone = true;
    return byteBufferIndexInput;
  }

  /**
   * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
   **/
  protected BbBufferedIndexInput buildSlice(long offset, long length) throws IOException
  {
    int truncatedOffset = (int) offset;
    int truncatedLength = (int) length;
    ByteBuffer duplicated = byteBuffer.duplicate();
    duplicated.position(truncatedOffset);
    duplicated.limit(truncatedOffset + truncatedLength);
    return new BbBufferedIndexInput(duplicated, truncatedLength);
  }


  @Override
  public final void close() throws IOException
  {
    super.close();
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

  @Override
  protected void readInternal(byte[] b, int offset, int length) throws IOException
  {
    byteBuffer.get(b,offset,length);
  }


}
