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

package org.apache.druid.compressedbigdecimal;

import java.nio.ByteBuffer;

/**
 * A compressed big decimal that holds its data with an embedded array.
 */
@SuppressWarnings("serial")
public class ByteBufferCompressedBigDecimal extends CompressedBigDecimal<ByteBufferCompressedBigDecimal>
{

  private final ByteBuffer buf;
  private final int position;
  private final int size;

  /**
   * Construct an AccumulatingBigDecimal using the referenced initial
   * value and scale.
   *
   * @param buf      The byte buffer to wrap
   * @param position the position in the byte buffer where the data should be stored
   * @param size     the size (in ints) of the byte buffer
   * @param scale    the scale to use
   */
  public ByteBufferCompressedBigDecimal(ByteBuffer buf, int position, int size, int scale)
  {
    super(scale);
    this.buf = buf;
    this.position = position;
    this.size = size;
  }

  /**
   * Construct a CompressedBigDecimal that uses a ByteBuffer for its storage and whose
   * initial value is copied from the specified CompressedBigDecimal.
   *
   * @param buf      the ByteBuffer to use for storage
   * @param position the position in the ByteBuffer
   * @param val      initial value
   */
  public ByteBufferCompressedBigDecimal(ByteBuffer buf, int position, CompressedBigDecimal<?> val)
  {
    super(val.getScale());
    this.buf = buf;
    this.position = position;
    this.size = val.getArraySize();

    copyToBuffer(buf, position, size, val);
  }

  /* (non-Javadoc)
   * @see org.apache.druid.compressedbigdecimal.CompressedBigDecimal#getArraySize()
   */
  @Override
  public int getArraySize()
  {
    return size;
  }

  /**
   * Package private access to entry in internal array.
   *
   * @param idx index to retrieve
   * @return the entry
   */
  @Override
  protected int getArrayEntry(int idx)
  {
    return buf.getInt(position + idx * Integer.BYTES);
  }

  /**
   * Package private access to set entry in internal array.
   *
   * @param idx index to retrieve
   * @param val value to set
   */
  @Override
  protected void setArrayEntry(int idx, int val)
  {
    buf.putInt(position + idx * Integer.BYTES, val);
  }

  /**
   * Copy a compressed big decimal into a Bytebuffer in a format understood by this class.
   *
   * @param buf      The buffer
   * @param position The position in the buffer to place the value
   * @param size     The space (in number of ints) allocated for the value
   * @param val      THe value to copy
   */
  public static void copyToBuffer(ByteBuffer buf, int position, int size, CompressedBigDecimal<?> val)
  {
    if (val.getArraySize() > size) {
      throw new IllegalArgumentException("Right hand side too big to fit in the result value");
    }
    for (int ii = 0; ii < size; ++ii) {
      buf.putInt(position + ii * Integer.BYTES, val.getArrayEntry(ii));
    }
  }

}
