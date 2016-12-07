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

/**
 * a high level abstract class for Druid to write index data
 * it's not thread safe
 */
public abstract class DataOutput
{

  /**
   * Writes a single byte.
   * <p>
   * The most primitive data type is an eight-bit byte. Files are
   * accessed as sequences of bytes. All other data types are defined
   * as sequences of bytes, big-endian ,so file formats are byte-order independent.
   *
   * @see IndexInput#readByte()
   */
  public abstract void writeByte(byte b) throws IOException;

  /**
   * Writes an array of bytes.
   *
   * @param b      the bytes to write
   * @param length the number of bytes to write
   *
   * @see DataInput#readBytes(byte[], int, int)
   */
  public void writeBytes(byte[] b, int length) throws IOException
  {
    writeBytes(b, 0, length);
  }

  /**
   * Writes an array of bytes.
   *
   * @param b      the bytes to write
   * @param offset the offset in the byte array
   * @param length the number of bytes to write
   *
   * @see DataInput#readBytes(byte[], int, int)
   */
  public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

  /**
   * Writes an int as four bytes.
   * <p>
   * 32-bit unsigned integer written as four bytes, high-order bytes first.
   *
   * @see DataInput#readInt()
   */
  public final void writeInt(int i) throws IOException
  {
    writeByte((byte) (i >> 24));
    writeByte((byte) (i >> 16));
    writeByte((byte) (i >> 8));
    writeByte((byte) i);
  }

  /**
   * Writes a short as two bytes.
   *
   * @see DataInput#readShort()
   */
  public final void writeShort(short i) throws IOException
  {
    writeByte((byte) (i >> 8));
    writeByte((byte) i);
  }


  /**
   * Writes a long as eight bytes.
   * <p>
   * 64-bit unsigned integer written as eight bytes, high-order bytes first.
   *
   * @see DataInput#readLong()
   */
  public final void writeLong(long i) throws IOException
  {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }


  private static int COPY_BUFFER_SIZE = 16384;
  private byte[] copyBuffer;

  /**
   * Copy numBytes bytes from input to ourself.
   */
  public void copyBytes(DataInput input, long numBytes) throws IOException
  {
    assert numBytes >= 0 : "numBytes=" + numBytes;
    long left = numBytes;
    if (copyBuffer == null) {
      copyBuffer = new byte[COPY_BUFFER_SIZE];
    }
    while (left > 0) {
      final int toCopy;
      if (left > COPY_BUFFER_SIZE) {
        toCopy = COPY_BUFFER_SIZE;
      } else {
        toCopy = (int) left;
      }
      input.readBytes(copyBuffer, 0, toCopy);
      writeBytes(copyBuffer, 0, toCopy);
      left -= toCopy;
    }
  }

}
