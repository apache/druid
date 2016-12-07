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
 * abstract class for druid to read low level data from different storage,
 * this DataInput may be a buffered input or a streamed input,and so on.
 * The short ,int,long type byte code is big endian
 */
public abstract class DataInput
{


  /**
   * Reads and returns a single byte.
   *
   * @see DataOutput#writeByte(byte)
   */
  public abstract byte readByte() throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param b      the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len    the number of bytes to read
   *
   * @see DataOutput#writeBytes(byte[], int)
   */
  public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

  /**
   * Reads two bytes and returns a short.
   *
   * @see DataOutput#writeByte(byte)
   */
  public final short readShort() throws IOException
  {
    return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
  }

  /**
   * Reads four bytes and returns an int.
   *
   * @see DataOutput#writeInt(int)
   */
  public final int readInt() throws IOException
  {
    return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
           | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
  }


  /**
   * Reads eight bytes and returns a long.
   *
   * @see DataOutput#writeLong(long)
   */
  public final long readLong() throws IOException
  {
    return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
  }


}
