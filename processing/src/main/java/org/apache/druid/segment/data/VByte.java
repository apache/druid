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

package org.apache.druid.segment.data;

import java.nio.ByteBuffer;

public class VByte
{
  /**
   * Read a variable byte (vbyte) encoded integer from a {@link ByteBuffer} at the current position. Moves the buffer
   * ahead by 1 to 5 bytes depending on how many bytes was required to encode the integer value.
   *
   * vbyte encoding stores values in the last 7 bits of a byte and reserves the high bit for the 'contination'. If 0,
   * one or more aditional bytes must be read to complete the value, and a 1 indicates the terminal byte. Because of
   * this, it can only store positive values, and larger integers can take up to 5 bytes.
   *
   * implementation based on:
   * https://github.com/lemire/JavaFastPFOR/blob/master/src/main/java/me/lemire/integercompression/VariableByte.java
   *
   */
  public static int readInt(ByteBuffer buffer)
  {
    byte b;
    int v = (b = buffer.get()) & 0x7F;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 7) | v;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 14) | v;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 21) | v;
    if (b < 0) {
      return v;
    }
    v = ((buffer.get() & 0x7F) << 28) | v;
    return v;
  }

  /**
   * Write a variable byte (vbyte) encoded integer to a {@link ByteBuffer} at the current position, advancing the buffer
   * position by the number of bytes required to represent the integer, between 1 and 5 bytes.
   *
   * vbyte encoding stores values in the last 7 bits of a byte and reserves the high bit for the 'contination'. If 0,
   * one or more aditional bytes must be read to complete the value, and a 1 indicates the terminal byte. Because of
   * this, it can only store positive values, and larger integers can take up to 5 bytes.
   *
   * implementation based on:
   * https://github.com/lemire/JavaFastPFOR/blob/master/src/main/java/me/lemire/integercompression/VariableByte.java
   *
   */
  public static int writeInt(ByteBuffer buffer, int val)
  {
    final int pos = buffer.position();
    if (val < (1 << 7)) {
      buffer.put((byte) (val | (1 << 7)));
    } else if (val < (1 << 14)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) (extract7bitsmaskless(1, (val)) | (1 << 7)));
    } else if (val < (1 << 21)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) (extract7bitsmaskless(2, (val)) | (1 << 7)));
    } else if (val < (1 << 28)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) extract7bits(2, val));
      buffer.put((byte) (extract7bitsmaskless(3, (val)) | (1 << 7)));
    } else {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) extract7bits(2, val));
      buffer.put((byte) extract7bits(3, val));
      buffer.put((byte) (extract7bitsmaskless(4, (val)) | (1 << 7)));
    }
    return buffer.position() - pos;
  }

  /**
   * Compute number of bytes required to represent variable byte encoded integer.
   *
   * vbyte encoding stores values in the last 7 bits of a byte and reserves the high bit for the 'contination'. If 0,
   * one or more aditional bytes must be read to complete the value, and a 1 indicates the terminal byte. Because of
   * this, it can only store positive values, and larger integers can take up to 5 bytes.
   */
  public static int computeIntSize(int val)
  {
    if (val < (1 << 7)) {
      return 1;
    } else if (val < (1 << 14)) {
      return 2;
    } else if (val < (1 << 21)) {
      return 3;
    } else if (val < (1 << 28)) {
      return 4;
    } else {
      return 5;
    }
  }

  private static byte extract7bits(int i, int val)
  {
    return (byte) ((val >> (7 * i)) & ((1 << 7) - 1));
  }

  private static byte extract7bitsmaskless(int i, int val)
  {
    return (byte) ((val >> (7 * i)));
  }
}
