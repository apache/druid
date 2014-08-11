/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.hyperloglog;

import java.nio.ByteBuffer;

/**
 */
public class HLLCV1 extends HyperLogLogCollector
{
  /**
   * Header:
   * Byte 0: version
   * Byte 1: registerOffset
   * Byte 2-3: numNonZeroRegisters
   * Byte 4: maxOverflowValue
   * Byte 5-6: maxOverflowRegister
   */
  public static final byte VERSION = 0x1;
  public static final int REGISTER_OFFSET_BYTE = 1;
  public static final int NUM_NON_ZERO_REGISTERS_BYTE = 2;
  public static final int MAX_OVERFLOW_VALUE_BYTE = 4;
  public static final int MAX_OVERFLOW_REGISTER_BYTE = 5;
  public static final int HEADER_NUM_BYTES = 7;
  public static final int NUM_BYTES_FOR_DENSE_STORAGE = NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES;

  private static final ByteBuffer defaultStorageBuffer = ByteBuffer.wrap(new byte[]{VERSION, 0, 0, 0, 0, 0, 0})
                                                                   .asReadOnlyBuffer();

  protected HLLCV1()
  {
    super(defaultStorageBuffer);
  }

  protected HLLCV1(ByteBuffer buffer)
  {
    super(buffer);
  }

  @Override
  public byte getVersion()
  {
    return VERSION;
  }

  @Override
  public void setVersion(ByteBuffer buffer)
  {
    buffer.put(buffer.position(), VERSION);
  }

  @Override
  public byte getRegisterOffset()
  {
    return getStorageBuffer().get(getInitPosition() + REGISTER_OFFSET_BYTE);
  }

  @Override
  public void setRegisterOffset(byte registerOffset)
  {
    getStorageBuffer().put(getInitPosition() + REGISTER_OFFSET_BYTE, registerOffset);
  }

  @Override
  public void setRegisterOffset(ByteBuffer buffer, byte registerOffset)
  {
    buffer.put(buffer.position() + REGISTER_OFFSET_BYTE, registerOffset);
  }

  @Override
  public short getNumNonZeroRegisters()
  {
    return getStorageBuffer().getShort(getInitPosition() + NUM_NON_ZERO_REGISTERS_BYTE);
  }

  @Override
  public void setNumNonZeroRegisters(short numNonZeroRegisters)
  {
    getStorageBuffer().putShort(getInitPosition() + NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
  }

  @Override
  public void setNumNonZeroRegisters(ByteBuffer buffer, short numNonZeroRegisters)
  {
    buffer.putShort(buffer.position() + NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
  }

  @Override
  public byte getMaxOverflowValue()
  {
    return getStorageBuffer().get(getInitPosition() + MAX_OVERFLOW_VALUE_BYTE);
  }

  @Override
  public void setMaxOverflowValue(byte value)
  {
    getStorageBuffer().put(getInitPosition() + MAX_OVERFLOW_VALUE_BYTE, value);
  }

  @Override
  public void setMaxOverflowValue(ByteBuffer buffer, byte value)
  {
    buffer.put(buffer.position() + MAX_OVERFLOW_VALUE_BYTE, value);
  }

  @Override
  public short getMaxOverflowRegister()
  {
    return getStorageBuffer().getShort(getInitPosition() + MAX_OVERFLOW_REGISTER_BYTE);
  }

  @Override
  public void setMaxOverflowRegister(short register)
  {
    getStorageBuffer().putShort(getInitPosition() + MAX_OVERFLOW_REGISTER_BYTE, register);
  }

  @Override
  public void setMaxOverflowRegister(ByteBuffer buffer, short register)
  {
    buffer.putShort(buffer.position() + MAX_OVERFLOW_REGISTER_BYTE, register);
  }

  @Override
  public int getNumHeaderBytes()
  {
    return HEADER_NUM_BYTES;
  }

  @Override
  public int getNumBytesForDenseStorage()
  {
    return NUM_BYTES_FOR_DENSE_STORAGE;
  }

  @Override
  public int getPayloadBytePosition()
  {
    return getInitPosition() + HEADER_NUM_BYTES;
  }

  @Override
  public int getPayloadBytePosition(ByteBuffer buffer)
  {
    return buffer.position() + HEADER_NUM_BYTES;
  }
}
