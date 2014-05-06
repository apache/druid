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
@Deprecated
public class HLLCV0 extends HyperLogLogCollector
{
  /**
   * Header:
   * Byte 0: registerOffset
   * Byte 1-2: numNonZeroRegisters
   */
  public static final int NUM_NON_ZERO_REGISTERS_BYTE = 1;
  public static final int HEADER_NUM_BYTES = 3;
  public static final int NUM_BYTES_FOR_DENSE_STORAGE = NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES;

  private static final ByteBuffer defaultStorageBuffer = ByteBuffer.wrap(new byte[]{0, 0, 0}).asReadOnlyBuffer();

  protected HLLCV0()
  {
    super(defaultStorageBuffer);
  }

  protected HLLCV0(ByteBuffer buffer)
  {
    super(buffer);
  }

  @Override
  public byte getVersion()
  {
    return 0;
  }

  @Override
  public void setVersion(ByteBuffer buffer)
  {
  }

  @Override
  public byte getRegisterOffset()
  {
    return getStorageBuffer().get(getInitPosition());
  }

  @Override
  public void setRegisterOffset(byte registerOffset)
  {
    getStorageBuffer().put(getInitPosition(), registerOffset);
  }

  @Override
  public void setRegisterOffset(ByteBuffer buffer, byte registerOffset)
  {
    buffer.put(buffer.position(), registerOffset);
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
    return 0;
  }

  @Override
  public void setMaxOverflowValue(byte value)
  {
  }

  @Override
  public void setMaxOverflowValue(ByteBuffer buffer, byte value)
  {
  }

  @Override
  public short getMaxOverflowRegister()
  {
    return 0;
  }

  @Override
  public void setMaxOverflowRegister(short register)
  {
  }

  @Override
  public void setMaxOverflowRegister(ByteBuffer buffer, short register)
  {
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
