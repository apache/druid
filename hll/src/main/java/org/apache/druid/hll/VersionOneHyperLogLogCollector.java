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

package org.apache.druid.hll;

import java.nio.ByteBuffer;

/**
 */
public class VersionOneHyperLogLogCollector extends HyperLogLogCollector
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

  private static final ByteBuffer DEFAULT_STORAGE_BUFFER = ByteBuffer.wrap(new byte[]{VERSION, 0, 0, 0, 0, 0, 0})
                                                                   .asReadOnlyBuffer();

  VersionOneHyperLogLogCollector()
  {
    super(DEFAULT_STORAGE_BUFFER.duplicate());
  }

  VersionOneHyperLogLogCollector(ByteBuffer buffer)
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
