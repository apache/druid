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
@Deprecated
public class VersionZeroHyperLogLogCollector extends HyperLogLogCollector
{
  /**
   * Header:
   * Byte 0: registerOffset
   * Byte 1-2: numNonZeroRegisters
   */
  public static final int NUM_NON_ZERO_REGISTERS_BYTE = 1;
  public static final int HEADER_NUM_BYTES = 3;
  public static final int NUM_BYTES_FOR_DENSE_STORAGE = NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES;

  VersionZeroHyperLogLogCollector(ByteBuffer buffer)
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
