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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.BaseState;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.internal.BaseStateImpl;
import org.apache.datasketches.memory.internal.UnsafeUtil;
import org.apache.datasketches.memory.internal.XxHash64;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Base class for making a regular {@link ByteBuffer} look like a {@link org.apache.datasketches.memory.Memory} or
 * {@link org.apache.datasketches.memory.Buffer}. All methods delegate directly to the {@link ByteBuffer} rather
 * than using 'unsafe' reads.
 *
 * @see SafeWritableMemory
 * @see SafeWritableBuffer
 */

@SuppressWarnings("unused")
public abstract class SafeWritableBase implements BaseState
{
  static final MemoryRequestServer SAFE_HEAP_REQUEST_SERVER = new HeapByteBufferMemoryRequestServer();

  final ByteBuffer buffer;

  public SafeWritableBase(ByteBuffer buffer)
  {
    this.buffer = buffer;
  }

  public MemoryRequestServer getMemoryRequestServer()
  {
    return SAFE_HEAP_REQUEST_SERVER;
  }

  public boolean getBoolean(long offsetBytes)
  {
    return getByte(Ints.checkedCast(offsetBytes)) != 0;
  }

  public byte getByte(long offsetBytes)
  {
    return buffer.get(Ints.checkedCast(offsetBytes));
  }

  public char getChar(long offsetBytes)
  {
    return buffer.getChar(Ints.checkedCast(offsetBytes));
  }

  public double getDouble(long offsetBytes)
  {
    return buffer.getDouble(Ints.checkedCast(offsetBytes));
  }

  public float getFloat(long offsetBytes)
  {
    return buffer.getFloat(Ints.checkedCast(offsetBytes));
  }

  public int getInt(long offsetBytes)
  {
    return buffer.getInt(Ints.checkedCast(offsetBytes));
  }

  public long getLong(long offsetBytes)
  {
    return buffer.getLong(Ints.checkedCast(offsetBytes));
  }

  public short getShort(long offsetBytes)
  {
    return buffer.getShort(Ints.checkedCast(offsetBytes));
  }

  public void putBoolean(long offsetBytes, boolean value)
  {
    buffer.put(Ints.checkedCast(offsetBytes), (byte) (value ? 1 : 0));
  }

  public void putByte(long offsetBytes, byte value)
  {
    buffer.put(Ints.checkedCast(offsetBytes), value);
  }

  public void putChar(long offsetBytes, char value)
  {
    buffer.putChar(Ints.checkedCast(offsetBytes), value);
  }

  public void putDouble(long offsetBytes, double value)
  {
    buffer.putDouble(Ints.checkedCast(offsetBytes), value);
  }

  public void putFloat(long offsetBytes, float value)
  {
    buffer.putFloat(Ints.checkedCast(offsetBytes), value);
  }

  public void putInt(long offsetBytes, int value)
  {
    buffer.putInt(Ints.checkedCast(offsetBytes), value);
  }

  public void putLong(long offsetBytes, long value)
  {
    buffer.putLong(Ints.checkedCast(offsetBytes), value);
  }

  public void putShort(long offsetBytes, short value)
  {
    buffer.putShort(Ints.checkedCast(offsetBytes), value);
  }

  @Override
  public ByteOrder getTypeByteOrder()
  {
    return buffer.order();
  }

  @Override
  public boolean isByteOrderCompatible(ByteOrder byteOrder)
  {
    return buffer.order().equals(byteOrder);
  }

  @Override
  public ByteBuffer getByteBuffer()
  {
    return buffer;
  }

  @Override
  public long getCapacity()
  {
    return buffer.capacity();
  }

  @Override
  public long getCumulativeOffset()
  {
    return 0;
  }

  @Override
  public long getCumulativeOffset(long offsetBytes)
  {
    return offsetBytes;
  }

  @Override
  public long getRegionOffset()
  {
    return 0;
  }

  @Override
  public long getRegionOffset(long offsetBytes)
  {
    return offsetBytes;
  }

  @Override
  public boolean hasArray()
  {
    return false;
  }

  @Override
  public long xxHash64(long offsetBytes, long lengthBytes, long seed)
  {
    return hash(buffer, offsetBytes, lengthBytes, seed);
  }

  @Override
  public long xxHash64(long in, long seed)
  {
    return XxHash64.hash(in, seed);
  }

  @Override
  public boolean hasByteBuffer()
  {
    return true;
  }

  @Override
  public boolean isDirect()
  {
    return false;
  }

  @Override
  public boolean isReadOnly()
  {
    return false;
  }

  @Override
  public boolean isSameResource(Object that)
  {
    return this.equals(that);
  }

  @Override
  public boolean isValid()
  {
    return true;
  }

  @Override
  public void checkValidAndBounds(long offsetBytes, long lengthBytes)
  {
    Preconditions.checkArgument(
        Ints.checkedCast(offsetBytes) < buffer.limit(),
        "start offset %s is greater than buffer limit %s",
        offsetBytes,
        buffer.limit()
    );
    Preconditions.checkArgument(
        Ints.checkedCast(offsetBytes + lengthBytes) < buffer.limit(),
        "end offset %s is greater than buffer limit %s",
        offsetBytes + lengthBytes,
        buffer.limit()
    );
  }

  /**
   * Adapted from {@link BaseStateImpl#toHexString(String, long, int)}
   */
  @Override
  public String toHexString(String header, long offsetBytes, int lengthBytes)
  {
    final String klass = this.getClass().getSimpleName();
    final String s1 = StringUtils.format("(..., %d, %d)", offsetBytes, lengthBytes);
    final long hcode = hashCode() & 0XFFFFFFFFL;
    final String call = ".toHexString" + s1 + ", hashCode: " + hcode;
    String sb = "### " + klass + " SUMMARY ###" + UnsafeUtil.LS
                + "Header Comment      : " + header + UnsafeUtil.LS
                + "Call Parameters     : " + call;
    return toHex(this, sb, offsetBytes, lengthBytes);
  }

  /**
   * Adapted from {@link BaseStateImpl#toHex(BaseStateImpl, String, long, int)}
   */
  static String toHex(
      final SafeWritableBase state,
      final String preamble,
      final long offsetBytes,
      final int lengthBytes
  )
  {
    final String lineSeparator = UnsafeUtil.LS;
    final long capacity = state.getCapacity();
    UnsafeUtil.checkBounds(offsetBytes, lengthBytes, capacity);
    final StringBuilder sb = new StringBuilder();
    final String uObjStr;
    final long uObjHeader;
    uObjStr = "null";
    uObjHeader = 0;
    final ByteBuffer bb = state.getByteBuffer();
    final String bbStr = bb == null ? "null"
                                    : bb.getClass().getSimpleName() + ", " + (bb.hashCode() & 0XFFFFFFFFL);
    final MemoryRequestServer memReqSvr = state.getMemoryRequestServer();
    final String memReqStr = memReqSvr != null
                             ? memReqSvr.getClass().getSimpleName() + ", " + (memReqSvr.hashCode() & 0XFFFFFFFFL)
                             : "null";
    final long cumBaseOffset = state.getCumulativeOffset();
    sb.append(preamble).append(lineSeparator);
    sb.append("UnsafeObj, hashCode : ").append(uObjStr).append(lineSeparator);
    sb.append("UnsafeObjHeader     : ").append(uObjHeader).append(lineSeparator);
    sb.append("ByteBuf, hashCode   : ").append(bbStr).append(lineSeparator);
    sb.append("RegionOffset        : ").append(state.getRegionOffset()).append(lineSeparator);
    sb.append("Capacity            : ").append(capacity).append(lineSeparator);
    sb.append("CumBaseOffset       : ").append(cumBaseOffset).append(lineSeparator);
    sb.append("MemReq, hashCode    : ").append(memReqStr).append(lineSeparator);
    sb.append("Valid               : ").append(state.isValid()).append(lineSeparator);
    sb.append("Read Only           : ").append(state.isReadOnly()).append(lineSeparator);
    sb.append("Type Byte Order     : ").append(state.getTypeByteOrder()).append(lineSeparator);
    sb.append("Native Byte Order   : ").append(ByteOrder.nativeOrder()).append(lineSeparator);
    sb.append("JDK Runtime Version : ").append(UnsafeUtil.JDK).append(lineSeparator);
    //Data detail
    sb.append("Data, littleEndian  :  0  1  2  3  4  5  6  7");

    for (long i = 0; i < lengthBytes; i++) {
      final int b = state.getByte(cumBaseOffset + offsetBytes + i) & 0XFF;
      if (i % 8 == 0) { //row header
        sb.append(StringUtils.format("%n%20s: ", offsetBytes + i));
      }
      sb.append(StringUtils.format("%02x ", b));
    }
    sb.append(lineSeparator);

    return sb.toString();
  }

  // copied from datasketches-memory XxHash64.java
  private static final long P1 = -7046029288634856825L;
  private static final long P2 = -4417276706812531889L;
  private static final long P3 = 1609587929392839161L;
  private static final long P4 = -8796714831421723037L;
  private static final long P5 = 2870177450012600261L;

  /**
   * Adapted from {@link XxHash64#hash(Object, long, long, long)} to work with {@link ByteBuffer}
   */
  static long hash(ByteBuffer memory, long cumOffsetBytes, final long lengthBytes, final long seed)
  {
    long hash;
    long remaining = lengthBytes;
    int offset = Ints.checkedCast(cumOffsetBytes);

    if (remaining >= 32) {
      long v1 = seed + P1 + P2;
      long v2 = seed + P2;
      long v3 = seed;
      long v4 = seed - P1;

      do {
        v1 += memory.getLong(offset) * P2;
        v1 = Long.rotateLeft(v1, 31);
        v1 *= P1;

        v2 += memory.getLong(offset + 8) * P2;
        v2 = Long.rotateLeft(v2, 31);
        v2 *= P1;

        v3 += memory.getLong(offset + 16) * P2;
        v3 = Long.rotateLeft(v3, 31);
        v3 *= P1;

        v4 += memory.getLong(offset + 24) * P2;
        v4 = Long.rotateLeft(v4, 31);
        v4 *= P1;

        offset += 32;
        remaining -= 32;
      } while (remaining >= 32);

      hash = Long.rotateLeft(v1, 1)
             + Long.rotateLeft(v2, 7)
             + Long.rotateLeft(v3, 12)
             + Long.rotateLeft(v4, 18);

      v1 *= P2;
      v1 = Long.rotateLeft(v1, 31);
      v1 *= P1;
      hash ^= v1;
      hash = (hash * P1) + P4;

      v2 *= P2;
      v2 = Long.rotateLeft(v2, 31);
      v2 *= P1;
      hash ^= v2;
      hash = (hash * P1) + P4;

      v3 *= P2;
      v3 = Long.rotateLeft(v3, 31);
      v3 *= P1;
      hash ^= v3;
      hash = (hash * P1) + P4;

      v4 *= P2;
      v4 = Long.rotateLeft(v4, 31);
      v4 *= P1;
      hash ^= v4;
      hash = (hash * P1) + P4;
    } else { //end remaining >= 32
      hash = seed + P5;
    }

    hash += lengthBytes;

    while (remaining >= 8) {
      long k1 = memory.getLong(offset);
      k1 *= P2;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= P1;
      hash ^= k1;
      hash = (Long.rotateLeft(hash, 27) * P1) + P4;
      offset += 8;
      remaining -= 8;
    }

    if (remaining >= 4) { //treat as unsigned ints
      hash ^= (memory.getInt(offset) & 0XFFFF_FFFFL) * P1;
      hash = (Long.rotateLeft(hash, 23) * P2) + P3;
      offset += 4;
      remaining -= 4;
    }

    while (remaining != 0) { //treat as unsigned bytes
      hash ^= (memory.get(offset) & 0XFFL) * P5;
      hash = Long.rotateLeft(hash, 11) * P1;
      --remaining;
      ++offset;
    }

    hash ^= hash >>> 33;
    hash *= P2;
    hash ^= hash >>> 29;
    hash *= P3;
    hash ^= hash >>> 32;
    return hash;
  }

  private static class HeapByteBufferMemoryRequestServer implements MemoryRequestServer
  {
    @Override
    public WritableMemory request(WritableMemory currentWritableMemory, long capacityBytes)
    {
      ByteBuffer newBuffer = ByteBuffer.allocate(Ints.checkedCast(capacityBytes));
      newBuffer.order(currentWritableMemory.getTypeByteOrder());
      return new SafeWritableMemory(newBuffer);
    }

    @Override
    public void requestClose(WritableMemory memToClose, WritableMemory newMemory)
    {
      // do nothing
    }
  }
}
