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

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.BaseBuffer;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Safety first! Don't trust something whose contents you locations to read and write stuff to, but need a
 * {@link Buffer} or {@link WritableBuffer}? use this!
 * <p>
 * Delegates everything to an underlying {@link ByteBuffer} so all read and write operations will have bounds checks
 * built in rather than using 'unsafe'.
 */
public class SafeWritableBuffer extends SafeWritableBase implements WritableBuffer
{
  private int start;
  private int end;

  public SafeWritableBuffer(ByteBuffer buffer)
  {
    super(buffer);
    this.start = 0;
    this.buffer.position(0);
    this.end = buffer.capacity();
  }

  @Override
  public WritableBuffer writableDuplicate()
  {
    return writableDuplicate(buffer.order());
  }

  @Override
  public WritableBuffer writableDuplicate(ByteOrder byteOrder)
  {
    ByteBuffer dupe = buffer.duplicate();
    dupe.order(byteOrder);
    WritableBuffer duplicate = new SafeWritableBuffer(dupe);
    duplicate.setStartPositionEnd(start, buffer.position(), end);
    return duplicate;
  }

  @Override
  public WritableBuffer writableRegion()
  {
    ByteBuffer dupe = buffer.duplicate().order(buffer.order());
    dupe.position(start);
    dupe.limit(end);
    ByteBuffer remaining = buffer.slice();
    remaining.order(dupe.order());
    return new SafeWritableBuffer(remaining);
  }

  @Override
  public WritableBuffer writableRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder)
  {
    ByteBuffer dupe = buffer.duplicate();
    dupe.position(Ints.checkedCast(offsetBytes));
    dupe.limit(dupe.position() + Ints.checkedCast(capacityBytes));
    return new SafeWritableBuffer(dupe.slice().order(byteOrder));
  }

  @Override
  public WritableMemory asWritableMemory(ByteOrder byteOrder)
  {
    ByteBuffer dupe = buffer.duplicate();
    dupe.order(byteOrder);
    return new SafeWritableMemory(dupe);
  }

  @Override
  public void putBoolean(boolean value)
  {
    buffer.put((byte) (value ? 1 : 0));
  }

  @Override
  public void putBooleanArray(boolean[] srcArray, int srcOffsetBooleans, int lengthBooleans)
  {
    for (int i = 0; i < lengthBooleans; i++) {
      putBoolean(srcArray[srcOffsetBooleans + i]);
    }
  }

  @Override
  public void putByte(byte value)
  {
    buffer.put(value);
  }

  @Override
  public void putByteArray(byte[] srcArray, int srcOffsetBytes, int lengthBytes)
  {
    buffer.put(srcArray, srcOffsetBytes, lengthBytes);
  }

  @Override
  public void putChar(char value)
  {
    buffer.putChar(value);
  }

  @Override
  public void putCharArray(char[] srcArray, int srcOffsetChars, int lengthChars)
  {
    for (int i = 0; i < lengthChars; i++) {
      buffer.putChar(srcArray[srcOffsetChars + i]);
    }
  }

  @Override
  public void putDouble(double value)
  {
    buffer.putDouble(value);
  }

  @Override
  public void putDoubleArray(double[] srcArray, int srcOffsetDoubles, int lengthDoubles)
  {
    for (int i = 0; i < lengthDoubles; i++) {
      buffer.putDouble(srcArray[srcOffsetDoubles + i]);
    }
  }

  @Override
  public void putFloat(float value)
  {
    buffer.putFloat(value);
  }

  @Override
  public void putFloatArray(float[] srcArray, int srcOffsetFloats, int lengthFloats)
  {
    for (int i = 0; i < lengthFloats; i++) {
      buffer.putFloat(srcArray[srcOffsetFloats + i]);
    }
  }

  @Override
  public void putInt(int value)
  {
    buffer.putInt(value);
  }

  @Override
  public void putIntArray(int[] srcArray, int srcOffsetInts, int lengthInts)
  {
    for (int i = 0; i < lengthInts; i++) {
      buffer.putInt(srcArray[srcOffsetInts + i]);
    }
  }

  @Override
  public void putLong(long value)
  {
    buffer.putLong(value);
  }

  @Override
  public void putLongArray(long[] srcArray, int srcOffsetLongs, int lengthLongs)
  {
    for (int i = 0; i < lengthLongs; i++) {
      buffer.putLong(srcArray[srcOffsetLongs + i]);
    }
  }

  @Override
  public void putShort(short value)
  {
    buffer.putShort(value);
  }

  @Override
  public void putShortArray(short[] srcArray, int srcOffsetShorts, int lengthShorts)
  {
    for (int i = 0; i < lengthShorts; i++) {
      buffer.putShort(srcArray[srcOffsetShorts + i]);
    }
  }

  @Override
  public Object getArray()
  {
    return null;
  }

  @Override
  public void clear()
  {
    fill((byte) 0);
  }

  @Override
  public void fill(byte value)
  {
    while (buffer.hasRemaining() && buffer.position() < end) {
      buffer.put(value);
    }
  }

  @Override
  public Buffer duplicate()
  {
    return writableDuplicate();
  }

  @Override
  public Buffer duplicate(ByteOrder byteOrder)
  {
    return writableDuplicate(byteOrder);
  }

  @Override
  public Buffer region()
  {
    return writableRegion();
  }

  @Override
  public Buffer region(long offsetBytes, long capacityBytes, ByteOrder byteOrder)
  {
    return writableRegion(offsetBytes, capacityBytes, byteOrder);
  }

  @Override
  public Memory asMemory(ByteOrder byteOrder)
  {
    return asWritableMemory(byteOrder);
  }

  @Override
  public boolean getBoolean()
  {
    return buffer.get() == 0 ? false : true;
  }

  @Override
  public void getBooleanArray(boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans)
  {
    for (int i = 0; i < lengthBooleans; i++) {
      dstArray[dstOffsetBooleans + i] = getBoolean();
    }
  }

  @Override
  public byte getByte()
  {
    return buffer.get();
  }

  @Override
  public void getByteArray(byte[] dstArray, int dstOffsetBytes, int lengthBytes)
  {
    for (int i = 0; i < lengthBytes; i++) {
      dstArray[dstOffsetBytes + i] = buffer.get();
    }
  }

  @Override
  public char getChar()
  {
    return buffer.getChar();
  }

  @Override
  public void getCharArray(char[] dstArray, int dstOffsetChars, int lengthChars)
  {
    for (int i = 0; i < lengthChars; i++) {
      dstArray[dstOffsetChars + i] = buffer.getChar();
    }
  }

  @Override
  public double getDouble()
  {
    return buffer.getDouble();
  }

  @Override
  public void getDoubleArray(double[] dstArray, int dstOffsetDoubles, int lengthDoubles)
  {
    for (int i = 0; i < lengthDoubles; i++) {
      dstArray[dstOffsetDoubles + i] = buffer.getDouble();
    }
  }

  @Override
  public float getFloat()
  {
    return buffer.getFloat();
  }

  @Override
  public void getFloatArray(float[] dstArray, int dstOffsetFloats, int lengthFloats)
  {
    for (int i = 0; i < lengthFloats; i++) {
      dstArray[dstOffsetFloats + i] = buffer.getFloat();
    }
  }

  @Override
  public int getInt()
  {
    return buffer.getInt();
  }

  @Override
  public void getIntArray(int[] dstArray, int dstOffsetInts, int lengthInts)
  {
    for (int i = 0; i < lengthInts; i++) {
      dstArray[dstOffsetInts + i] = buffer.getInt();
    }
  }

  @Override
  public long getLong()
  {
    return buffer.getLong();
  }

  @Override
  public void getLongArray(long[] dstArray, int dstOffsetLongs, int lengthLongs)
  {
    for (int i = 0; i < lengthLongs; i++) {
      dstArray[dstOffsetLongs + i] = buffer.getLong();
    }
  }

  @Override
  public short getShort()
  {
    return buffer.getShort();
  }

  @Override
  public void getShortArray(short[] dstArray, int dstOffsetShorts, int lengthShorts)
  {
    for (int i = 0; i < lengthShorts; i++) {
      dstArray[dstOffsetShorts + i] = buffer.getShort();
    }
  }

  @Override
  public int compareTo(
      long thisOffsetBytes,
      long thisLengthBytes,
      Buffer that,
      long thatOffsetBytes,
      long thatLengthBytes
  )
  {
    final int thisLength = Ints.checkedCast(thisLengthBytes);
    final int thatLength = Ints.checkedCast(thatLengthBytes);

    final int commonLength = Math.min(thisLength, thatLength);

    for (int i = 0; i < commonLength; i++) {
      final int cmp = Byte.compare(getByte(thisOffsetBytes + i), that.getByte(thatOffsetBytes + i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(thisLength, thatLength);
  }

  @Override
  public BaseBuffer incrementPosition(long increment)
  {
    buffer.position(buffer.position() + Ints.checkedCast(increment));
    return this;
  }

  @Override
  public BaseBuffer incrementAndCheckPosition(long increment)
  {
    checkInvariants(start, buffer.position() + increment, end, buffer.capacity());
    return incrementPosition(increment);
  }

  @Override
  public long getEnd()
  {
    return end;
  }

  @Override
  public long getPosition()
  {
    return buffer.position();
  }

  @Override
  public long getStart()
  {
    return start;
  }

  @Override
  public long getRemaining()
  {
    return buffer.remaining();
  }

  @Override
  public boolean hasRemaining()
  {
    return buffer.hasRemaining();
  }

  @Override
  public BaseBuffer resetPosition()
  {
    buffer.position(start);
    return this;
  }

  @Override
  public BaseBuffer setPosition(long position)
  {
    buffer.position(Ints.checkedCast(position));
    return this;
  }

  @Override
  public BaseBuffer setAndCheckPosition(long position)
  {
    checkInvariants(start, position, end, buffer.capacity());
    return setPosition(position);
  }

  @Override
  public BaseBuffer setStartPositionEnd(long start, long position, long end)
  {
    this.start = Ints.checkedCast(start);
    this.end = Ints.checkedCast(end);
    buffer.position(Ints.checkedCast(position));
    buffer.limit(this.end);
    return this;
  }

  @Override
  public BaseBuffer setAndCheckStartPositionEnd(long start, long position, long end)
  {
    checkInvariants(start, position, end, buffer.capacity());
    return setStartPositionEnd(start, position, end);
  }

  @Override
  public boolean equalTo(long thisOffsetBytes, Object that, long thatOffsetBytes, long lengthBytes)
  {
    if (!(that instanceof SafeWritableBuffer)) {
      return false;
    }
    return compareTo(thisOffsetBytes, lengthBytes, (SafeWritableBuffer) that, thatOffsetBytes, lengthBytes) == 0;
  }

  /**
   * Adapted from {@link org.apache.datasketches.memory.internal.BaseBufferImpl#checkInvariants(long, long, long, long)}
   */
  static void checkInvariants(final long start, final long pos, final long end, final long cap)
  {
    if ((start | pos | end | cap | (pos - start) | (end - pos) | (cap - end)) < 0L) {
      throw new IllegalArgumentException(
          "Violation of Invariants: "
          + "start: " + start
          + " <= pos: " + pos
          + " <= end: " + end
          + " <= cap: " + cap
          + "; (pos - start): " + (pos - start)
          + ", (end - pos): " + (end - pos)
          + ", (cap - end): " + (cap - end)
      );
    }
  }
}
