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
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.Utf8CodingException;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Safety first! Don't trust something whose contents you locations to read and write stuff to, but need a
 * {@link Memory} or {@link WritableMemory}? use this!
 * <p>
 * Delegates everything to an underlying {@link ByteBuffer} so all read and write operations will have bounds checks
 * built in rather than using 'unsafe'.
 */
public class SafeWritableMemory extends SafeWritableBase implements WritableMemory
{
  public static SafeWritableMemory wrap(byte[] bytes)
  {
    return wrap(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()), 0, bytes.length);
  }

  public static SafeWritableMemory wrap(ByteBuffer buffer)
  {
    return wrap(buffer.duplicate().order(buffer.order()), 0, buffer.capacity());
  }

  public static SafeWritableMemory wrap(ByteBuffer buffer, ByteOrder byteOrder)
  {
    return wrap(buffer.duplicate().order(byteOrder), 0, buffer.capacity());
  }

  public static SafeWritableMemory wrap(ByteBuffer buffer, int offset, int size)
  {
    final ByteBuffer dupe = buffer.duplicate().order(buffer.order());
    dupe.position(offset);
    dupe.limit(offset + size);
    return new SafeWritableMemory(dupe.slice().order(buffer.order()));
  }

  public SafeWritableMemory(ByteBuffer buffer)
  {
    super(buffer);
  }

  @Override
  public Memory region(long offsetBytes, long capacityBytes, ByteOrder byteOrder)
  {
    return writableRegion(offsetBytes, capacityBytes, byteOrder);
  }

  @Override
  public Buffer asBuffer(ByteOrder byteOrder)
  {
    return asWritableBuffer(byteOrder);
  }

  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthBooleans; j++) {
      dstArray[dstOffsetBooleans + j] = buffer.get(offset + j) != 0;
    }
  }

  @Override
  public void getByteArray(long offsetBytes, byte[] dstArray, int dstOffsetBytes, int lengthBytes)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthBytes; j++) {
      dstArray[dstOffsetBytes + j] = buffer.get(offset + j);
    }
  }

  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffsetChars, int lengthChars)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthChars; j++) {
      dstArray[dstOffsetChars + j] = buffer.getChar(offset + (j * Character.BYTES));
    }
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, Appendable dst)
      throws IOException, Utf8CodingException
  {
    ByteBuffer dupe = buffer.asReadOnlyBuffer().order(buffer.order());
    dupe.position(Ints.checkedCast(offsetBytes));
    String s = StringUtils.fromUtf8(dupe, utf8LengthBytes);
    dst.append(s);
    return s.length();
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, StringBuilder dst) throws Utf8CodingException
  {
    ByteBuffer dupe = buffer.asReadOnlyBuffer().order(buffer.order());
    dupe.position(Ints.checkedCast(offsetBytes));
    String s = StringUtils.fromUtf8(dupe, utf8LengthBytes);
    dst.append(s);
    return s.length();
  }

  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffsetDoubles, int lengthDoubles)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthDoubles; j++) {
      dstArray[dstOffsetDoubles + j] = buffer.getDouble(offset + (j * Double.BYTES));
    }
  }

  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffsetFloats, int lengthFloats)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthFloats; j++) {
      dstArray[dstOffsetFloats + j] = buffer.getFloat(offset + (j * Float.BYTES));
    }
  }

  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffsetInts, int lengthInts)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthInts; j++) {
      dstArray[dstOffsetInts + j] = buffer.getInt(offset + (j * Integer.BYTES));
    }
  }

  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffsetLongs, int lengthLongs)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthLongs; j++) {
      dstArray[dstOffsetLongs + j] = buffer.getLong(offset + (j * Long.BYTES));
    }
  }

  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffsetShorts, int lengthShorts)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int j = 0; j < lengthShorts; j++) {
      dstArray[dstOffsetShorts + j] = buffer.getShort(offset + (j * Short.BYTES));
    }
  }

  @Override
  public int compareTo(
      long thisOffsetBytes,
      long thisLengthBytes,
      Memory that,
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
  public void copyTo(long srcOffsetBytes, WritableMemory destination, long dstOffsetBytes, long lengthBytes)
  {
    int offset = Ints.checkedCast(srcOffsetBytes);
    for (int i = 0; i < lengthBytes; i++) {
      destination.putByte(dstOffsetBytes + i, buffer.get(offset + i));
    }
  }

  @Override
  public void writeTo(long offsetBytes, long lengthBytes, WritableByteChannel out) throws IOException
  {
    ByteBuffer dupe = buffer.duplicate();
    dupe.position(Ints.checkedCast(offsetBytes));
    dupe.limit(dupe.position() + Ints.checkedCast(lengthBytes));
    ByteBuffer view = dupe.slice();
    view.order(buffer.order());
    out.write(view);
  }

  @Override
  public boolean equalTo(long thisOffsetBytes, Object that, long thatOffsetBytes, long lengthBytes)
  {
    if (!(that instanceof SafeWritableMemory)) {
      return false;
    }
    return compareTo(thisOffsetBytes, lengthBytes, (SafeWritableMemory) that, thatOffsetBytes, lengthBytes) == 0;
  }


  @Override
  public WritableMemory writableRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder)
  {
    final ByteBuffer dupe = buffer.duplicate().order(buffer.order());
    final int sizeBytes = Ints.checkedCast(capacityBytes);
    dupe.position(Ints.checkedCast(offsetBytes));
    dupe.limit(dupe.position() + sizeBytes);
    final ByteBuffer view = dupe.slice();
    view.order(byteOrder);
    return new SafeWritableMemory(view);
  }

  @Override
  public WritableBuffer asWritableBuffer(ByteOrder byteOrder)
  {
    return new SafeWritableBuffer(buffer.duplicate().order(byteOrder));
  }

  @Override
  public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffsetBooleans, int lengthBooleans)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthBooleans; i++) {
      buffer.put(offset + i, (byte) (srcArray[i + srcOffsetBooleans] ? 1 : 0));
    }
  }

  @Override
  public void putByteArray(long offsetBytes, byte[] srcArray, int srcOffsetBytes, int lengthBytes)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthBytes; i++) {
      buffer.put(offset + i, srcArray[srcOffsetBytes + i]);
    }
  }

  @Override
  public void putCharArray(long offsetBytes, char[] srcArray, int srcOffsetChars, int lengthChars)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthChars; i++) {
      buffer.putChar(offset + (i * Character.BYTES), srcArray[srcOffsetChars + i]);
    }
  }

  @Override
  public long putCharsToUtf8(long offsetBytes, CharSequence src)
  {
    final byte[] bytes = StringUtils.toUtf8(src.toString());
    putByteArray(offsetBytes, bytes, 0, bytes.length);
    return bytes.length;
  }

  @Override
  public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffsetDoubles, int lengthDoubles)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthDoubles; i++) {
      buffer.putDouble(offset + (i * Double.BYTES), srcArray[srcOffsetDoubles + i]);
    }
  }

  @Override
  public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffsetFloats, int lengthFloats)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthFloats; i++) {
      buffer.putFloat(offset + (i * Float.BYTES), srcArray[srcOffsetFloats + i]);
    }
  }

  @Override
  public void putIntArray(long offsetBytes, int[] srcArray, int srcOffsetInts, int lengthInts)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthInts; i++) {
      buffer.putInt(offset + (i * Integer.BYTES), srcArray[srcOffsetInts + i]);
    }
  }

  @Override
  public void putLongArray(long offsetBytes, long[] srcArray, int srcOffsetLongs, int lengthLongs)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthLongs; i++) {
      buffer.putLong(offset + (i * Long.BYTES), srcArray[srcOffsetLongs + i]);
    }
  }

  @Override
  public void putShortArray(long offsetBytes, short[] srcArray, int srcOffsetShorts, int lengthShorts)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    for (int i = 0; i < lengthShorts; i++) {
      buffer.putShort(offset + (i * Short.BYTES), srcArray[srcOffsetShorts + i]);
    }
  }

  @Override
  public long getAndAddLong(long offsetBytes, long delta)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    final long currentValue;
    synchronized (buffer) {
      currentValue = buffer.getLong(offset);
      buffer.putLong(offset, currentValue + delta);
    }
    return currentValue;
  }

  @Override
  public boolean compareAndSwapLong(long offsetBytes, long expect, long update)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    synchronized (buffer) {
      final long actual = buffer.getLong(offset);
      if (expect == actual) {
        buffer.putLong(offset, update);
        return true;
      }
    }
    return false;
  }

  @Override
  public long getAndSetLong(long offsetBytes, long newValue)
  {
    int offset = Ints.checkedCast(offsetBytes);
    synchronized (buffer) {
      long l = buffer.getLong(offset);
      buffer.putLong(offset, newValue);
      return l;
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
  public void clear(long offsetBytes, long lengthBytes)
  {
    fill(offsetBytes, lengthBytes, (byte) 0);
  }

  @Override
  public void clearBits(long offsetBytes, byte bitMask)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    int value = buffer.get(offset) & 0XFF;
    value &= ~bitMask;
    buffer.put(offset, (byte) value);
  }

  @Override
  public void fill(byte value)
  {
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.put(i, value);
    }
  }

  @Override
  public void fill(long offsetBytes, long lengthBytes, byte value)
  {
    int offset = Ints.checkedCast(offsetBytes);
    int length = Ints.checkedCast(lengthBytes);
    for (int i = 0; i < length; i++) {
      buffer.put(offset + i, value);
    }
  }

  @Override
  public void setBits(long offsetBytes, byte bitMask)
  {
    final int offset = Ints.checkedCast(offsetBytes);
    buffer.put(offset, (byte) (buffer.get(offset) | bitMask));
  }
}
