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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.memory.internal.UnsafeUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SafeWritableMemoryTest
{
  private static final int CAPACITY = 1024;

  @Test
  public void testPutAndGet()
  {
    final WritableMemory memory = getMemory();
    memory.putByte(3L, (byte) 0x01);
    Assert.assertEquals(memory.getByte(3L), 0x01);

    memory.putBoolean(1L, true);
    Assert.assertTrue(memory.getBoolean(1L));
    memory.putBoolean(1L, false);
    Assert.assertFalse(memory.getBoolean(1L));

    memory.putChar(10L, 'c');
    Assert.assertEquals('c', memory.getChar(10L));

    memory.putDouble(14L, 3.3);
    Assert.assertEquals(3.3, memory.getDouble(14L), 0.0);

    memory.putFloat(27L, 3.3f);
    Assert.assertEquals(3.3f, memory.getFloat(27L), 0.0);

    memory.putInt(11L, 1234);
    Assert.assertEquals(1234, memory.getInt(11L));

    memory.putLong(500L, 500L);
    Assert.assertEquals(500L, memory.getLong(500L));

    memory.putShort(11L, (short) 15);
    Assert.assertEquals(15, memory.getShort(11L));

    long l = memory.getAndSetLong(900L, 10L);
    Assert.assertEquals(0L, l);
    l = memory.getAndSetLong(900L, 100L);
    Assert.assertEquals(10L, l);
    l = memory.getAndAddLong(900L, 10L);
    Assert.assertEquals(100L, l);
    Assert.assertEquals(110L, memory.getLong(900L));
    Assert.assertTrue(memory.compareAndSwapLong(900L, 110L, 120L));
    Assert.assertFalse(memory.compareAndSwapLong(900L, 110L, 120L));
    Assert.assertEquals(120L, memory.getLong(900L));
  }

  @Test
  public void testPutAndGetArrays()
  {
    final WritableMemory memory = getMemory();
    final byte[] b1 = new byte[]{0x01, 0x02, 0x08, 0x08};
    final byte[] b2 = new byte[b1.length];
    memory.putByteArray(12L, b1, 0, 3);
    memory.putByteArray(15L, b1, 3, 1);
    memory.getByteArray(12L, b2, 0, 3);
    memory.getByteArray(15L, b2, 3, 1);
    Assert.assertArrayEquals(b1, b2);

    final boolean[] bool1 = new boolean[]{true, false, false, true};
    final boolean[] bool2 = new boolean[bool1.length];
    memory.putBooleanArray(100L, bool1, 0, 2);
    memory.putBooleanArray(102L, bool1, 2, 2);
    memory.getBooleanArray(100L, bool2, 0, 2);
    memory.getBooleanArray(102L, bool2, 2, 2);
    Assert.assertArrayEquals(bool1, bool2);

    final char[] chars1 = new char[]{'a', 'b', 'c', 'd'};
    final char[] chars2 = new char[chars1.length];
    memory.putCharArray(10L, chars1, 0, 4);
    memory.getCharArray(10L, chars2, 0, chars1.length);
    Assert.assertArrayEquals(chars1, chars2);

    final double[] double1 = new double[]{1.1, -2.2, 3.3, 4.4};
    final double[] double2 = new double[double1.length];
    memory.putDoubleArray(100L, double1, 0, 1);
    memory.putDoubleArray(100L + Double.BYTES, double1, 1, 3);
    memory.getDoubleArray(100L, double2, 0, 2);
    memory.getDoubleArray(100L + (2 * Double.BYTES), double2, 2, 2);
    for (int i = 0; i < double1.length; i++) {
      Assert.assertEquals(double1[i], double2[i], 0.0);
    }

    final float[] float1 = new float[]{1.1f, 2.2f, -3.3f, 4.4f};
    final float[] float2 = new float[float1.length];
    memory.putFloatArray(100L, float1, 0, 1);
    memory.putFloatArray(100L + Float.BYTES, float1, 1, 3);
    memory.getFloatArray(100L, float2, 0, 2);
    memory.getFloatArray(100L + (2 * Float.BYTES), float2, 2, 2);
    for (int i = 0; i < float1.length; i++) {
      Assert.assertEquals(float1[i], float2[i], 0.0);
    }

    final int[] ints1 = new int[]{1, 2, -3, 4};
    final int[] ints2 = new int[ints1.length];
    memory.putIntArray(100L, ints1, 0, 1);
    memory.putIntArray(100L + Integer.BYTES, ints1, 1, 3);
    memory.getIntArray(100L, ints2, 0, 2);
    memory.getIntArray(100L + (2 * Integer.BYTES), ints2, 2, 2);
    Assert.assertArrayEquals(ints1, ints2);

    final long[] longs1 = new long[]{1L, -2L, 3L, -14L};
    final long[] longs2 = new long[ints1.length];
    memory.putLongArray(100L, longs1, 0, 1);
    memory.putLongArray(100L + Long.BYTES, longs1, 1, 3);
    memory.getLongArray(100L, longs2, 0, 2);
    memory.getLongArray(100L + (2 * Long.BYTES), longs2, 2, 2);
    Assert.assertArrayEquals(longs1, longs2);

    final short[] shorts1 = new short[]{1, -2, 3, -14};
    final short[] shorts2 = new short[ints1.length];
    memory.putShortArray(100L, shorts1, 0, 1);
    memory.putShortArray(100L + Short.BYTES, shorts1, 1, 3);
    memory.getShortArray(100L, shorts2, 0, 2);
    memory.getShortArray(100L + (2 * Short.BYTES), shorts2, 2, 2);
    Assert.assertArrayEquals(shorts1, shorts2);
  }

  @Test
  public void testFill()
  {
    final byte theByte = 0x01;
    final byte anotherByte = 0x02;
    final WritableMemory memory = getMemory();
    final int halfWay = (int) (memory.getCapacity() / 2);

    memory.fill(theByte);
    for (int i = 0; i < memory.getCapacity(); i++) {
      Assert.assertEquals(theByte, memory.getByte(i));
    }

    memory.fill(halfWay, memory.getCapacity() - halfWay, anotherByte);
    for (int i = 0; i < memory.getCapacity(); i++) {
      if (i < halfWay) {
        Assert.assertEquals(theByte, memory.getByte(i));
      } else {
        Assert.assertEquals(anotherByte, memory.getByte(i));
      }
    }

    memory.clear(halfWay, memory.getCapacity() - halfWay);
    for (int i = 0; i < memory.getCapacity(); i++) {
      if (i < halfWay) {
        Assert.assertEquals(theByte, memory.getByte(i));
      } else {
        Assert.assertEquals(0, memory.getByte(i));
      }
    }

    memory.setBits(halfWay - 1, anotherByte);
    Assert.assertEquals(0x03, memory.getByte(halfWay - 1));
    memory.clearBits(halfWay - 1, theByte);
    Assert.assertEquals(anotherByte, memory.getByte(halfWay - 1));

    memory.clear();
    for (int i = 0; i < memory.getCapacity(); i++) {
      Assert.assertEquals(0, memory.getByte(i));
    }
  }

  @Test
  public void testStringStuff() throws IOException
  {
    WritableMemory memory = getMemory();
    String s1 = "hello ";
    memory.putCharsToUtf8(10L, s1);

    StringBuilder builder = new StringBuilder();
    memory.getCharsFromUtf8(10L, s1.length(), builder);
    Assert.assertEquals(s1, builder.toString());

    CharArrayWriter someAppendable = new CharArrayWriter();
    memory.getCharsFromUtf8(10L, s1.length(), someAppendable);
    Assert.assertEquals(s1, someAppendable.toString());
  }

  @Test
  public void testRegion()
  {
    WritableMemory memory = getMemory();
    Assert.assertEquals(CAPACITY, memory.getCapacity());
    Assert.assertEquals(0, memory.getCumulativeOffset());
    Assert.assertEquals(10L, memory.getCumulativeOffset(10L));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> memory.checkValidAndBounds(CAPACITY - 10, 11L)
    );

    final byte[] someBytes = new byte[]{0x01, 0x02, 0x03, 0x04};
    memory.putByteArray(10L, someBytes, 0, someBytes.length);

    Memory region = memory.region(10L, someBytes.length);
    Assert.assertEquals(someBytes.length, region.getCapacity());
    Assert.assertEquals(0, region.getCumulativeOffset());
    Assert.assertEquals(2L, region.getCumulativeOffset(2L));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> region.checkValidAndBounds(2L, 4L)
    );

    final byte[] andBack = new byte[someBytes.length];
    region.getByteArray(0L, andBack, 0, someBytes.length);
    Assert.assertArrayEquals(someBytes, andBack);

    Memory differentOrderRegion = memory.region(10L, someBytes.length, ByteOrder.BIG_ENDIAN);
    // different order
    Assert.assertFalse(region.isByteOrderCompatible(differentOrderRegion.getTypeByteOrder()));
    // contents are equal tho
    Assert.assertTrue(region.equalTo(0L, differentOrderRegion, 0L, someBytes.length));
  }

  @Test
  public void testCompareAndEquals()
  {
    WritableMemory memory = getMemory();
    final byte[] someBytes = new byte[]{0x01, 0x02, 0x03, 0x04};
    final byte[] shorterSameBytes = new byte[]{0x01, 0x02, 0x03};
    final byte[] differentBytes = new byte[]{0x02, 0x02, 0x03, 0x04};
    memory.putByteArray(10L, someBytes, 0, someBytes.length);
    memory.putByteArray(400L, someBytes, 0, someBytes.length);
    memory.putByteArray(200L, shorterSameBytes, 0, shorterSameBytes.length);
    memory.putByteArray(500L, differentBytes, 0, differentBytes.length);

    Assert.assertEquals(0, memory.compareTo(10L, someBytes.length, memory, 400L, someBytes.length));
    Assert.assertEquals(4, memory.compareTo(10L, someBytes.length, memory, 200L, someBytes.length));
    Assert.assertEquals(-1, memory.compareTo(10L, someBytes.length, memory, 500L, differentBytes.length));

    WritableMemory memory2 = getMemory();
    memory2.putByteArray(0L, someBytes, 0, someBytes.length);

    Assert.assertEquals(0, memory.compareTo(10L, someBytes.length, memory2, 0L, someBytes.length));

    Assert.assertTrue(memory.equalTo(10L, memory2, 0L, someBytes.length));

    WritableMemory memory3 = getMemory();
    memory2.copyTo(0L, memory3, 0L, CAPACITY);
    Assert.assertTrue(memory2.equalTo(0L, memory3, 0L, CAPACITY));
  }

  @Test
  public void testHash()
  {
    WritableMemory memory = getMemory();
    final long[] someLongs = new long[]{1L, 10L, 100L, 1000L, 10000L};
    final int[] someInts = new int[]{1, 2, 3};
    final byte[] someBytes = new byte[]{0x01, 0x02, 0x03};
    final int longsLength = Long.BYTES * someLongs.length;
    final int someIntsLength = Integer.BYTES * someInts.length;
    final int totalLength = longsLength + someIntsLength + someBytes.length;
    memory.putLongArray(2L, someLongs, 0, someLongs.length);
    memory.putIntArray(2L + longsLength, someInts, 0, someInts.length);
    memory.putByteArray(2L + longsLength + someIntsLength, someBytes, 0, someBytes.length);
    Memory memory2 = Memory.wrap(memory.getByteBuffer(), ByteOrder.LITTLE_ENDIAN);
    Assert.assertEquals(
        memory2.xxHash64(2L, totalLength, 0),
        memory.xxHash64(2L, totalLength, 0)
    );

    Assert.assertEquals(
        memory2.xxHash64(2L, 0),
        memory.xxHash64(2L, 0)
    );
  }

  @Test
  public void testToHexString()
  {

    final byte[] bytes = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
    final WritableMemory memory = getMemory(bytes.length);
    memory.putByteArray(0L, bytes, 0, bytes.length);
    final long hcode = memory.hashCode() & 0XFFFFFFFFL;
    final long bufferhcode = memory.getByteBuffer().hashCode() & 0XFFFFFFFFL;
    final long reqhcode = memory.getMemoryRequestServer().hashCode() & 0XFFFFFFFFL;
    Assert.assertEquals(
        "### SafeWritableMemory SUMMARY ###\n"
        + "Header Comment      : test memory dump\n"
        + "Call Parameters     : .toHexString(..., 0, 8), hashCode: " + hcode + "\n"
        + "UnsafeObj, hashCode : null\n"
        + "UnsafeObjHeader     : 0\n"
        + "ByteBuf, hashCode   : HeapByteBuffer, " + bufferhcode + "\n"
        + "RegionOffset        : 0\n"
        + "Capacity            : 8\n"
        + "CumBaseOffset       : 0\n"
        + "MemReq, hashCode    : HeapByteBufferMemoryRequestServer, " + reqhcode + "\n"
        + "Valid               : true\n"
        + "Read Only           : false\n"
        + "Type Byte Order     : LITTLE_ENDIAN\n"
        + "Native Byte Order   : LITTLE_ENDIAN\n"
        + "JDK Runtime Version : " + UnsafeUtil.JDK + "\n"
        + "Data, littleEndian  :  0  1  2  3  4  5  6  7\n"
        + "                   0: 00 01 02 03 04 05 06 07 \n",
        memory.toHexString("test memory dump", 0, bytes.length)
    );
  }

  @Test
  public void testMisc()
  {
    WritableMemory memory = getMemory(10);
    WritableMemory memory2 = memory.getMemoryRequestServer().request(memory, 20);
    Assert.assertEquals(20, memory2.getCapacity());

    Assert.assertFalse(memory2.hasArray());

    Assert.assertFalse(memory2.isReadOnly());
    Assert.assertFalse(memory2.isDirect());
    Assert.assertTrue(memory2.isValid());
    Assert.assertTrue(memory2.hasByteBuffer());

    Assert.assertFalse(memory2.isSameResource(memory));
    Assert.assertTrue(memory2.isSameResource(memory2));

    // does nothing
    memory.getMemoryRequestServer().requestClose(memory, memory2);
  }

  private WritableMemory getMemory()
  {
    return getMemory(CAPACITY);
  }

  private WritableMemory getMemory(int capacity)
  {
    final ByteBuffer aBuffer = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    return SafeWritableMemory.wrap(aBuffer);
  }
}
