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

import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.WritableBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SafeWritableBufferTest
{
  private static final int CAPACITY = 1024;

  @Test
  public void testPutAndGet()
  {
    WritableBuffer b1 = getBuffer();
    Assert.assertEquals(0, b1.getPosition());
    b1.putByte((byte) 0x01);
    Assert.assertEquals(1, b1.getPosition());
    b1.putBoolean(true);
    Assert.assertEquals(2, b1.getPosition());
    b1.putBoolean(false);
    Assert.assertEquals(3, b1.getPosition());
    b1.putChar('c');
    Assert.assertEquals(5, b1.getPosition());
    b1.putDouble(1.1);
    Assert.assertEquals(13, b1.getPosition());
    b1.putFloat(1.1f);
    Assert.assertEquals(17, b1.getPosition());
    b1.putInt(100);
    Assert.assertEquals(21, b1.getPosition());
    b1.putLong(1000L);
    Assert.assertEquals(29, b1.getPosition());
    b1.putShort((short) 15);
    Assert.assertEquals(31, b1.getPosition());
    b1.resetPosition();

    Assert.assertEquals(0x01, b1.getByte());
    Assert.assertTrue(b1.getBoolean());
    Assert.assertFalse(b1.getBoolean());
    Assert.assertEquals('c', b1.getChar());
    Assert.assertEquals(1.1, b1.getDouble(), 0.0);
    Assert.assertEquals(1.1f, b1.getFloat(), 0.0);
    Assert.assertEquals(100, b1.getInt());
    Assert.assertEquals(1000L, b1.getLong());
    Assert.assertEquals(15, b1.getShort());
  }

  @Test
  public void testPutAndGetArrays()
  {
    WritableBuffer buffer = getBuffer();
    final byte[] b1 = new byte[]{0x01, 0x02, 0x08, 0x08};
    final byte[] b2 = new byte[b1.length];

    final boolean[] bool1 = new boolean[]{true, false, false, true};
    final boolean[] bool2 = new boolean[bool1.length];

    final char[] chars1 = new char[]{'a', 'b', 'c', 'd'};
    final char[] chars2 = new char[chars1.length];

    final double[] double1 = new double[]{1.1, -2.2, 3.3, 4.4};
    final double[] double2 = new double[double1.length];

    final float[] float1 = new float[]{1.1f, 2.2f, -3.3f, 4.4f};
    final float[] float2 = new float[float1.length];

    final int[] ints1 = new int[]{1, 2, -3, 4};
    final int[] ints2 = new int[ints1.length];

    final long[] longs1 = new long[]{1L, -2L, 3L, -14L};
    final long[] longs2 = new long[ints1.length];

    final short[] shorts1 = new short[]{1, -2, 3, -14};
    final short[] shorts2 = new short[ints1.length];

    buffer.putByteArray(b1, 0, 2);
    buffer.putByteArray(b1, 2, b1.length - 2);
    buffer.putBooleanArray(bool1, 0, bool1.length);
    buffer.putCharArray(chars1, 0, chars1.length);
    buffer.putDoubleArray(double1, 0, double1.length);
    buffer.putFloatArray(float1, 0, float1.length);
    buffer.putIntArray(ints1, 0, ints1.length);
    buffer.putLongArray(longs1, 0, longs1.length);
    buffer.putShortArray(shorts1, 0, shorts1.length);
    long pos = buffer.getPosition();
    buffer.resetPosition();
    buffer.getByteArray(b2, 0, b1.length);
    buffer.getBooleanArray(bool2, 0, bool1.length);
    buffer.getCharArray(chars2, 0, chars1.length);
    buffer.getDoubleArray(double2, 0, double1.length);
    buffer.getFloatArray(float2, 0, float1.length);
    buffer.getIntArray(ints2, 0, ints1.length);
    buffer.getLongArray(longs2, 0, longs1.length);
    buffer.getShortArray(shorts2, 0, shorts1.length);

    Assert.assertArrayEquals(b1, b2);
    Assert.assertArrayEquals(bool1, bool2);
    Assert.assertArrayEquals(chars1, chars2);
    for (int i = 0; i < double1.length; i++) {
      Assert.assertEquals(double1[i], double2[i], 0.0);
    }
    for (int i = 0; i < float1.length; i++) {
      Assert.assertEquals(float1[i], float2[i], 0.0);
    }
    Assert.assertArrayEquals(ints1, ints2);
    Assert.assertArrayEquals(longs1, longs2);
    Assert.assertArrayEquals(shorts1, shorts2);

    Assert.assertEquals(pos, buffer.getPosition());
  }

  @Test
  public void testStartEndRegionAndDuplicate()
  {
    WritableBuffer buffer = getBuffer();
    Assert.assertEquals(0, buffer.getPosition());
    Assert.assertEquals(0, buffer.getStart());
    Assert.assertEquals(CAPACITY, buffer.getEnd());
    Assert.assertEquals(CAPACITY, buffer.getRemaining());
    Assert.assertEquals(CAPACITY, buffer.getCapacity());
    Assert.assertTrue(buffer.hasRemaining());
    buffer.fill((byte) 0x07);
    buffer.setAndCheckStartPositionEnd(10L, 15L, 100L);
    Assert.assertEquals(15L, buffer.getPosition());
    Assert.assertEquals(10L, buffer.getStart());
    Assert.assertEquals(100L, buffer.getEnd());
    Assert.assertEquals(85L, buffer.getRemaining());
    Assert.assertEquals(CAPACITY, buffer.getCapacity());
    buffer.fill((byte) 0x70);
    buffer.resetPosition();
    Assert.assertEquals(10L, buffer.getPosition());
    for (int i = 0; i < 90; i++) {
      if (i < 5) {
        Assert.assertEquals(0x07, buffer.getByte());
      } else {
        Assert.assertEquals(0x70, buffer.getByte());
      }
    }
    buffer.setAndCheckPosition(50);

    Buffer duplicate = buffer.duplicate();
    Assert.assertEquals(buffer.getStart(), duplicate.getStart());
    Assert.assertEquals(buffer.getPosition(), duplicate.getPosition());
    Assert.assertEquals(buffer.getEnd(), duplicate.getEnd());
    Assert.assertEquals(buffer.getRemaining(), duplicate.getRemaining());
    Assert.assertEquals(buffer.getCapacity(), duplicate.getCapacity());

    duplicate.resetPosition();
    for (int i = 0; i < 90; i++) {
      if (i < 5) {
        Assert.assertEquals(0x07, duplicate.getByte());
      } else {
        Assert.assertEquals(0x70, duplicate.getByte());
      }
    }

    Buffer region = buffer.region(5L, 105L, buffer.getTypeByteOrder());
    Assert.assertEquals(0, region.getStart());
    Assert.assertEquals(0, region.getPosition());
    Assert.assertEquals(105L, region.getEnd());
    Assert.assertEquals(105L, region.getRemaining());
    Assert.assertEquals(105L, region.getCapacity());

    for (int i = 0; i < 105; i++) {
      if (i < 10) {
        Assert.assertEquals(0x07, region.getByte());
      } else if (i < 95) {
        Assert.assertEquals(0x70, region.getByte());
      } else {
        Assert.assertEquals(0x07, region.getByte());
      }
    }
  }

  @Test
  public void testFill()
  {
    WritableBuffer buffer = getBuffer();
    WritableBuffer anotherBuffer = getBuffer();

    buffer.fill((byte) 0x0F);
    anotherBuffer.fill((byte) 0x0F);
    Assert.assertTrue(buffer.equalTo(0L, anotherBuffer, 0L, CAPACITY));

    anotherBuffer.setPosition(100);
    anotherBuffer.clear();
    Assert.assertFalse(buffer.equalTo(0L, anotherBuffer, 0L, CAPACITY));
    Assert.assertTrue(buffer.equalTo(0L, anotherBuffer, 0L, 100L));
  }

  private WritableBuffer getBuffer()
  {
    return getBuffer(CAPACITY);
  }

  private WritableBuffer getBuffer(int capacity)
  {
    final ByteBuffer aBuffer = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    SafeWritableBuffer memory = new SafeWritableBuffer(aBuffer);
    return memory;
  }
}
