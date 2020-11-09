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

package org.apache.druid.compressedbigdecimal;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.apache.druid.compressedbigdecimal.Utils.accumulate;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Unit tests for CompressedBigDecimal.
 */
public class ArrayCompressedBigDecimalTest
{

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal(long, int)}.
   */
  @Test
  public void testLongConstructorZero()
  {
    // Validate simple 0 case with longs.
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(0, 0);
    d.reset();
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(0, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testLongConstructorPositive()
  {
    // Validate positive number that doesn't flow into the next int
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(Integer.MAX_VALUE, 9);
    ArrayCompressedBigDecimal dl = d;
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(Integer.MAX_VALUE, array[0]);
    assertEquals(0, array[1]);
    assertEquals(0, d.compareTo(new ArrayCompressedBigDecimal(Integer.MAX_VALUE, 9)));
    assertEquals(0, d.compareTo(dl));


  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testLongConstructorNegative()
  {
    // validate negative number correctly fills in upper bits.
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(Integer.MIN_VALUE, 5);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(Integer.MIN_VALUE, array[0]);
    assertEquals(-1, array[1]);
    assertEquals(-21475, d.intValue());
    assertEquals(-21475, d.longValue());
    assertEquals(-21475, d.shortValue());

  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorZero()
  {
    // simple zero case to test short circuiting
    BigDecimal bd = new BigDecimal(0);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(0, array[0]);
    assertEquals("0", d.toString());
    assertEquals(0, d.signum());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorSmallPositive()
  {
    // simple one int positive example
    BigDecimal bd = new BigDecimal(Integer.MAX_VALUE).scaleByPowerOfTen(-9);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(Integer.MAX_VALUE, array[0]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorSmallNegative()
  {
    // simple one int negative example
    BigDecimal bd = new BigDecimal(Integer.MIN_VALUE).scaleByPowerOfTen(-5);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(Integer.MIN_VALUE, array[0]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorLargePositive()
  {
    // simple two int positive example
    BigDecimal bd = new BigDecimal(Long.MAX_VALUE).scaleByPowerOfTen(-9);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(-1, array[0]);
    assertEquals(Integer.MAX_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorLargeNegative()
  {
    // simple two int negative example
    BigDecimal bd = new BigDecimal(Long.MIN_VALUE).scaleByPowerOfTen(-5);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(Integer.MIN_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorUnevenMultiplePositive()
  {
    // test positive when number of bytes in BigDecimal isn't an even multiple of sizeof(int)
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(-1, array[0]);
    assertEquals(0x7f, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorUnevenMultipleNegative()
  {
    // test negative when number of bytes in BigDecimal isn't an even multiple of sizeof(int)
    BigDecimal bd = new BigDecimal(new BigInteger(-1, new byte[] {Byte.MIN_VALUE, 0, 0, 0, 0}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(Byte.MIN_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal(CompressedBigDecimal)}.
   */
  @Test
  public void testCopyConstructor()
  {
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);

    ArrayCompressedBigDecimal d2 = new ArrayCompressedBigDecimal(d);
    assertEquals(d.getScale(), d2.getScale());
    assertArrayEquals(d.getArray(), d2.getArray());
    assertNotSame(d.getArray(), d2.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#wrap(int[], int)}.
   */
  @Test
  public void testWrap()
  {
    int[] array = new int[] {Integer.MAX_VALUE, -1};
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(array, 0);
    assertSame(array, bd.getArray());
    assertEquals(0, bd.getScale());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#allocate(int, int)}.
   */
  @Test
  public void testAllocate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 5);
    assertEquals(5, bd.getScale());
    assertEquals(2, bd.getArray().length);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd.accumulate(add);
    assertArrayEquals(new int[] {1, 0}, bd.getArray());
    bd.accumulate(add);
    assertArrayEquals(new int[] {2, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulateOverflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0x80000000, 0}, 0);
    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {0x7fffffff, 0}, 0);
    ArrayCompressedBigDecimal add1 = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
    bd.accumulate(add1);
    assertArrayEquals(new int[] {0, 1}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulateUnderflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {-1, -1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testUnevenAccumulateUnderflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {-1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testUnevenAccumulateOverflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0xffffffff, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0, 2}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUnevenAccumulateOverflowWithTruncate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {Integer.MAX_VALUE}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {1, 1}, 0);

    bd.accumulate(add);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAccumulateScaleMismatch()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 1);
    ArrayCompressedBigDecimal add = new ArrayCompressedBigDecimal(1, 0);
    bd.accumulate(add);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#toBigDecimal()}.
   */
  @Test
  public void testToBigDecimal()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {1}, 0);
    assertEquals(BigDecimal.ONE, bd.toBigDecimal());

    bd = ArrayCompressedBigDecimal.wrap(new int[] {Integer.MAX_VALUE}, 0);
    assertEquals(new BigDecimal(Integer.MAX_VALUE), bd.toBigDecimal());

    bd = ArrayCompressedBigDecimal.wrap(new int[] {0}, 0);
    assertEquals(BigDecimal.ZERO, bd.toBigDecimal());
    bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 0}, 0);
    assertEquals(BigDecimal.ZERO, bd.toBigDecimal());
    bd = new ArrayCompressedBigDecimal(-1, 9);
    assertEquals(new BigDecimal(-1).scaleByPowerOfTen(-9), bd.toBigDecimal());
    bd = ArrayCompressedBigDecimal.wrap(new int[] {1410065408, 2}, 9);
    assertEquals(new BigDecimal(10).setScale(9), bd.toBigDecimal());
  }

  /**
   * Test method for {@link ByteBufferCompressedBigDecimal()}.
   */
  @Test
  public void testBigDecimalConstructorwithByteBuffer()
  {
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    ByteBuffer buf = ByteBuffer.allocate(4);
    CompressedBigDecimal cbd = new ByteBufferCompressedBigDecimal(buf, 0, d);
    assertEquals(0, cbd.getScale());
    assertEquals(8388607, cbd.intValue());
    assertEquals(new Long(8388607L).doubleValue(), cbd.floatValue(), 0.001);
    assertEquals(new Long(8388607L).doubleValue(), cbd.doubleValue(), 0.001);
  }

  /**
   * Test method for {@link  ArrayCompressedBigDecimal#setArrayEntry
   */
  @Test
  public void testSetArrayEntry()
  {
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    d.setArrayEntry(0, 2);
    assertEquals(2, d.intValue());
  }

  /**
   * Test method for {@link  ByteBufferCompressedBigDecimal#copyToBuffer(ByteBuffer, int, int, CompressedBigDecimal)}
   */
  @Test
  public void testCopyToBuffer()
  {
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 4});
    ByteBufferCompressedBigDecimal bbdl = new ByteBufferCompressedBigDecimal(bb, 0, 1, 0);
    bbdl.setArrayEntry(0, 2);
    assertEquals(2, bbdl.intValue());
  }

  /**
   * Test method for {@link Utils#accumulate(ByteBuffer, int, int, int, CompressedBigDecimal)}
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUtilsAccumulateByteBuf()
  {
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    ByteBuffer buf = ByteBuffer.allocate(4);
    accumulate(buf, 0, 1, 2, new ArrayCompressedBigDecimal(new BigDecimal(Long.MAX_VALUE)));
  }

  /**
   * Test method for {@link Utils#accumulate(CompressedBigDecimal, long, int)}
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUtilsAccumulateCbdWithExeception()
  {
    BigDecimal bd = new BigDecimal(new BigInteger("1"));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    accumulate(d, 0L, 1);
  }

  /**
   * Test method for {@link Utils#accumulate(CompressedBigDecimal, long, int)}
   */
  @Test
  public void testUtilsAccumulateCbd()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 0);
    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd.accumulate(add);
    accumulate(bd, 1, 0);
    assertEquals("2", bd.toString());
    CompressedBigDecimal x = accumulate(bd, new BigDecimal("2"));
    assertEquals(4, x.intValue());

    CompressedBigDecimalObjectStrategy c1 = new CompressedBigDecimalObjectStrategy();
    c1.compare(bd, add);
  }

  /**
   * Test method for {@link CompressedBigDecimalObjectStrategy
   */
  @Test
  public void testCompressedBigDecimalObjectStrategy()
  {
    ArrayCompressedBigDecimal bd;
    ArrayCompressedBigDecimal acd = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd = acd;
    CompressedBigDecimalObjectStrategy c1 = new CompressedBigDecimalObjectStrategy();

    BigDecimal d = new BigDecimal(new BigInteger(1, new byte[] {0, 0, 1}));
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 4});
    CompressedBigDecimal cbl = c1.fromByteBuffer(bb, 8);
    byte[] bf = c1.toBytes(bd);
    ArrayCompressedBigDecimal cbd = new ArrayCompressedBigDecimal(new BigDecimal(new BigInteger(1, bf)));

    assertEquals(67108864, cbl.intValue());
    assertEquals(0, c1.compare(bd, acd));
    assertEquals(0, cbd.intValue());
  }

}
