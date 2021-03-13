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
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@RunWith(Enclosed.class)
public class VSizeLongSerdeTest
{
  @RunWith(Parameterized.class)
  public static class EveryLittleBitTest
  {
    private final int numBits;

    public EveryLittleBitTest(int numBits)
    {
      this.numBits = numBits;
    }

    @Parameterized.Parameters(name = "numBits={0}")
    public static Collection<Object[]> data()
    {
      return Arrays.stream(VSizeLongSerde.SUPPORTED_SIZES)
                   .mapToObj(value -> new Object[]{value})
                   .collect(Collectors.toList());
    }

    @Test
    public void testEveryPowerOfTwo() throws IOException
    {
      // Test every long that has a single bit set.

      final int numLongs = Math.min(64, numBits);
      final long[] longs = new long[numLongs];

      for (int bit = 0; bit < numLongs; bit++) {
        longs[bit] = 1L << bit;
      }

      testSerde(numBits, longs);
    }

    @Test
    public void testEveryPowerOfTwoMinusOne() throws IOException
    {
      // Test every long with runs of low bits set.

      final int numLongs = Math.min(64, numBits + 1);
      final long[] longs = new long[numLongs];

      for (int bit = 0; bit < numLongs; bit++) {
        longs[bit] = (1L << bit) - 1;
      }

      testSerde(numBits, longs);
    }
  }

  public static class SpecificValuesTest
  {
    private final long[] values0 = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
    private final long[] values1 = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
    private final long[] values2 = {12, 5, 2, 9, 3, 2, 5, 1, 0, 6, 13, 10, 15};
    private final long[] values3 = {1, 1, 1, 1, 1, 11, 11, 11, 11};
    private final long[] values4 = {200, 200, 200, 401, 200, 301, 200, 200, 200, 404, 200, 200, 200, 200};
    private final long[] values5 = {123, 632, 12, 39, 536, 0, 1023, 52, 777, 526, 214, 562, 823, 346};
    private final long[] values6 = {1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008};

    @Test
    public void testGetBitsForMax()
    {
      Assert.assertEquals(1, VSizeLongSerde.getBitsForMax(1));
      Assert.assertEquals(1, VSizeLongSerde.getBitsForMax(2));
      Assert.assertEquals(2, VSizeLongSerde.getBitsForMax(3));
      Assert.assertEquals(4, VSizeLongSerde.getBitsForMax(16));
      Assert.assertEquals(8, VSizeLongSerde.getBitsForMax(200));
      Assert.assertEquals(12, VSizeLongSerde.getBitsForMax(999));
      Assert.assertEquals(24, VSizeLongSerde.getBitsForMax(12345678));
      Assert.assertEquals(32, VSizeLongSerde.getBitsForMax(Integer.MAX_VALUE));
      Assert.assertEquals(64, VSizeLongSerde.getBitsForMax(Long.MAX_VALUE));
    }

    @Test
    public void testSerdeValues() throws IOException
    {
      for (int i : VSizeLongSerde.SUPPORTED_SIZES) {
        testSerde(i, values0);
        if (i >= 1) {
          testSerde(i, values1);
        }
        if (i >= 4) {
          testSerde(i, values2);
          testSerde(i, values3);
        }
        if (i >= 9) {
          testSerde(i, values4);
        }
        if (i >= 10) {
          testSerde(i, values5);
        }
        if (i >= 20) {
          testSerde(i, values6);
        }
      }
    }

    @Test
    public void testSerdeLoop() throws IOException
    {
      final long[] zeroTo256 = generateSequentialLongs(0, 256);
      final long[] zeroTo50000 = generateSequentialLongs(0, 50000);

      for (int i : VSizeLongSerde.SUPPORTED_SIZES) {
        if (i >= 8) {
          testSerde(i, zeroTo256);
        }
        if (i >= 16) {
          testSerde(i, zeroTo50000);
        }
      }
    }

    private long[] generateSequentialLongs(final long start, final long end)
    {
      final long[] values = new long[Ints.checkedCast(end - start)];

      for (int i = 0; i < values.length; i++) {
        values[i] = start + i;
      }

      return values;
    }
  }

  public static void testSerde(int numBits, long[] values) throws IOException
  {
    final int bufferOffset = 1;
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    outStream.write(0xAF); // Dummy byte so the real stuff starts at bufferOffset

    final ByteBuffer buffer =
        ByteBuffer.allocate(VSizeLongSerde.getSerializedSize(numBits, values.length) + bufferOffset);
    buffer.rewind();
    buffer.put(0, (byte) 0xAF); // Dummy byte again.
    VSizeLongSerde.LongSerializer streamSer = VSizeLongSerde.getSerializer(numBits, outStream);
    VSizeLongSerde.LongSerializer bufferSer = VSizeLongSerde.getSerializer(numBits, buffer, bufferOffset);
    for (long value : values) {
      streamSer.write(value);
      bufferSer.write(value);
    }
    streamSer.close();
    bufferSer.close();

    // Verify serialized sizes.
    final ByteBuffer bufferFromStream = ByteBuffer.wrap(outStream.toByteArray());
    Assert.assertEquals(
        StringUtils.format("Serialized size (stream, numBits = %d)", numBits),
        VSizeLongSerde.getSerializedSize(numBits, values.length),
        bufferFromStream.capacity() - bufferOffset
    );
    Assert.assertEquals(
        StringUtils.format("Serialized size (buffer, numBits = %d)", numBits),
        VSizeLongSerde.getSerializedSize(numBits, values.length),
        buffer.position() - bufferOffset
    );

    // Verify the actual serialized contents.
    Assert.assertArrayEquals(
        StringUtils.format("Stream and buffer serialized images are equal (numBits = %d)", numBits),
        bufferFromStream.array(),
        buffer.array()
    );

    // Verify deserialization. We know the two serialized buffers are equal, so from this point on, just use one.
    VSizeLongSerde.LongDeserializer deserializer = VSizeLongSerde.getDeserializer(numBits, buffer, bufferOffset);

    testGetSingleRow(deserializer, numBits, values);
    testContiguousGetSingleRow(deserializer, numBits, values);
    testContiguousGetWholeRegion(deserializer, numBits, values);
    testNoncontiguousGetSingleRow(deserializer, numBits, values);
    testNoncontiguousGetEveryOtherValue(deserializer, numBits, values);
    testNoncontiguousGetEveryOtherValueWithLimit(deserializer, numBits, values);
  }

  private static void testGetSingleRow(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(
          StringUtils.format("Deserializer (testGetSingleRow, numBits = %d, position = %d)", numBits, i),
          values[i],
          deserializer.get(i)
      );
    }
  }

  private static void testContiguousGetSingleRow(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    final int outPosition = 1;
    final long[] out = new long[values.length + outPosition];

    for (int i = 0; i < values.length; i++) {

      Arrays.fill(out, -1);
      deserializer.getDelta(out, outPosition, i, 1, 0);

      Assert.assertEquals(
          StringUtils.format("Deserializer (testContiguousGetSingleRow, numBits = %d, position = %d)", numBits, i),
          values[i],
          out[outPosition]
      );
    }
  }

  private static void testContiguousGetWholeRegion(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    final int outPosition = 1;
    final long[] out = new long[values.length + outPosition];
    Arrays.fill(out, -1);
    deserializer.getDelta(out, outPosition, 0, values.length, 0);

    Assert.assertArrayEquals(
        StringUtils.format("Deserializer (testContiguousGetWholeRegion, numBits = %d)", numBits),
        values,
        Arrays.stream(out).skip(outPosition).toArray()
    );
  }

  private static void testNoncontiguousGetSingleRow(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    final int indexOffset = 1;
    final int outPosition = 1;
    final long[] out = new long[values.length + outPosition];
    final int[] indexes = new int[values.length + outPosition];

    for (int i = 0; i < values.length; i++) {
      Arrays.fill(out, -1);
      Arrays.fill(indexes, -1);
      indexes[outPosition] = i + indexOffset;

      deserializer.getDelta(out, outPosition, indexes, 1, indexOffset, values.length, 0);

      Assert.assertEquals(
          StringUtils.format("Deserializer (testNoncontiguousGetSingleRow, numBits = %d, position = %d)", numBits, i),
          values[i],
          out[outPosition]
      );
    }
  }

  private static void testNoncontiguousGetEveryOtherValue(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    final int indexOffset = 1;
    final int outPosition = 1;
    final long[] out = new long[values.length + outPosition];
    final long[] expectedOut = new long[values.length + outPosition];
    final int[] indexes = new int[values.length + outPosition];

    Arrays.fill(out, -1);
    Arrays.fill(expectedOut, -1);
    Arrays.fill(indexes, -1);

    int cnt = 0;
    for (int i = 0; i < values.length; i++) {
      if (i % 2 == 0) {
        indexes[outPosition + i / 2] = i + indexOffset;
        expectedOut[outPosition + i / 2] = values[i];
        cnt++;
      }
    }

    deserializer.getDelta(out, outPosition, indexes, cnt, indexOffset, values.length, 0);

    Assert.assertArrayEquals(
        StringUtils.format("Deserializer (testNoncontiguousGetEveryOtherValue, numBits = %d)", numBits),
        expectedOut,
        out
    );
  }

  private static void testNoncontiguousGetEveryOtherValueWithLimit(
      final VSizeLongSerde.LongDeserializer deserializer,
      final int numBits,
      final long[] values
  )
  {
    final int indexOffset = 1;
    final int outPosition = 1;
    final long[] out = new long[values.length + outPosition];
    final long[] expectedOut = new long[values.length + outPosition];
    final int[] indexes = new int[values.length + outPosition];
    final int limit = values.length - 2; // Don't do the last value

    Arrays.fill(out, -1);
    Arrays.fill(expectedOut, -1);
    Arrays.fill(indexes, -1);

    int cnt = 0;
    for (int i = 0; i < values.length; i++) {
      if (i % 2 == 0) {
        indexes[outPosition + i / 2] = i + indexOffset;

        if (i < limit) {
          expectedOut[outPosition + i / 2] = values[i];
        }

        cnt++;
      }
    }

    final int ret = deserializer.getDelta(out, outPosition, indexes, cnt, indexOffset, limit, 0);

    Assert.assertArrayEquals(
        StringUtils.format("Deserializer (testNoncontiguousGetEveryOtherValue, numBits = %d)", numBits),
        expectedOut,
        out
    );

    Assert.assertEquals(Math.max(0, cnt - 1), ret);
  }
}
