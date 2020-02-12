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


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class VSizeLongSerdeTest
{
  private ByteBuffer buffer;
  private ByteArrayOutputStream outStream;
  private ByteBuffer outBuffer;
  private final long[] values0 = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
  private final long[] values1 = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
  private final long[] values2 = {12, 5, 2, 9, 3, 2, 5, 1, 0, 6, 13, 10, 15};
  private final long[] values3 = {1, 1, 1, 1, 1, 11, 11, 11, 11};
  private final long[] values4 = {200, 200, 200, 401, 200, 301, 200, 200, 200, 404, 200, 200, 200, 200};
  private final long[] values5 = {123, 632, 12, 39, 536, 0, 1023, 52, 777, 526, 214, 562, 823, 346};
  private final long[] values6 = {1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008};

  @Before
  public void setUp()
  {
    outStream = new ByteArrayOutputStream();
    outBuffer = ByteBuffer.allocate(500000);
  }

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
    for (int i : VSizeLongSerde.SUPPORTED_SIZES) {
      if (i >= 8) {
        testSerdeIncLoop(i, 0, 256);
      }
      if (i >= 16) {
        testSerdeIncLoop(i, 0, 50000);
      }
    }
  }

  public void testSerde(int longSize, long[] values) throws IOException
  {
    outBuffer.rewind();
    outStream.reset();
    VSizeLongSerde.LongSerializer streamSer = VSizeLongSerde.getSerializer(longSize, outStream);
    VSizeLongSerde.LongSerializer bufferSer = VSizeLongSerde.getSerializer(longSize, outBuffer, 0);
    for (long value : values) {
      streamSer.write(value);
      bufferSer.write(value);
    }
    streamSer.close();
    bufferSer.close();

    buffer = ByteBuffer.wrap(outStream.toByteArray());
    Assert.assertEquals(VSizeLongSerde.getSerializedSize(longSize, values.length), buffer.capacity());
    Assert.assertEquals(VSizeLongSerde.getSerializedSize(longSize, values.length), outBuffer.position());
    VSizeLongSerde.LongDeserializer streamDes = VSizeLongSerde.getDeserializer(longSize, buffer, 0);
    VSizeLongSerde.LongDeserializer bufferDes = VSizeLongSerde.getDeserializer(longSize, outBuffer, 0);
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i], streamDes.get(i));
      Assert.assertEquals(values[i], bufferDes.get(i));
    }
  }

  public void testSerdeIncLoop(int longSize, long start, long end) throws IOException
  {
    outBuffer.rewind();
    outStream.reset();
    VSizeLongSerde.LongSerializer streamSer = VSizeLongSerde.getSerializer(longSize, outStream);
    VSizeLongSerde.LongSerializer bufferSer = VSizeLongSerde.getSerializer(longSize, outBuffer, 0);
    for (long i = start; i < end; i++) {
      streamSer.write(i);
      bufferSer.write(i);
    }
    streamSer.close();
    bufferSer.close();

    buffer = ByteBuffer.wrap(outStream.toByteArray());
    Assert.assertEquals(VSizeLongSerde.getSerializedSize(longSize, (int) (end - start)), buffer.capacity());
    Assert.assertEquals(VSizeLongSerde.getSerializedSize(longSize, (int) (end - start)), outBuffer.position());
    VSizeLongSerde.LongDeserializer streamDes = VSizeLongSerde.getDeserializer(longSize, buffer, 0);
    VSizeLongSerde.LongDeserializer bufferDes = VSizeLongSerde.getDeserializer(longSize, outBuffer, 0);
    for (int i = 0; i < end - start; i++) {
      Assert.assertEquals(start + i, streamDes.get(i));
      Assert.assertEquals(start + i, bufferDes.get(i));
    }
  }


}
