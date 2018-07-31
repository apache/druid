/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import io.druid.java.util.common.StringUtils;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(Parameterized.class)
public class CompressedLongsAutoEncodingSerdeTest
{
  @Parameterized.Parameters(name = "{0} {1} {2}")
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (long bpv : bitsPerValueParameters) {
      for (CompressionStrategy strategy : CompressionStrategy.values()) {
        data.add(new Object[]{bpv, strategy, ByteOrder.BIG_ENDIAN});
        data.add(new Object[]{bpv, strategy, ByteOrder.LITTLE_ENDIAN});
      }
    }
    return data;
  }

  private static final long[] bitsPerValueParameters = new long[]{1, 2, 4, 7, 11, 14, 18, 23, 31, 39, 46, 55, 62};

  protected final CompressionFactory.LongEncodingStrategy encodingStrategy = CompressionFactory.LongEncodingStrategy.AUTO;
  protected final CompressionStrategy compressionStrategy;
  protected final ByteOrder order;
  protected final long bitsPerValue;

  public CompressedLongsAutoEncodingSerdeTest(
      long bitsPerValue,
      CompressionStrategy compressionStrategy,
      ByteOrder order
  )
  {
    this.bitsPerValue = bitsPerValue;
    this.compressionStrategy = compressionStrategy;
    this.order = order;
  }

  @Test
  public void testFidelity() throws Exception
  {
    final long bound = 1L << bitsPerValue;
    // big enough to have at least 2 blocks, and a handful of sizes offset by 1 from each other
    int blockSize = 1 << 16;
    int numBits = (Long.SIZE - Long.numberOfLeadingZeros(1 << (bitsPerValue - 1)));
    double numValuesPerByte = 8.0 / (double) numBits;

    int numRows = (int) (blockSize * numValuesPerByte) * 2 + ThreadLocalRandom.current().nextInt(1, 101);
    long chunk[] = new long[numRows];
    for (int i = 0; i < numRows; i++) {
      chunk[i] = ThreadLocalRandom.current().nextLong(bound);
    }
    testValues(chunk);

    numRows++;
    chunk = new long[numRows];
    for (int i = 0; i < numRows; i++) {
      chunk[i] = ThreadLocalRandom.current().nextLong(bound);
    }
    testValues(chunk);
  }

  public void testValues(long[] values) throws Exception
  {
    ColumnarLongsSerializer serializer = CompressionFactory.getLongSerializer(
        new OffHeapMemorySegmentWriteOutMedium(),
        "test",
        order,
        encodingStrategy,
        compressionStrategy
    );
    serializer.open();

    for (long value : values) {
      serializer.add(value);
    }
    Assert.assertEquals(values.length, serializer.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.writeTo(Channels.newChannel(baos), null);
    Assert.assertEquals(baos.size(), serializer.getSerializedSize());
    CompressedColumnarLongsSupplier supplier =
        CompressedColumnarLongsSupplier.fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order);
    ColumnarLongs longs = supplier.get();

    assertIndexMatchesVals(longs, values);
    longs.close();
  }

  private void assertIndexMatchesVals(ColumnarLongs indexed, long[] vals)
  {
    Assert.assertEquals(vals.length, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(
          StringUtils.format(
              "Value [%d] at row '%d' does not match [%d]",
              indexed.get(i),
              i,
              vals[i]
          ),
          vals[i],
          indexed.get(i)
      );
    }
  }
}
