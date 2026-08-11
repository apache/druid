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

package org.apache.druid.spectator.histogram;

import com.netflix.spectator.api.histogram.PercentileBuckets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpectatorHistogramTest
{
  @Test
  public void testToBytesSmallValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.insert(10);
    histogram.insert(30);
    histogram.insert(40);
    histogram.insert(40);
    histogram.insert(40);
    histogram.insert(50);
    histogram.insert(50);
    // Check the full range of bucket IDs still work
    long bigValue = PercentileBuckets.get(270);
    histogram.insert(bigValue);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(8, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    int valSize = 0;
    Assertions.assertEquals(5 * (keySize + valSize), bytes.length, "Should compact small values within key bytes");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(1L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(1L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(3L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(2L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(1L, deserialized.get(PercentileBuckets.indexOf(bigValue)));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(8, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesSmallishValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 64L);
    histogram.add(PercentileBuckets.indexOf(30), 127L);
    histogram.add(PercentileBuckets.indexOf(40), 111L);
    histogram.add(PercentileBuckets.indexOf(50), 99L);
    histogram.add(270, 100L);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(501, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    int valSize = Byte.BYTES;
    Assertions.assertEquals(5 * (keySize + valSize), bytes.length, "Should compact small values to a byte");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(64L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(127L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(111L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(99L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(100L, deserialized.get(270));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(501, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesMedValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 512L);
    histogram.add(PercentileBuckets.indexOf(30), 1024L);
    histogram.add(PercentileBuckets.indexOf(40), 2048L);
    histogram.add(PercentileBuckets.indexOf(50), 4096L);
    histogram.add(270, 8192L);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(15872, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    int valSize = Short.BYTES;
    Assertions.assertEquals(5 * (keySize + valSize), bytes.length, "Should compact medium values to short");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(512L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(1024L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(2048L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(4096L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(8192L, deserialized.get(270));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(15872, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesLargerValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 100000L);
    histogram.add(PercentileBuckets.indexOf(30), 200000L);
    histogram.add(PercentileBuckets.indexOf(40), 500000L);
    histogram.add(PercentileBuckets.indexOf(50), 10000000L);
    histogram.add(270, 50000000L);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(60800000, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    int valSize = Integer.BYTES;
    Assertions.assertEquals(5 * (keySize + valSize), bytes.length, "Should compact larger values to integer");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(100000L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(200000L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(500000L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(10000000L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(50000000L, deserialized.get(270));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(60800000, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesBiggestValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 10000000000L);
    histogram.add(PercentileBuckets.indexOf(30), 20000000000L);
    histogram.add(PercentileBuckets.indexOf(40), 50000000000L);
    histogram.add(PercentileBuckets.indexOf(50), 100000000000L);
    histogram.add(270, 5000000000000L);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(5180000000000L, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    int valSize = Long.BYTES;
    Assertions.assertEquals(5 * (keySize + valSize), bytes.length, "Should not compact larger values");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(10000000000L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(20000000000L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(50000000000L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(100000000000L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(5000000000000L, deserialized.get(270));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(5180000000000L, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesMixedValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 1L);
    histogram.add(PercentileBuckets.indexOf(30), 300L);
    histogram.add(PercentileBuckets.indexOf(40), 200000L);
    histogram.add(PercentileBuckets.indexOf(50), 100000000000L);
    histogram.add(270, 5000000000000L);

    Assertions.assertEquals(5, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(5100000200301L, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    Assertions.assertEquals((5 * keySize) + 0 + 2 + 4 + 8 + 8, bytes.length, "Should not compact larger values");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(1L, deserialized.get(PercentileBuckets.indexOf(10)));
    Assertions.assertEquals(300L, deserialized.get(PercentileBuckets.indexOf(30)));
    Assertions.assertEquals(200000L, deserialized.get(PercentileBuckets.indexOf(40)));
    Assertions.assertEquals(100000000000L, deserialized.get(PercentileBuckets.indexOf(50)));
    Assertions.assertEquals(5000000000000L, deserialized.get(270));

    Assertions.assertEquals(5, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(5100000200301L, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testToBytesBoundaryValues()
  {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(6, 63L);
    histogram.add(7, 64L);
    histogram.add(8, 255L);
    histogram.add(9, 256L);
    histogram.add(16, 65535L);
    histogram.add(17, 65536L);
    histogram.add(32, 4294967295L);
    histogram.add(33, 4294967296L);

    Assertions.assertEquals(8, histogram.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(8590066300L, histogram.getSum(), "Should have sum matching number entries");

    byte[] bytes = histogram.toBytes();
    int keySize = Short.BYTES;
    Assertions.assertEquals((8 * keySize) + 0 + 1 + 1 + 2 + 2 + 4 + 4 + 8, bytes.length, "Should compact");

    SpectatorHistogram deserialized = SpectatorHistogram.deserialize(bytes);
    Assertions.assertEquals(63L, deserialized.get(6));
    Assertions.assertEquals(64L, deserialized.get(7));
    Assertions.assertEquals(255L, deserialized.get(8));
    Assertions.assertEquals(256L, deserialized.get(9));
    Assertions.assertEquals(65535L, deserialized.get(16));
    Assertions.assertEquals(65536L, deserialized.get(17));
    Assertions.assertEquals(4294967295L, deserialized.get(32));
    Assertions.assertEquals(4294967296L, deserialized.get(33));

    Assertions.assertEquals(8, deserialized.size(), "Should have size matching number of buckets");
    Assertions.assertEquals(8590066300L, deserialized.getSum(), "Should have sum matching number entries");
  }

  @Test
  public void testBucketOutOfRangeMax()
  {
    assertThrows(IAE.class, () -> {
      SpectatorHistogram histogram = new SpectatorHistogram();
      histogram.add(500, 1);
    });
  }

  @Test
  public void testBucketOutOfRangeNegative()
  {
    assertThrows(IAE.class, () -> {
      SpectatorHistogram histogram = new SpectatorHistogram();
      histogram.add(-2, 1);
    });
  }

  @Test
  public void testSerializeAndDeserialize() throws IOException
  {
    SegmentWriteOutMedium medium = new OnHeapMemorySegmentWriteOutMedium();
    SpectatorHistogramObjectStrategy strategy = new SpectatorHistogramObjectStrategy();
    Assertions.assertFalse(strategy.readRetainsBufferReference());
    SpectatorHistogramSerializer serializer = SpectatorHistogramSerializer.create(medium, "test", strategy);
    serializer.open();

    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(6, 63L);
    histogram.add(7, 64L);
    histogram.add(8, 255L);
    histogram.add(9, 256L);
    histogram.add(16, 65535L);
    histogram.add(17, 65536L);
    histogram.add(32, 4294967295L);
    histogram.add(33, 4294967296L);

    ColumnValueSelector<SpectatorHistogram> selector = new ColumnValueSelector<>()
    {
      private int callCount = 0;

      @Override
      public boolean isNull()
      {
        return false;
      }

      @Override
      public long getLong()
      {
        return 0;
      }

      @Override
      public float getFloat()
      {
        return 0;
      }

      @Override
      public double getDouble()
      {
        return 0;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public SpectatorHistogram getObject()
      {
        // On every 3rd fetch and after 6, we'll return a null.
        // Columns ending with a lot of nulls won't add to the
        // size of the segment at all.
        ++callCount;
        if ((callCount % 3 == 0) || callCount > 6) {
          return null;
        }
        return histogram;
      }

      @Override
      public Class<? extends SpectatorHistogram> classOfObject()
      {
        return histogram.getClass();
      }
    };

    int count = 0;
    // Serialize lots of nulls at the end to ensure
    // we don't waste space on nulls.
    for (int i = 0; i < 125000; i++) {
      serializer.serialize(selector);
      count++;
    }

    long serializedSize = serializer.getSerializedSize();
    // Column header =  6 bytes
    // Offset header (Size + BitmapLength + ValueBitMap + Offsets)
    //   size          = 4 bytes
    //   bitmap length = 4 bytes
    //   bitmap        = 1 byte
    //   offsets * 4   = 16 bytes (no offset for nulls)
    // Offset header = 25 bytes
    // 4 values      = 152 bytes
    //   each value    = 38 bytes
    // Total = 6 + 25 + 152 = 183
    Assertions.assertEquals(183L, serializedSize, "Expect serialized size");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    serializer.writeTo(channel, null);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assertions.assertEquals(serializer.getSerializedSize(), byteBuffer.remaining());
    SpectatorHistogramIndexed indexedDeserialized = SpectatorHistogramIndexed.read(byteBuffer, strategy);
    Assertions.assertEquals(0, byteBuffer.remaining());

    Assertions.assertEquals(count, indexedDeserialized.size(), "Count of entries should match");

    for (int i = 0; i < count; i++) {
      SpectatorHistogram deserialized = indexedDeserialized.get(i);
      if ((i + 1) % 3 == 0 || i >= 6) {
        // Expect null
        Assertions.assertNull(deserialized);
      } else {
        Assertions.assertEquals(63L, deserialized.get(6));
        Assertions.assertEquals(64L, deserialized.get(7));
        Assertions.assertEquals(255L, deserialized.get(8));
        Assertions.assertEquals(256L, deserialized.get(9));
        Assertions.assertEquals(65535L, deserialized.get(16));
        Assertions.assertEquals(65536L, deserialized.get(17));
        Assertions.assertEquals(4294967295L, deserialized.get(32));
        Assertions.assertEquals(4294967296L, deserialized.get(33));
      }
    }
  }

  @Test
  public void testPercentileComputation0()
  {
    SpectatorHistogram h = new SpectatorHistogram();
    h.insert(0);
    Assertions.assertEquals(0.1, h.getPercentileValue(10.0), 0.01);
    Assertions.assertEquals(0.5, h.getPercentileValue(50.0), 0.01);
    Assertions.assertEquals(0.99, h.getPercentileValue(99.0), 0.01);
    Assertions.assertEquals(1.0, h.getPercentileValue(100.0), 0.01);
  }

  @Test
  public void testPercentileComputation1_100()
  {
    SpectatorHistogram h = new SpectatorHistogram();
    for (int i = 0; i < 100; i++) {
      h.insert(i);
    }
    // Precision assigned to half of the bucket width
    Assertions.assertEquals(10.0, h.getPercentileValue(10.0), 0.5);
    Assertions.assertEquals(50.0, h.getPercentileValue(50.0), 2.5);
    Assertions.assertEquals(99.0, h.getPercentileValue(99.0), 10.5);
    Assertions.assertEquals(100.0, h.getPercentileValue(100.0), 10.5);
  }

  @Test
  public void testPercentileComputation0_Big()
  {
    SpectatorHistogram h = new SpectatorHistogram();
    // one very small value, 99 very big values
    h.add(0, 1);
    h.add(200, 99);
    long upperBoundOfBucket0 = PercentileBuckets.get(0);
    long upperBoundOfBucket200 = PercentileBuckets.get(200);
    long lowerBoundOfBucket200 = PercentileBuckets.get(199);
    long widthOfBucket = upperBoundOfBucket200 - lowerBoundOfBucket200;
    // P1 should be pulled towards the very low value
    // P >1 should be pulled towards the very big value
    Assertions.assertEquals(upperBoundOfBucket0, h.getPercentileValue(1.0), 0.01);
    Assertions.assertEquals(lowerBoundOfBucket200, h.getPercentileValue(50.0), widthOfBucket / 2.0);
    Assertions.assertEquals(upperBoundOfBucket200, h.getPercentileValue(99.0), widthOfBucket / 2.0);
    Assertions.assertEquals(upperBoundOfBucket200, h.getPercentileValue(100.0), widthOfBucket / 2.0);
  }

  @Test
  public void testMedianOfSequence()
  {
    int[] nums = new int[]{9, 10, 12, 13, 13, 13, 15, 15, 16, 16, 18, 22, 23, 24, 24, 25};
    SpectatorHistogram h = new SpectatorHistogram();

    for (int num : nums) {
      h.insert(num);
    }

    // Expect middle of the "15.5" bucket, which is 18.0
    int index = PercentileBuckets.indexOf(15);
    long upperBoundOfFifteenPointFiveBucket = PercentileBuckets.get(index);
    long lowerBoundOfFifteenPointFiveBucket = PercentileBuckets.get(index - 1);
    long halfBucketWidth = ((upperBoundOfFifteenPointFiveBucket - lowerBoundOfFifteenPointFiveBucket) / 2);
    long middleOfFifteenPointFiveBucket = lowerBoundOfFifteenPointFiveBucket + halfBucketWidth;

    Assertions.assertEquals(middleOfFifteenPointFiveBucket, h.getPercentileValue(50.0), 0.01);
  }
}
