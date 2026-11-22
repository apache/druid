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

import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public class MetricCompressionLevelTest
{
  private static final int ROW_COUNT = 20_000;

  @Test
  public void testLz4FastLongRoundTrip() throws Exception
  {
    assertLongRoundTrip(CompressionStrategy.LZ4, 0, CompressionFactory.LongEncodingStrategy.LONGS);
  }

  @Test
  public void testLz4HighLongRoundTrip() throws Exception
  {
    assertLongRoundTrip(CompressionStrategy.LZ4, 17, CompressionFactory.LongEncodingStrategy.LONGS);
  }

  @Test
  public void testZstdLongRoundTrip() throws Exception
  {
    assertLongRoundTrip(CompressionStrategy.ZSTD, 3, CompressionFactory.LongEncodingStrategy.LONGS);
  }

  @Test
  public void testZstdAutoEncodedLongRoundTrip() throws Exception
  {
    assertLongRoundTrip(CompressionStrategy.ZSTD, 3, CompressionFactory.LongEncodingStrategy.AUTO);
  }

  @Test
  public void testLz4FastFloatRoundTrip() throws Exception
  {
    assertFloatRoundTrip(CompressionStrategy.LZ4, 0);
  }

  @Test
  public void testZstdFloatRoundTrip() throws Exception
  {
    assertFloatRoundTrip(CompressionStrategy.ZSTD, 3);
  }

  @Test
  public void testLz4HighDoubleRoundTrip() throws Exception
  {
    assertDoubleRoundTrip(CompressionStrategy.LZ4, 12);
  }

  @Test
  public void testZstdDoubleRoundTrip() throws Exception
  {
    assertDoubleRoundTrip(CompressionStrategy.ZSTD, 3);
  }

  private static void assertLongRoundTrip(
      CompressionStrategy strategy,
      Integer level,
      CompressionFactory.LongEncodingStrategy encoding
  ) throws Exception
  {
    try (SegmentWriteOutMedium medium = new OffHeapMemorySegmentWriteOutMedium()) {
      final ColumnarLongsSerializer serializer = CompressionFactory.getLongSerializer(
          "test",
          medium,
          "metric-long",
          IndexIO.BYTE_ORDER,
          encoding,
          strategy,
          medium.getCloser(),
          level
      );
      serializer.open();
      for (int i = 0; i < ROW_COUNT; i++) {
        serializer.add(expectedLong(i));
      }

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      serializer.writeTo(Channels.newChannel(output), null);
      Assertions.assertEquals(output.size(), serializer.getSerializedSize());

      final CompressedColumnarLongsSupplier supplier = CompressedColumnarLongsSupplier.fromByteBuffer(
          ByteBuffer.wrap(output.toByteArray()),
          IndexIO.BYTE_ORDER,
          null
      );
      try (ColumnarLongs reader = supplier.get()) {
        Assertions.assertEquals(ROW_COUNT, reader.size());
        for (int i = 0; i < ROW_COUNT; i++) {
          Assertions.assertEquals(expectedLong(i), reader.get(i));
        }
      }
    }
  }

  private static void assertFloatRoundTrip(CompressionStrategy strategy, Integer level) throws Exception
  {
    try (SegmentWriteOutMedium medium = new OffHeapMemorySegmentWriteOutMedium()) {
      final ColumnarFloatsSerializer serializer = CompressionFactory.getFloatSerializer(
          "test",
          medium,
          "metric-float",
          IndexIO.BYTE_ORDER,
          strategy,
          medium.getCloser(),
          level
      );
      serializer.open();
      for (int i = 0; i < ROW_COUNT; i++) {
        serializer.add(expectedFloat(i));
      }

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      serializer.writeTo(Channels.newChannel(output), null);
      Assertions.assertEquals(output.size(), serializer.getSerializedSize());

      final CompressedColumnarFloatsSupplier supplier = CompressedColumnarFloatsSupplier.fromByteBuffer(
          ByteBuffer.wrap(output.toByteArray()),
          IndexIO.BYTE_ORDER,
          null
      );
      try (ColumnarFloats reader = supplier.get()) {
        Assertions.assertEquals(ROW_COUNT, reader.size());
        for (int i = 0; i < ROW_COUNT; i++) {
          Assertions.assertEquals(expectedFloat(i), reader.get(i));
        }
      }
    }
  }

  private static void assertDoubleRoundTrip(CompressionStrategy strategy, Integer level) throws Exception
  {
    try (SegmentWriteOutMedium medium = new OffHeapMemorySegmentWriteOutMedium()) {
      final ColumnarDoublesSerializer serializer = CompressionFactory.getDoubleSerializer(
          "test",
          medium,
          "metric-double",
          IndexIO.BYTE_ORDER,
          strategy,
          medium.getCloser(),
          level
      );
      serializer.open();
      for (int i = 0; i < ROW_COUNT; i++) {
        serializer.add(expectedDouble(i));
      }

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      serializer.writeTo(Channels.newChannel(output), null);
      Assertions.assertEquals(output.size(), serializer.getSerializedSize());

      final com.google.common.base.Supplier<ColumnarDoubles> supplier = CompressedColumnarDoublesSuppliers.fromByteBuffer(
          ByteBuffer.wrap(output.toByteArray()),
          IndexIO.BYTE_ORDER,
          null
      );
      try (ColumnarDoubles reader = supplier.get()) {
        Assertions.assertEquals(ROW_COUNT, reader.size());
        for (int i = 0; i < ROW_COUNT; i++) {
          Assertions.assertEquals(expectedDouble(i), reader.get(i));
        }
      }
    }
  }

  private static long expectedLong(int i)
  {
    return (i % 1033) * 17L - (i * 3L);
  }

  private static float expectedFloat(int i)
  {
    return (i % 2003) * 0.25f - i * 0.125f;
  }

  private static double expectedDouble(int i)
  {
    return (i % 1997) * 0.125d - i * 0.0625d;
  }
}
