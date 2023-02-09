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

package org.apache.druid.benchmark.compression;

import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.segment.BitmapOffset;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.ColumnarLongsSerializer;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.vector.BitmapVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@State(Scope.Benchmark)
public class BaseColumnarLongsBenchmark
{
  static final int VECTOR_SIZE = 512;

  Map<String, ColumnarLongs> decoders = new HashMap<>();
  Map<String, Integer> encodedSize = new HashMap<>();
  /**
   * Name of the long encoding strategy. For longs, this is a composite of both byte level block compression and
   * encoding of values within the block.
   */
  @Param({
      "zstd-longs",
      "lz4-longs",
      "zstd-auto",
      "lz4-auto"
  })
  String encoding;

  Random rand = new Random(0);

  long[] vals;

  long minValue;
  long maxValue;

  Offset offset;
  VectorOffset vectorOffset;


  void scan(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    ColumnarLongs encoder = decoders.get(encoding);
    while (offset.withinBounds()) {
      blackhole.consume(encoder.get(offset.getOffset()));
      offset.increment();
    }
    offset.reset();
    blackhole.consume(offset);
  }

  void scanVectorized(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    ColumnarLongs columnDecoder = decoders.get(encoding);
    long[] vector = new long[VECTOR_SIZE];
    while (!vectorOffset.isDone()) {
      if (vectorOffset.isContiguous()) {
        columnDecoder.get(vector, vectorOffset.getStartOffset(), vectorOffset.getCurrentVectorSize());
      } else {
        columnDecoder.get(vector, vectorOffset.getOffsets(), vectorOffset.getCurrentVectorSize());
      }
      for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++) {
        blackhole.consume(vector[i]);
      }
      vectorOffset.advance();
    }
    blackhole.consume(vector);
    blackhole.consume(vectorOffset);
    vectorOffset.reset();
    columnDecoder.close();
  }

  void setupFilters(int rows, double filteredRowCountPercentage, String filterDistribution)
  {
    final int filteredRowCount = (int) Math.floor(rows * filteredRowCountPercentage);


    if (filteredRowCount < rows) {
      switch (filterDistribution) {
        case "random":
          setupRandomFilter(rows, filteredRowCount);
          break;
        case "contiguous-start":
          offset = new SimpleAscendingOffset(rows);
          vectorOffset = new NoFilterVectorOffset(VECTOR_SIZE, 0, filteredRowCount);
          break;
        case "contiguous-end":
          offset = new SimpleAscendingOffset(rows);
          vectorOffset = new NoFilterVectorOffset(VECTOR_SIZE, rows - filteredRowCount, rows);
          break;
        case "contiguous-bitmap-start":
          setupContiguousBitmapFilter(rows, filteredRowCount, 0);
          break;
        case "contiguous-bitmap-end":
          setupContiguousBitmapFilter(rows, filteredRowCount, rows - filteredRowCount);
          break;
        case "chunky-1000":
          setupChunkyFilter(rows, filteredRowCount, 1000);
          break;
        case "chunky-10000":
          setupChunkyFilter(rows, filteredRowCount, 10000);
          break;
        default:
          throw new IllegalArgumentException("unknown filter distribution");
      }
    } else {
      offset = new SimpleAscendingOffset(rows);
      vectorOffset = new NoFilterVectorOffset(VECTOR_SIZE, 0, rows);
    }
  }

  private void setupRandomFilter(int rows, int filteredRowCount)
  {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int i = 0; i < filteredRowCount; i++) {
      int rowToAccess = rand.nextInt(rows);
      // Skip already selected rows if any
      while (bitmap.contains(rowToAccess)) {
        rowToAccess = rand.nextInt(rows);
      }
      bitmap.add(rowToAccess);
    }
    offset = BitmapOffset.of(
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        false,
        rows
    );
    vectorOffset = new BitmapVectorOffset(
        VECTOR_SIZE,
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        0,
        rows
    );
  }

  private void setupContiguousBitmapFilter(int rows, int filterRowCount, int startOffset)
  {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int i = startOffset; i < filterRowCount; i++) {
      bitmap.add(i);
    }
    offset = BitmapOffset.of(
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        false,
        rows
    );
    vectorOffset = new BitmapVectorOffset(
        VECTOR_SIZE,
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        startOffset,
        rows
    );
  }

  private void setupChunkyFilter(int rows, int filteredRowCount, int chunkSize)
  {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int count = 0; count < filteredRowCount; ) {
      int chunkOffset = rand.nextInt(rows - chunkSize);
      // Skip already selected rows if any
      while (bitmap.contains(chunkOffset)) {
        chunkOffset = rand.nextInt(rows - chunkSize);
      }
      int numAdded = 0;
      for (; numAdded < chunkSize && count + numAdded < filteredRowCount; numAdded++) {
        // break if we run into an existing contiguous section
        if (bitmap.contains(numAdded)) {
          break;
        }
        bitmap.add(chunkOffset + numAdded);
      }
      count += numAdded;
    }
    offset = BitmapOffset.of(
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        false,
        rows
    );
    vectorOffset = new BitmapVectorOffset(
        VECTOR_SIZE,
        new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap()),
        0,
        rows
    );
  }

  static int encodeToFile(long[] vals, String encoding, FileChannel output)throws IOException
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();

    ColumnarLongsSerializer serializer;
    switch (encoding) {
      case "lz4-longs":
        serializer = CompressionFactory.getLongSerializer(
            encoding,
            writeOutMedium,
            "lz4-longs",
            ByteOrder.LITTLE_ENDIAN,
            CompressionFactory.LongEncodingStrategy.LONGS,
            CompressionStrategy.LZ4
        );
        break;
      case "lz4-auto":
        serializer = CompressionFactory.getLongSerializer(
            encoding,
            writeOutMedium,
            "lz4-auto",
            ByteOrder.LITTLE_ENDIAN,
            CompressionFactory.LongEncodingStrategy.AUTO,
            CompressionStrategy.LZ4
        );
        break;
      case "none-longs":
        serializer = CompressionFactory.getLongSerializer(
            encoding,
            writeOutMedium,
            "none-longs",
            ByteOrder.LITTLE_ENDIAN,
            CompressionFactory.LongEncodingStrategy.LONGS,
            CompressionStrategy.NONE
        );
        break;
      case "none-auto":
        serializer = CompressionFactory.getLongSerializer(
            encoding,
            writeOutMedium,
            "none-auto",
            ByteOrder.LITTLE_ENDIAN,
            CompressionFactory.LongEncodingStrategy.AUTO,
            CompressionStrategy.NONE
        );
        break;
      case "zstd-longs":
        serializer = CompressionFactory.getLongSerializer(
                encoding,
                writeOutMedium,
                "zstd-longs",
                ByteOrder.LITTLE_ENDIAN,
                CompressionFactory.LongEncodingStrategy.LONGS,
                CompressionStrategy.ZSTD
        );
        break;
      case "zstd-auto":
        serializer = CompressionFactory.getLongSerializer(
                encoding,
                writeOutMedium,
                "zstd-auto",
                ByteOrder.LITTLE_ENDIAN,
                CompressionFactory.LongEncodingStrategy.AUTO,
                CompressionStrategy.ZSTD
        );
        break;
      default:
        throw new RuntimeException("unknown encoding");
    }

    serializer.open();
    for (long val : vals) {
      serializer.add(val);
    }
    serializer.writeTo(output, null);
    return (int) serializer.getSerializedSize();
  }

  static ColumnarLongs createColumnarLongs(String encoding, ByteBuffer buffer)
  {
    switch (encoding) {
      case "lz4-longs":
      case "lz4-auto":
      case "none-auto":
      case "none-longs":
      case "zstd-auto":
      case "zstd-longs":
        return CompressedColumnarLongsSupplier.fromByteBuffer(buffer, ByteOrder.LITTLE_ENDIAN).get();
    }

    throw new IllegalArgumentException("unknown encoding");
  }


  // for testing encodings: validate that all encoders read the same values
  // noinspection unused
  static void checkSanity(Map<String, ColumnarLongs> encoders, List<String> encodings, int rows)
  {
    for (int i = 0; i < rows; i++) {
      checkRowSanity(encoders, encodings, i);
    }
  }

  static void checkRowSanity(Map<String, ColumnarLongs> encoders, List<String> encodings, int row)
  {
    if (encodings.size() > 1) {
      for (int i = 0; i < encodings.size() - 1; i++) {
        String currentKey = encodings.get(i);
        String nextKey = encodings.get(i + 1);
        ColumnarLongs current = encoders.get(currentKey);
        ColumnarLongs next = encoders.get(nextKey);
        long vCurrent = current.get(row);
        long vNext = next.get(row);
        if (vCurrent != vNext) {
          throw new RE(
              "values do not match at row %s - %s:%s %s:%s",
              row,
              currentKey,
              vCurrent,
              nextKey,
              vNext
          );
        }
      }
    }
  }
}
