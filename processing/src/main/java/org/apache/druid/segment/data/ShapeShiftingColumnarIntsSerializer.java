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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.codecs.ints.BytePackedIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.CompressedIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.CompressibleIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.ConstantIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.IntCodecs;
import org.apache.druid.segment.data.codecs.ints.IntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.IntFormMetrics;
import org.apache.druid.segment.data.codecs.ints.LemireIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.RunLengthBytePackedIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.UnencodedIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.ZeroIntFormEncoder;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * {@link ShapeShiftingColumnSerializer} implementation for {@link ShapeShiftingColumnarInts}, using
 * {@link IntFormEncoder} to encode values and {@link IntFormMetrics} to analyze them and assist with decisions of how
 * the value chunks will be encoded when 'flushed' to the {@link SegmentWriteOutMedium}.
 */
public class ShapeShiftingColumnarIntsSerializer
    extends ShapeShiftingColumnSerializer<int[], IntFormMetrics>
    implements SingleValueColumnarIntsSerializer
{
  private ResourceHolder<int[]> unencodedValuesHolder;

  private final boolean enableEncoderOptOut;

  public static IntFormEncoder[] getDefaultIntFormEncoders(
      IndexSpec.ShapeShiftBlockSize blockSize,
      CompressionStrategy compressionStrategy,
      Closer closer,
      ByteOrder byteOrder
  )
  {
    byte intBlockSize = (byte) (blockSize.getLogBlockSize() - 2);

    ByteBuffer uncompressedDataBuffer =
        compressionStrategy.getCompressor()
                               .allocateInBuffer(8 + ((1 << intBlockSize) * Integer.BYTES), closer)
                               .order(byteOrder);
    ByteBuffer compressedDataBuffer =
        compressionStrategy.getCompressor()
                               .allocateOutBuffer(((1 << intBlockSize) * Integer.BYTES) + 1024, closer);

    final CompressibleIntFormEncoder rle = new RunLengthBytePackedIntFormEncoder(
        intBlockSize,
        byteOrder
    );
    final CompressibleIntFormEncoder bytepack = new BytePackedIntFormEncoder(intBlockSize, byteOrder);

    final IntFormEncoder[] defaultCodecs = new IntFormEncoder[]{
        new ZeroIntFormEncoder(intBlockSize, byteOrder),
        new ConstantIntFormEncoder(intBlockSize, byteOrder),
        new UnencodedIntFormEncoder(intBlockSize, byteOrder),
        rle,
        bytepack,
        new CompressedIntFormEncoder(
            intBlockSize,
            byteOrder,
            compressionStrategy,
            rle,
            uncompressedDataBuffer,
            compressedDataBuffer
        ),
        new CompressedIntFormEncoder(
            intBlockSize,
            byteOrder,
            compressionStrategy,
            bytepack,
            uncompressedDataBuffer,
            compressedDataBuffer
        ),
        new LemireIntFormEncoder(intBlockSize, IntCodecs.FASTPFOR, "fastpfor", byteOrder)
    };

    return defaultCodecs;
  }

  public ShapeShiftingColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final IntFormEncoder[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftBlockSize blockSize,
      @Nullable final ByteOrder overrideByteOrder
  )
  {
    this(
        segmentWriteOutMedium,
        codecs,
        optimizationTarget,
        blockSize,
        overrideByteOrder,
        null
    );
  }

  @VisibleForTesting
  public ShapeShiftingColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final IntFormEncoder[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftBlockSize blockSize,
      @Nullable final ByteOrder overrideByteOrder,
      @Nullable final Byte overrideLogValuesPerChunk
  )
  {
    super(
        segmentWriteOutMedium,
        codecs,
        optimizationTarget,
        blockSize,
        2,
        ShapeShiftingColumnarInts.VERSION,
        overrideByteOrder,
        overrideLogValuesPerChunk
    );

    Closer closer = segmentWriteOutMedium.getCloser();
    if (closer != null) {
      closer.register(() -> {
        if (unencodedValuesHolder != null) {
          unencodedValuesHolder.close();
        }
      });
    }

    // enable optimization of encoders such as rle if there are additional non-zero and non-constant encoders.
    this.enableEncoderOptOut =
        Arrays.stream(codecs)
              .filter(e -> !(e instanceof ZeroIntFormEncoder) && !(e instanceof ConstantIntFormEncoder))
              .count() > 1;
  }

  @Override
  public void initializeChunk()
  {
    unencodedValuesHolder = CompressedPools.getShapeshiftIntsDecodedValuesArray(logValuesPerChunk);
    currentChunk = unencodedValuesHolder.get();
  }

  @Override
  public void resetChunkCollector()
  {
    chunkMetrics = new IntFormMetrics(optimizationTarget, this.enableEncoderOptOut);
  }

  /**
   * Adds a value to the current chunk of ints, stored in an array, analyzing values with {@link IntFormMetrics}, and
   * flushing to the {@link SegmentWriteOutMedium} if the current chunk is full.
   *
   * @param val
   *
   * @throws IOException
   */
  @Override
  public void addValue(int val) throws IOException
  {
    if (currentChunkPos == valuesPerChunk) {
      flushCurrentChunk();
    }

    chunkMetrics.processNextRow(val);

    currentChunk[currentChunkPos++] = val;
    numValues++;
  }
}
