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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.segment.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class ApproximateCompactHistogram extends ApproximateHistogramHolder
{
  private static final long INVERTED_FLAG_BIT = 1L;

  private static final byte[] MASKS = new byte[]{
      0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80
  };

  public ApproximateCompactHistogram(
      int size,
      float[] positions,
      long[] bins,
      int binCount,
      float min,
      float max,
      long count,
      float lowerLimit,
      float upperLimit
  )
  {
    super(size, positions, bins, binCount, min, max, count, lowerLimit, upperLimit);
  }

  public ApproximateCompactHistogram()
  {
    this(DEFAULT_HISTOGRAM_SIZE);
  }

  public ApproximateCompactHistogram(int size)
  {
    super(size);
  }

  public ApproximateCompactHistogram(int size, float lowerLimit, float upperLimit)
  {
    super(size, lowerLimit, upperLimit);
  }

  public ApproximateCompactHistogram(int binCount, float[] positions, long[] bins, float min, float max)
  {
    super(binCount, positions, bins, min, max);
  }

  public ApproximateCompactHistogram(int size, int binCount, float[] positions, long[] bins, float min, float max)
  {
    super(size, binCount, positions, bins, min, max);
  }

  @Override
  @JsonValue
  public byte[] toBytes()
  {
    ByteBuffer buf = ByteBuffer.allocate(getMaxStorageSize());
    toBytes(buf);
    return Arrays.copyOfRange(buf.array(), 0, buf.position());
  }

  private void toBytes(ByteBuffer buf)
  {
    final long exactCount = getExactCount();
    final boolean exact = exactCount == count;

    ByteBuffers.writeVLong(buf, (size << 1) + (exact ? 0 : 1));
    ByteBuffers.writeVLong(buf, binCount);

    for (int x = 0; x < binCount; ) {
      byte maker = 0;
      final int limit = Math.min(binCount, x + 8);
      for (int i = x; i < limit; i++) {
        buf.putFloat(positions[i]);
        if (bins[i] > 1) {
          maker |= MASKS[x % 8];
        }
      }
      buf.put(maker);
      for (int i = x; i < limit; i++) {
        if (bins[i] > 1) {
          boolean approximate = (bins[i] & APPROX_FLAG_BIT) != 0;
          ByteBuffers.writeVLong(buf, (bins[i] << 1) + (approximate ? 1 : 0));
        }
      }
      x = limit;
    }

    if (!exact) {
      buf.putFloat(min);
      buf.putFloat(max);
    }
  }

  @Override
  public ApproximateCompactHistogram fromBytes(ByteBuffer buf)
  {
    int size = ByteBuffers.readVInt(buf);
    int binCount = ByteBuffers.readVInt(buf);
    if (binCount == 0) {
      reset(size >> 1);
      return this;
    }
    final boolean exact = (size & INVERTED_FLAG_BIT) == 0;
    float[] positions = new float[binCount];
    long[] bins = new long[binCount];
    for (int x = 0; x < binCount; ) {
      final int limit = Math.min(binCount, x + 8);
      for (int i = x; i < limit; i++) {
        positions[i] = buf.getFloat();
      }
      final byte mask = buf.get();
      for (int i = x; i < limit; i++) {
        if ((mask & MASKS[i % 8]) == 0) {
          bins[i] = 1;
          continue;
        }
        long value = ByteBuffers.readVLong(buf);
        boolean approximate = (value & INVERTED_FLAG_BIT) != 0;
        value >>= 1;
        if (approximate) {
          bins[i] = value | APPROX_FLAG_BIT;
        } else {
          bins[i] = value;
        }
      }
      x = limit;
    }

    float min, max;
    if (exact) {
      min = positions[0];
      max = positions[positions.length - 1];
    } else {
      min = buf.getFloat();
      max = buf.getFloat();
    }

    this.size = size >> 1;
    this.binCount = binCount;
    this.positions = positions;
    this.bins = bins;
    this.min = min;
    this.max = max;
    this.count = sumBins(bins, binCount);
    this.lowerLimit = Float.NEGATIVE_INFINITY;
    this.upperLimit = Float.POSITIVE_INFINITY;

    return this;
  }
}
