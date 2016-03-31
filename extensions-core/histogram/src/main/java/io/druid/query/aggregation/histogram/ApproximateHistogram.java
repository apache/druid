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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ApproximateHistogram extends ApproximateHistogramHolder
{
  public ApproximateHistogram(
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

  public ApproximateHistogram()
  {
    this(DEFAULT_HISTOGRAM_SIZE);
  }

  public ApproximateHistogram(int size)
  {
    super( size);
  }

  public ApproximateHistogram(int size, float lowerLimit, float upperLimit)
  {
    super(size, lowerLimit, upperLimit);
  }

  public ApproximateHistogram(int binCount, float[] positions, long[] bins, float min, float max)
  {
    super(binCount, positions, bins, min, max);
  }

  public ApproximateHistogram(int size, int binCount, float[] positions, long[] bins, float min, float max)
  {
    super(size, binCount, positions, bins, min, max);
  }

  @Override
  @JsonValue
  public byte[] toBytes()
  {
    ByteBuffer buf = ByteBuffer.allocate(getMinStorageSize());
    toBytes(buf);
    return buf.array();
  }

  public int getCompactStorageSize(final long exactCount)
  {

    // ensures exactCount and (count - exactCount) can safely be cast to (int)
    Preconditions.checkState(canStoreCompact(exactCount), "Approximate histogram cannot be stored in compact form");

    if (exactCount == count) {
      return Shorts.BYTES + 1 + Floats.BYTES * (int) exactCount;
    } else {
      return Shorts.BYTES
             + 1
             + Floats.BYTES * (int) exactCount
             + 1
             + Floats.BYTES * (int) (count - exactCount)
             + Floats.BYTES * 2;
    }
  }

  /**
   * Returns the minimum number of bytes required to store this ApproximateHistogram object
   *
   * @return required number of bytes
   */
  public int getMinStorageSize()
  {
    final long exactCount = getExactCount();
    // sparse is always small than dense, so no need to check
    if (canStoreCompact(exactCount) && getCompactStorageSize(exactCount) < getSparseStorageSize()) {
      return getCompactStorageSize(exactCount);
    } else {
      return getSparseStorageSize();
    }
  }

  /**
   * Checks whether this approximate histogram can be stored in a compact form
   *
   * @return true if yes, false otherwise
   */
  private boolean canStoreCompact(final long exactCount)
  {
    return (
        size <= Short.MAX_VALUE
        && exactCount <= Byte.MAX_VALUE
        && (count - exactCount) <= Byte.MAX_VALUE
    );
  }

  /**
   * Writes the representation of this ApproximateHistogram object to the given byte-buffer
   *
   * @param buf ByteBuffer to write the ApproximateHistogram to
   */
  private void toBytes(ByteBuffer buf)
  {
    final long exactCount = getExactCount();
    if (canStoreCompact(exactCount) && getCompactStorageSize(exactCount) < getSparseStorageSize()) {
      // store compact
      toBytesCompact(buf);
    } else {
      // store sparse
      toBytesSparse(buf);
    }
  }

  /**
   * Writes the dense representation of this ApproximateHistogram object to the given byte-buffer
   *
   * Requires 16 + 12 * size bytes of storage
   *
   * @param buf ByteBuffer to write the ApproximateHistogram to
   */
  void toBytesDense(ByteBuffer buf)
  {
    buf.putInt(size);
    buf.putInt(binCount);

    buf.asFloatBuffer().put(positions);
    buf.position(buf.position() + Floats.BYTES * positions.length);
    buf.asLongBuffer().put(bins);
    buf.position(buf.position() + Longs.BYTES * bins.length);

    buf.putFloat(min);
    buf.putFloat(max);
  }

  /**
   * Writes the sparse representation of this ApproximateHistogram object to the given byte-buffer
   *
   * Requires 16 + 12 * binCount bytes of storage
   *
   * @param buf ByteBuffer to write the ApproximateHistogram to
   */
  @VisibleForTesting
  void toBytesSparse(ByteBuffer buf)
  {
    buf.putInt(size);
    buf.putInt(-1 * binCount); // use negative binCount to indicate sparse storage
    for (int i = 0; i < binCount; ++i) {
      buf.putFloat(positions[i]);
    }
    for (int i = 0; i < binCount; ++i) {
      buf.putLong(bins[i]);
    }
    buf.putFloat(min);
    buf.putFloat(max);
  }

  /**
   * Returns a compact byte-buffer representation of this ApproximateHistogram object
   * storing actual values as opposed to histogram bins
   *
   * Requires 3 + 4 * count bytes of storage with count &lt;= 127
   *
   * @param buf ByteBuffer to write the ApproximateHistogram to
   */
  @VisibleForTesting
  void toBytesCompact(ByteBuffer buf)
  {
    final long exactCount = getExactCount();
    Preconditions.checkState(canStoreCompact(exactCount), "Approximate histogram cannot be stored in compact form");

    buf.putShort((short) (-1 * size)); // use negative size to indicate compact storage

    if (exactCount != count) {
      // use negative count to indicate approximate bins
      buf.put((byte) (-1 * (count - exactCount)));

      // store actual values instead of bins
      for (int i = 0; i < binCount; ++i) {
        // repeat each value bins[i] times for approximate bins
        if ((bins[i] & APPROX_FLAG_BIT) != 0) {
          for (int k = 0; k < (bins[i] & COUNT_BITS); ++k) {
            buf.putFloat(positions[i]);
          }
        }
      }

      // tack on min and max since they may be lost int the approximate bins
      buf.putFloat(min);
      buf.putFloat(max);
    }

    buf.put((byte) exactCount);
    // store actual values instead of bins
    for (int i = 0; i < binCount; ++i) {
      // repeat each value bins[i] times for exact bins
      if ((bins[i] & APPROX_FLAG_BIT) == 0) {
        for (int k = 0; k < (bins[i] & COUNT_BITS); ++k) {
          buf.putFloat(positions[i]);
        }
      }
    }
  }

  /**
   * Constructs an ApproximateHistogram object from the given dense byte-buffer representation
   *
   * @param buf ByteBuffer to construct an ApproximateHistogram from
   *
   * @return ApproximateHistogram constructed from the given ByteBuffer
   */
  @VisibleForTesting
  ApproximateHistogram fromBytesDense(ByteBuffer buf)
  {
    int size = buf.getInt();
    int binCount = buf.getInt();

    float[] positions = new float[size];
    long[] bins = new long[size];

    buf.asFloatBuffer().get(positions);
    buf.position(buf.position() + Floats.BYTES * positions.length);
    buf.asLongBuffer().get(bins);
    buf.position(buf.position() + Longs.BYTES * bins.length);

    float min = buf.getFloat();
    float max = buf.getFloat();

    this.size = size;
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

  /**
   * Constructs an ApproximateHistogram object from the given dense byte-buffer representation
   *
   * @param buf ByteBuffer to construct an ApproximateHistogram from
   *
   * @return ApproximateHistogram constructed from the given ByteBuffer
   */
  @VisibleForTesting
  ApproximateHistogram fromBytesSparse(ByteBuffer buf)
  {
    int size = buf.getInt();
    int binCount = -1 * buf.getInt();

    float[] positions = new float[size];
    long[] bins = new long[size];

    for (int i = 0; i < binCount; ++i) {
      positions[i] = buf.getFloat();
    }
    for (int i = 0; i < binCount; ++i) {
      bins[i] = buf.getLong();
    }

    float min = buf.getFloat();
    float max = buf.getFloat();

    this.size = size;
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

  /**
   * Constructs an ApproximateHistogram object from the given compact byte-buffer representation
   *
   * @param buf ByteBuffer to construct an ApproximateHistogram from
   *
   * @return ApproximateHistogram constructed from the given ByteBuffer
   */
  @VisibleForTesting
  ApproximateHistogram fromBytesCompact(ByteBuffer buf)
  {
    short size = (short) (-1 * buf.getShort());
    byte count = buf.get();

    if (count >= 0) {
      // only exact bins
      reset(size);
      for (int i = 0; i < count; ++i) {
        offer(buf.getFloat());
      }
      return this;
    } else {
      byte approxCount = (byte) (-1 * count);

      Map<Float, Long> approx = Maps.newHashMap();

      for (int i = 0; i < approxCount; ++i) {
        final float value = buf.getFloat();
        if (approx.containsKey(value)) {
          approx.put(value, approx.get(value) + 1);
        } else {
          approx.put(value, 1L);
        }
      }

      float min = buf.getFloat();
      float max = buf.getFloat();

      byte exactCount = buf.get();

      Map<Float, Long> exact = Maps.newHashMap();

      for (int i = 0; i < exactCount; ++i) {
        final float value = buf.getFloat();
        if (exact.containsKey(value)) {
          exact.put(value, exact.get(value) + 1);
        } else {
          exact.put(value, 1L);
        }
      }

      int binCount = exact.size() + approx.size();

      List<Float> pos = Lists.newArrayList();
      pos.addAll(exact.keySet());
      pos.addAll(approx.keySet());
      Collections.sort(pos);

      float[] positions = new float[size];
      long[] bins = new long[size];

      for (int i = 0; i < pos.size(); ++i) {
        positions[i] = pos.get(i);
      }

      for (int i = 0; i < pos.size(); ++i) {
        final float value = pos.get(i);
        if (exact.containsKey(value)) {
          bins[i] = exact.get(value);
        } else {
          bins[i] = approx.get(value) | APPROX_FLAG_BIT;
        }
      }

      this.size = size;
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

  @Override
  public ApproximateHistogramHolder fromBytes(ByteBuffer buf)
  {
    ByteBuffer copy = buf.asReadOnlyBuffer();
    // negative size indicates compact representation
    // this works regardless of whether we use int or short for the size since the leftmost bit is the sign bit
    if (copy.getShort(buf.position()) < 0) {
      return fromBytesCompact(buf);
    } else {
      // ignore size
      copy.getInt();
      // determine if sparse or dense based on sign of binCount
      if (copy.getInt() < 0) {
        return fromBytesSparse(buf);
      } else {
        return fromBytesDense(buf);
      }
    }
  }
}
