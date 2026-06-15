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

package org.apache.druid.query.aggregation.simd;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.DoubleSumVectorAggregator;
import org.apache.druid.query.aggregation.FloatSumVectorAggregator;
import org.apache.druid.query.aggregation.LongSumVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.IntPredicate;

/**
 * For each (sum type, vector size, null pattern) combination, drives the SIMD and scalar sum vector aggregators
 * directly and asserts equivalent buffer state. Two paths covered:
 *   - the ungrouped no-null path (SIMD subclass's overridden {@code aggregate(buf, pos, start, end)} vs the scalar
 *     parent's implementation), and
 *   - the null-aware path on the SIMD class (the new {@code aggregate(buf, pos, start, end, nullVector)} declared by
 *     {@link org.apache.druid.query.aggregation.NullAwareVectorAggregator}), compared against a manually-computed
 *     reference sum.
 *
 * Each scenario is exercised twice: once with {@code (position=0, startRow=0, endRow=size)} and once with
 * {@code (position=1, startRow=1, endRow=size+1)} where the row at index 0 is a deliberately extreme "poison"
 * value that would visibly skew the result if the aggregator incorrectly read past {@code startRow}, and the
 * buffer slot starts at byte offset 1 so any indexing off the position parameter shows up.
 */
public class SimdSumVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int[] VECTOR_SIZES = {1, 8, 17, 64, 1023};
  private static final long POISON_LONG = Long.MAX_VALUE / 2;
  private static final double POISON_DOUBLE = 1e15;
  private static final float POISON_FLOAT = 1e10f;

  @Test
  public void testLongSum()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern pattern : NullPattern.values()) {
        runLong(size, pattern, 0, 0);
        runLong(size, pattern, 1, 1);
      }
    }
  }

  @Test
  public void testDoubleSum()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern pattern : NullPattern.values()) {
        runDouble(size, pattern, 0, 0);
        runDouble(size, pattern, 1, 1);
      }
    }
  }

  @Test
  public void testFloatSum()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern pattern : NullPattern.values()) {
        runFloat(size, pattern, 0, 0);
        runFloat(size, pattern, 1, 1);
      }
    }
  }

  private static void runLong(int size, NullPattern pattern, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final long[] values = new long[arrLen];
    for (int i = 0; i < startRow; i++) {
      values[i] = POISON_LONG;
    }
    System.arraycopy(randomLongs(size, 0), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, values, null, null, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[long] size[%s] nulls[%s] pos[%s] start[%s]",
        size, pattern, position, startRow
    );

    final LongSumVectorAggregator scalar = new LongSumVectorAggregator(selector);
    final SimdLongSumVectorAggregator simd = new SimdLongSumVectorAggregator(selector);

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Long.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Long.BYTES);
      scalar.init(scalarBuf, position);
      simd.init(simdBuf, position);
      scalar.aggregate(scalarBuf, position, startRow, endRow);
      simd.aggregate(simdBuf, position, startRow, endRow);
      Assert.assertEquals(msg, scalarBuf.getLong(position), simdBuf.getLong(position));
    } else {
      long expected = 0;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected += values[i];
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Long.BYTES);
      simd.init(simdBuf, position);
      final boolean reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      if (reported) {
        Assert.assertEquals(msg, expected, simdBuf.getLong(position));
      }
    }
  }

  private static void runDouble(int size, NullPattern pattern, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final double[] values = new double[arrLen];
    for (int i = 0; i < startRow; i++) {
      values[i] = POISON_DOUBLE;
    }
    System.arraycopy(randomDoubles(size, 1), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, null, values, null, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[double] size[%s] nulls[%s] pos[%s] start[%s]",
        size, pattern, position, startRow
    );

    final DoubleSumVectorAggregator scalar = new DoubleSumVectorAggregator(selector);
    final SimdDoubleSumVectorAggregator simd = new SimdDoubleSumVectorAggregator(selector);

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Double.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Double.BYTES);
      scalar.init(scalarBuf, position);
      simd.init(simdBuf, position);
      scalar.aggregate(scalarBuf, position, startRow, endRow);
      simd.aggregate(simdBuf, position, startRow, endRow);
      Assert.assertEquals(
          msg,
          scalarBuf.getDouble(position),
          simdBuf.getDouble(position),
          Math.max(Math.abs(scalarBuf.getDouble(position)) * 1e-12, 1e-12)
      );
    } else {
      double expected = 0;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected += values[i];
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Double.BYTES);
      simd.init(simdBuf, position);
      final boolean reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      if (reported) {
        Assert.assertEquals(
            msg,
            expected,
            simdBuf.getDouble(position),
            Math.max(Math.abs(expected) * 1e-12, 1e-12)
        );
      }
    }
  }

  private static void runFloat(int size, NullPattern pattern, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final float[] values = new float[arrLen];
    for (int i = 0; i < startRow; i++) {
      values[i] = POISON_FLOAT;
    }
    System.arraycopy(randomFloats(size, 2), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, null, null, values, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[float] size[%s] nulls[%s] pos[%s] start[%s]",
        size, pattern, position, startRow
    );

    final FloatSumVectorAggregator scalar = new FloatSumVectorAggregator(selector);
    final SimdFloatSumVectorAggregator simd = new SimdFloatSumVectorAggregator(selector);

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Float.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Float.BYTES);
      scalar.init(scalarBuf, position);
      simd.init(simdBuf, position);
      scalar.aggregate(scalarBuf, position, startRow, endRow);
      simd.aggregate(simdBuf, position, startRow, endRow);
      Assert.assertEquals(
          msg,
          scalarBuf.getFloat(position),
          simdBuf.getFloat(position),
          Math.max(Math.abs(scalarBuf.getFloat(position)) * 1e-5f, 1e-5f)
      );
    } else {
      float expected = 0;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected += values[i];
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Float.BYTES);
      simd.init(simdBuf, position);
      final boolean reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      if (reported) {
        Assert.assertEquals(
            msg,
            expected,
            simdBuf.getFloat(position),
            Math.max(Math.abs(expected) * 1e-5f, 1e-5f)
        );
      }
    }
  }

  private static boolean[] padNulls(boolean[] realNulls, int startRow)
  {
    final boolean[] padded = new boolean[startRow + realNulls.length];
    System.arraycopy(realNulls, 0, padded, startRow, realNulls.length);
    return padded;
  }

  private static long[] randomLongs(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final long[] out = new long[size];
    for (int i = 0; i < size; i++) {
      out[i] = r.nextInt() & 0xFFFFFL;
    }
    return out;
  }

  private static double[] randomDoubles(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final double[] out = new double[size];
    for (int i = 0; i < size; i++) {
      out[i] = (r.nextDouble() - 0.5) * 1000.0;
    }
    return out;
  }

  private static float[] randomFloats(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final float[] out = new float[size];
    for (int i = 0; i < size; i++) {
      out[i] = (r.nextFloat() - 0.5f) * 1000.0f;
    }
    return out;
  }

  private enum NullPattern
  {
    NONE(i -> false),
    ALL(i -> true),
    ALTERNATING(i -> (i & 1) == 0),
    SPARSE(i -> i % 7 == 0),
    FIRST_THREE(i -> i < 3),
    CHUNK_BOUNDARY(i -> i == 7 || i == 8);

    private final IntPredicate predicate;

    NullPattern(IntPredicate predicate)
    {
      this.predicate = predicate;
    }

    @Nullable
    boolean[] toMask(int size)
    {
      if (this == NONE) {
        return null;        // models a column with no null vector at all
      }
      final boolean[] mask = new boolean[size];
      for (int i = 0; i < size; i++) {
        mask[i] = predicate.test(i);
      }
      return mask;
    }
  }

  /**
   * Minimal in-memory {@link VectorValueSelector} backed by pre-built primitive arrays for tests. Only the
   * accessor for the type used by a given test is non-null.
   */
  private static final class FakeVectorValueSelector implements VectorValueSelector
  {
    private final int size;
    @Nullable
    private final long[] longs;
    @Nullable
    private final double[] doubles;
    @Nullable
    private final float[] floats;
    @Nullable
    private final boolean[] nulls;

    FakeVectorValueSelector(
        int size,
        @Nullable long[] longs,
        @Nullable double[] doubles,
        @Nullable float[] floats,
        @Nullable boolean[] nulls
    )
    {
      this.size = size;
      this.longs = longs;
      this.doubles = doubles;
      this.floats = floats;
      this.nulls = nulls;
    }

    @Override
    public long[] getLongVector()
    {
      return longs;
    }

    @Override
    public float[] getFloatVector()
    {
      return floats;
    }

    @Override
    public double[] getDoubleVector()
    {
      return doubles;
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return nulls;
    }

    @Override
    public int getMaxVectorSize()
    {
      return size;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return size;
    }
  }
}
