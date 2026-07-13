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
import org.apache.druid.query.aggregation.mean.DoubleMeanHolder;
import org.apache.druid.query.aggregation.mean.DoubleMeanVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.IntPredicate;

public class SimdDoubleMeanVectorAggregatorTest extends InitializedNullHandlingTest
{
  private static final int[] VECTOR_SIZES = {1, 8, 17, 64, 1023};
  private static final double POISON_DOUBLE = 1e15;
  private static final double DELTA = 1e-9;

  @Test
  public void testDoubleMean()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern pattern : NullPattern.values()) {
        runDoubleMean(size, pattern, 0, 0);
        runDoubleMean(size, pattern, 1, 1);
      }
    }
  }

  private static void runDoubleMean(
      final int size,
      final NullPattern pattern,
      final int position,
      final int startRow
  )
  {
    final int arrLen = startRow + size;
    final double[] values = new double[arrLen];
    for (int i = 0; i < startRow; i++) {
      values[i] = POISON_DOUBLE;
    }
    System.arraycopy(randomDoubles(size), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, values, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "size[%s] nulls[%s] pos[%s] start[%s]",
        size,
        pattern,
        position,
        startRow
    );

    final DoubleMeanVectorAggregator scalar = new DoubleMeanVectorAggregator(selector);
    final SimdDoubleMeanVectorAggregator simd = new SimdDoubleMeanVectorAggregator(selector);
    final ByteBuffer scalarBuf = ByteBuffer.allocate(position + DoubleMeanHolder.MAX_INTERMEDIATE_SIZE);
    final ByteBuffer simdBuf = ByteBuffer.allocate(position + DoubleMeanHolder.MAX_INTERMEDIATE_SIZE);
    scalar.init(scalarBuf, position);
    simd.init(simdBuf, position);

    scalar.aggregate(scalarBuf, position, startRow, endRow);
    simd.aggregate(simdBuf, position, startRow, endRow);
    assertMeanHolderEquals(msg, scalarBuf, simdBuf, position);

    if (nulls != null) {
      final ByteBuffer directBuf = ByteBuffer.allocate(position + DoubleMeanHolder.MAX_INTERMEDIATE_SIZE);
      simd.init(directBuf, position);
      final boolean reported = simd.aggregate(directBuf, position, startRow, endRow, nulls);
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull(nulls, startRow, endRow), reported);
      assertMeanHolderEquals(msg + " (direct null-aware)", scalarBuf, directBuf, position);
    }
  }

  private static void assertMeanHolderEquals(
      final String msg,
      final ByteBuffer expectedBuf,
      final ByteBuffer actualBuf,
      final int position
  )
  {
    final DoubleMeanHolder expected = DoubleMeanHolder.get(expectedBuf, position);
    final DoubleMeanHolder actual = DoubleMeanHolder.get(actualBuf, position);
    Assert.assertEquals(msg + " (count)", count(expected), count(actual));
    Assert.assertEquals(
        msg + " (sum)",
        sum(expected),
        sum(actual),
        Math.max(Math.abs(sum(expected)) * 1e-12, DELTA)
    );
    Assert.assertEquals(
        msg + " (mean)",
        expected.mean(),
        actual.mean(),
        Math.max(Math.abs(expected.mean()) * 1e-12, DELTA)
    );
  }

  private static double sum(final DoubleMeanHolder holder)
  {
    return ByteBuffer.wrap(holder.toBytes()).getDouble(0);
  }

  private static long count(final DoubleMeanHolder holder)
  {
    return ByteBuffer.wrap(holder.toBytes()).getLong(Double.BYTES);
  }

  private static boolean anyNonNull(final boolean[] nulls, final int startRow, final int endRow)
  {
    for (int i = startRow; i < endRow; i++) {
      if (!nulls[i]) {
        return true;
      }
    }
    return false;
  }

  private static boolean[] padNulls(final boolean[] realNulls, final int startRow)
  {
    final boolean[] padded = new boolean[startRow + realNulls.length];
    System.arraycopy(realNulls, 0, padded, startRow, realNulls.length);
    return padded;
  }

  private static double[] randomDoubles(final int size)
  {
    final Random r = new Random(0xC0FFEEL);
    final double[] out = new double[size];
    for (int i = 0; i < size; i++) {
      out[i] = (r.nextDouble() - 0.5) * 1000.0;
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

    NullPattern(final IntPredicate predicate)
    {
      this.predicate = predicate;
    }

    @Nullable
    boolean[] toMask(final int size)
    {
      if (this == NONE) {
        return null;
      }
      final boolean[] mask = new boolean[size];
      for (int i = 0; i < size; i++) {
        mask[i] = predicate.test(i);
      }
      return mask;
    }
  }

  private static final class FakeVectorValueSelector implements VectorValueSelector
  {
    private final int size;
    private final double[] doubles;
    @Nullable
    private final boolean[] nulls;

    FakeVectorValueSelector(final int size, final double[] doubles, @Nullable final boolean[] nulls)
    {
      this.size = size;
      this.doubles = doubles;
      this.nulls = nulls;
    }

    @Override
    public long[] getLongVector()
    {
      return null;
    }

    @Override
    public float[] getFloatVector()
    {
      return null;
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
