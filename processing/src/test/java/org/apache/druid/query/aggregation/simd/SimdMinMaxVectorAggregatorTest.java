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
import org.apache.druid.query.aggregation.DoubleMaxVectorAggregator;
import org.apache.druid.query.aggregation.DoubleMinVectorAggregator;
import org.apache.druid.query.aggregation.FloatMaxVectorAggregator;
import org.apache.druid.query.aggregation.FloatMinVectorAggregator;
import org.apache.druid.query.aggregation.LongMaxVectorAggregator;
import org.apache.druid.query.aggregation.LongMinVectorAggregator;
import org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.FakeVectorValueSelector;
import org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.NullPattern;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.VECTOR_SIZES;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.padNulls;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.randomDoubles;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.randomFloats;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.randomLongs;

/**
 * Equivalence tests for SIMD Min/Max vector aggregators. For each (op, type, vector size, null pattern) tuple,
 * drives the SIMD aggregator directly and compares against either the scalar parent (no-null path) or a manually
 * computed reference (null-aware path). When every row is null the null-aware path must report false and leave
 * the buffer's seeded identity value untouched.
 *
 * Each scenario is exercised twice: once with {@code (position=0, startRow=0, endRow=size)} and once with
 * {@code (position=1, startRow=1, endRow=size+1)} where the row at index 0 holds a deliberately op-defeating
 * "poison" value (an extreme low for min, extreme high for max) that would visibly change the result if the
 * aggregator incorrectly read past {@code startRow}, and the buffer slot starts at byte offset 1 so any
 * indexing off the position parameter shows up.
 */
public class SimdMinMaxVectorAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void testLongMin()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runLong(size, p, true, 0, 0);
        runLong(size, p, true, 1, 1);
      }
    }
  }

  @Test
  public void testLongMax()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runLong(size, p, false, 0, 0);
        runLong(size, p, false, 1, 1);
      }
    }
  }

  @Test
  public void testDoubleMin()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runDouble(size, p, true, 0, 0);
        runDouble(size, p, true, 1, 1);
      }
    }
  }

  @Test
  public void testDoubleMax()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runDouble(size, p, false, 0, 0);
        runDouble(size, p, false, 1, 1);
      }
    }
  }

  @Test
  public void testFloatMin()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runFloat(size, p, true, 0, 0);
        runFloat(size, p, true, 1, 1);
      }
    }
  }

  @Test
  public void testFloatMax()
  {
    for (int size : VECTOR_SIZES) {
      for (NullPattern p : NullPattern.values()) {
        runFloat(size, p, false, 0, 0);
        runFloat(size, p, false, 1, 1);
      }
    }
  }

  private static void runLong(int size, NullPattern pattern, boolean isMin, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final long[] values = new long[arrLen];
    final long poison = isMin ? Long.MIN_VALUE : Long.MAX_VALUE;
    for (int i = 0; i < startRow; i++) {
      values[i] = poison;
    }
    System.arraycopy(randomLongs(size, isMin ? 0 : 1), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, values, null, null, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[long] op[%s] size[%s] nulls[%s] pos[%s] start[%s]",
        isMin ? "min" : "max", size, pattern, position, startRow
    );
    final long identity = isMin ? Long.MAX_VALUE : Long.MIN_VALUE;

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Long.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Long.BYTES);
      if (isMin) {
        final LongMinVectorAggregator scalar = new LongMinVectorAggregator(selector);
        final SimdLongMinVectorAggregator simd = new SimdLongMinVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      } else {
        final LongMaxVectorAggregator scalar = new LongMaxVectorAggregator(selector);
        final SimdLongMaxVectorAggregator simd = new SimdLongMaxVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      }
      Assert.assertEquals(msg, scalarBuf.getLong(position), simdBuf.getLong(position));
    } else {
      long expected = identity;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected = isMin ? Math.min(expected, values[i]) : Math.max(expected, values[i]);
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Long.BYTES);
      final boolean reported;
      if (isMin) {
        final SimdLongMinVectorAggregator simd = new SimdLongMinVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      } else {
        final SimdLongMaxVectorAggregator simd = new SimdLongMaxVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      }
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      Assert.assertEquals(msg, expected, simdBuf.getLong(position));
    }
  }

  private static void runDouble(int size, NullPattern pattern, boolean isMin, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final double[] values = new double[arrLen];
    final double poison = isMin ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    for (int i = 0; i < startRow; i++) {
      values[i] = poison;
    }
    System.arraycopy(randomDoubles(size, isMin ? 2 : 3), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, null, values, null, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[double] op[%s] size[%s] nulls[%s] pos[%s] start[%s]",
        isMin ? "min" : "max", size, pattern, position, startRow
    );
    final double identity = isMin ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Double.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Double.BYTES);
      if (isMin) {
        final DoubleMinVectorAggregator scalar = new DoubleMinVectorAggregator(selector);
        final SimdDoubleMinVectorAggregator simd = new SimdDoubleMinVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      } else {
        final DoubleMaxVectorAggregator scalar = new DoubleMaxVectorAggregator(selector);
        final SimdDoubleMaxVectorAggregator simd = new SimdDoubleMaxVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      }
      // min/max produces a value that was present in the input -- exact equality is fine.
      Assert.assertEquals(msg, scalarBuf.getDouble(position), simdBuf.getDouble(position), 0.0);
    } else {
      double expected = identity;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected = isMin ? Math.min(expected, values[i]) : Math.max(expected, values[i]);
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Double.BYTES);
      final boolean reported;
      if (isMin) {
        final SimdDoubleMinVectorAggregator simd = new SimdDoubleMinVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      } else {
        final SimdDoubleMaxVectorAggregator simd = new SimdDoubleMaxVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      }
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      Assert.assertEquals(msg, expected, simdBuf.getDouble(position), 0.0);
    }
  }

  private static void runFloat(int size, NullPattern pattern, boolean isMin, int position, int startRow)
  {
    final int arrLen = startRow + size;
    final float[] values = new float[arrLen];
    final float poison = isMin ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
    for (int i = 0; i < startRow; i++) {
      values[i] = poison;
    }
    System.arraycopy(randomFloats(size, isMin ? 4 : 5), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, null, null, values, nulls);
    final int endRow = startRow + size;
    final String msg = StringUtils.format(
        "type[float] op[%s] size[%s] nulls[%s] pos[%s] start[%s]",
        isMin ? "min" : "max", size, pattern, position, startRow
    );
    final float identity = isMin ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;

    if (nulls == null) {
      final ByteBuffer scalarBuf = ByteBuffer.allocate(position + Float.BYTES);
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Float.BYTES);
      if (isMin) {
        final FloatMinVectorAggregator scalar = new FloatMinVectorAggregator(selector);
        final SimdFloatMinVectorAggregator simd = new SimdFloatMinVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      } else {
        final FloatMaxVectorAggregator scalar = new FloatMaxVectorAggregator(selector);
        final SimdFloatMaxVectorAggregator simd = new SimdFloatMaxVectorAggregator(selector);
        scalar.init(scalarBuf, position);
        simd.init(simdBuf, position);
        scalar.aggregate(scalarBuf, position, startRow, endRow);
        simd.aggregate(simdBuf, position, startRow, endRow);
      }
      Assert.assertEquals(msg, scalarBuf.getFloat(position), simdBuf.getFloat(position), 0.0f);
    } else {
      float expected = identity;
      boolean anyNonNull = false;
      for (int i = startRow; i < endRow; i++) {
        if (!nulls[i]) {
          expected = isMin ? Math.min(expected, values[i]) : Math.max(expected, values[i]);
          anyNonNull = true;
        }
      }
      final ByteBuffer simdBuf = ByteBuffer.allocate(position + Float.BYTES);
      final boolean reported;
      if (isMin) {
        final SimdFloatMinVectorAggregator simd = new SimdFloatMinVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      } else {
        final SimdFloatMaxVectorAggregator simd = new SimdFloatMaxVectorAggregator(selector);
        simd.init(simdBuf, position);
        reported = simd.aggregate(simdBuf, position, startRow, endRow, nulls);
      }
      Assert.assertEquals(msg + " (anyNonNull)", anyNonNull, reported);
      Assert.assertEquals(msg, expected, simdBuf.getFloat(position), 0.0f);
    }
  }

}
