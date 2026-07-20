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
import org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.FakeVectorValueSelector;
import org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.NullPattern;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.VECTOR_SIZES;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.padNulls;
import static org.apache.druid.query.aggregation.simd.SimdAggregatorTestHelpers.randomDoubles;

/**
 * Equivalence test for the SIMD doubleMean vector aggregator. For each (vector size, null pattern) tuple it drives
 * both the SIMD aggregator and the scalar parent over the same input and asserts the resulting mean holders match.
 * The SIMD aggregator handles nulls internally (it is not a {@code NullAwareVectorAggregator}), so the contiguous
 * {@code aggregate} entry point covers both the masked and unmasked loops.
 *
 * Each scenario is exercised twice: once with {@code (position=0, startRow=0)} and once with
 * {@code (position=1, startRow=1)} where the rows before {@code startRow} hold a large "poison" value that would
 * visibly skew the mean if the aggregator incorrectly read past {@code startRow}, and the buffer slot starts at byte
 * offset 1 so any indexing off the position parameter shows up.
 */
public class SimdDoubleMeanVectorAggregatorTest extends InitializedNullHandlingTest
{
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
    System.arraycopy(randomDoubles(size, 0), 0, values, startRow, size);

    final boolean[] realNulls = pattern.toMask(size);
    final boolean[] nulls = realNulls == null ? null : padNulls(realNulls, startRow);

    final FakeVectorValueSelector selector = new FakeVectorValueSelector(arrLen, null, values, null, nulls);
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
}
