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

import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Random;
import java.util.function.IntPredicate;

/**
 * Shared fixtures for the SIMD vector aggregator equivalence tests. Holds the vector-size axis, a set of null
 * patterns designed to stress specific regions of the SIMD chunk loop, a minimal in-memory
 * {@link VectorValueSelector}, and small pseudo-random data generators.
 */
final class SimdAggregatorTestHelpers
{
  /**
   * Vector sizes picked to exercise distinct regimes against typical SIMD species lengths: 1 is tail-only,
   * 8 fills an exact AVX2/AVX-512 chunk, 17 leaves a ragged tail after one or more chunks, 64 spans many chunks,
   * and 1023 is large with a ragged tail.
   */
  static final int[] VECTOR_SIZES = {1, 8, 17, 64, 1023};

  private SimdAggregatorTestHelpers()
  {
  }

  /**
   * Returns a {@code boolean[]} of length {@code startRow + realNulls.length} with the real null pattern copied
   * to indices {@code [startRow, startRow + realNulls.length)}. The leading slots are left as {@code false}
   * (non-null) so that if the aggregator incorrectly reads past {@code startRow} the poison values would slip in.
   */
  static boolean[] padNulls(boolean[] realNulls, int startRow)
  {
    final boolean[] padded = new boolean[startRow + realNulls.length];
    System.arraycopy(realNulls, 0, padded, startRow, realNulls.length);
    return padded;
  }

  static long[] randomLongs(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final long[] out = new long[size];
    for (int i = 0; i < size; i++) {
      out[i] = r.nextInt() & 0xFFFFFL;
    }
    return out;
  }

  static double[] randomDoubles(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final double[] out = new double[size];
    for (int i = 0; i < size; i++) {
      out[i] = (r.nextDouble() - 0.5) * 1000.0;
    }
    return out;
  }

  static float[] randomFloats(int size, int seed)
  {
    final Random r = new Random(0xC0FFEEL + seed);
    final float[] out = new float[size];
    for (int i = 0; i < size; i++) {
      out[i] = (r.nextFloat() - 0.5f) * 1000.0f;
    }
    return out;
  }

  /**
   * Null distributions designed to stress the SIMD null-aware path. {@link #NONE} returns {@code null} from
   * {@link #toMask(int)} to model a column with no null vector at all (the common non-nullable case). The rest
   * exercise sparse, dense, all-null, alternating, and chunk-boundary-adjacent placements.
   */
  enum NullPattern
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
        return null;
      }
      final boolean[] mask = new boolean[size];
      for (int i = 0; i < size; i++) {
        mask[i] = predicate.test(i);
      }
      return mask;
    }
  }

  /**
   * Minimal in-memory {@link VectorValueSelector} backed by pre-built primitive arrays. Only the accessor for the
   * type used by a given test is non-null; the others return whatever was passed at construction (typically null).
   */
  static final class FakeVectorValueSelector implements VectorValueSelector
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
