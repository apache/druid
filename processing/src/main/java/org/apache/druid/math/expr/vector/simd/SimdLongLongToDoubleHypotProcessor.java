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

package org.apache.druid.math.expr.vector.simd;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongsFunction;

/**
 * SIMD specialization of {@code (long[], long[]) -> double[]} {@code hypot(x, y)}. Both long inputs are widened
 * lane-by-lane to {@link DoubleVector} via {@code castShape} before {@code lanewise(VectorOperators.HYPOT)}.
 * Backed by a vectorized math library (see {@link SimdSupportedBinaryOp#HYPOT}).
 */
public final class SimdLongLongToDoubleHypotProcessor extends SimdLongLongToDoubleProcessor
{
  public SimdLongLongToDoubleHypotProcessor(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      DoubleBivariateLongsFunction scalarFallback
  )
  {
    super(left, right, scalarFallback);
  }

  @Override
  protected void processVector(
      long[] leftInput,
      long[] rightInput,
      boolean[] leftNulls,
      boolean[] rightNulls,
      int currentSize
  )
  {
    final int laneCount = DOUBLE_SPECIES.length();
    final int upperBound = DOUBLE_SPECIES.loopBound(currentSize);
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      final DoubleVector va =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, leftInput, i).castShape(DOUBLE_SPECIES, 0);
      final DoubleVector vb =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, rightInput, i).castShape(DOUBLE_SPECIES, 0);
      va.lanewise(VectorOperators.HYPOT, vb).intoArray(outValues, i);
    }
    if (i < currentSize) {
      final VectorMask<Long> longMask = LONG_SPECIES.indexInRange(i, currentSize);
      final VectorMask<Double> doubleMask = DOUBLE_SPECIES.indexInRange(i, currentSize);
      final DoubleVector va =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, leftInput, i, longMask).castShape(DOUBLE_SPECIES, 0);
      final DoubleVector vb =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, rightInput, i, longMask).castShape(DOUBLE_SPECIES, 0);
      va.lanewise(VectorOperators.HYPOT, vb).intoArray(outValues, i, doubleMask);
    }
    SimdBinaryNulls.combineInto(outNulls, leftNulls, rightNulls, currentSize);
  }
}
