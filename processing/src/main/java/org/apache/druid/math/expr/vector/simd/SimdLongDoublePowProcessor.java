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
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;

/**
 * SIMD specialization of {@code (long[], double[]) -> double[]} {@code pow(x, y)}. The long input is widened
 * lane-by-lane to a {@link DoubleVector} via {@code castShape} before {@code lanewise(VectorOperators.POW)}. Backed
 * by a vectorized math library (see {@link SimdSupportedBinaryOp#POW}). The result is run through
 * {@link SimdDoubleDoublePowProcessor#withNanFixup} to recover {@link Math#pow}'s NaN behavior at the C99 corner
 * cases: the widened long base can be exactly {@code ±1.0}, and the double exponent can be {@code NaN} or
 * {@code ±Infinity}.
 */
public final class SimdLongDoublePowProcessor extends SimdLongDoubleProcessor
{
  public SimdLongDoublePowProcessor(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      DoubleBivariateLongDoubleFunction scalarFallback
  )
  {
    super(left, right, scalarFallback);
  }

  @Override
  protected void processVector(
      long[] leftInput,
      double[] rightInput,
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
      final DoubleVector vb = DoubleVector.fromArray(DOUBLE_SPECIES, rightInput, i);
      SimdDoubleDoublePowProcessor.withNanFixup(va, vb).intoArray(outValues, i);
    }
    if (i < currentSize) {
      final VectorMask<Long> longMask = LONG_SPECIES.indexInRange(i, currentSize);
      final VectorMask<Double> doubleMask = DOUBLE_SPECIES.indexInRange(i, currentSize);
      final DoubleVector va =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, leftInput, i, longMask).castShape(DOUBLE_SPECIES, 0);
      final DoubleVector vb = DoubleVector.fromArray(DOUBLE_SPECIES, rightInput, i, doubleMask);
      SimdDoubleDoublePowProcessor.withNanFixup(va, vb).intoArray(outValues, i, doubleMask);
    }
    SimdBinaryNulls.combineInto(outNulls, leftNulls, rightNulls, currentSize);
  }
}
