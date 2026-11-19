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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateDoubleFunction;

import java.util.Arrays;

/**
 * SIMD specialization of {@code (double[]) -> double[]} {@code exp(x)-1}. See {@link SimdSupportedUnaryOp#EXPM1}
 * for the shared VO_MATHLIB performance notes.
 */
public final class SimdDoubleExpm1Processor extends SimdDoubleUnaryProcessor
{
  public SimdDoubleExpm1Processor(ExprVectorProcessor<?> input, DoubleUnivariateDoubleFunction scalarFallback)
  {
    super(input, scalarFallback);
  }

  @Override
  protected void processVector(double[] input, boolean[] inputNulls, int currentSize)
  {
    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(currentSize);
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      DoubleVector.fromArray(SPECIES, input, i).lanewise(VectorOperators.EXPM1).intoArray(outValues, i);
    }
    if (i < currentSize) {
      final VectorMask<Double> mask = SPECIES.indexInRange(i, currentSize);
      DoubleVector.fromArray(SPECIES, input, i, mask).lanewise(VectorOperators.EXPM1).intoArray(outValues, i, mask);
    }
    if (inputNulls == null) {
      Arrays.fill(outNulls, 0, currentSize, false);
    } else {
      System.arraycopy(inputNulls, 0, outNulls, 0, currentSize);
    }
  }
}
