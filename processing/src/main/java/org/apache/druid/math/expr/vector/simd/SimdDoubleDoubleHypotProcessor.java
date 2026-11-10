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
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;

/**
 * SIMD specialization of {@code (double[], double[]) -> double[]} {@code hypot(x, y)}.
 * {@code lanewise(VectorOperators.HYPOT)} is emitted with the operator literally inline; the JDK routes this to a
 * vectorized math library (Intel SVML on x86, SLEEF on Arm) rather than a single hardware instruction. Speedup is
 * JVM/build/hardware dependent, see the {@link SimdSupportedBinaryOp#HYPOT} javadoc.
 */
public final class SimdDoubleDoubleHypotProcessor extends SimdDoubleDoubleProcessor
{
  public SimdDoubleDoubleHypotProcessor(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      DoubleBivariateDoublesFunction scalarFallback
  )
  {
    super(left, right, scalarFallback);
  }

  @Override
  protected void processVector(
      double[] leftInput,
      double[] rightInput,
      boolean[] leftNulls,
      boolean[] rightNulls,
      int currentSize
  )
  {
    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(currentSize);
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      final DoubleVector va = DoubleVector.fromArray(SPECIES, leftInput, i);
      final DoubleVector vb = DoubleVector.fromArray(SPECIES, rightInput, i);
      va.lanewise(VectorOperators.HYPOT, vb).intoArray(outValues, i);
    }
    if (i < currentSize) {
      final VectorMask<Double> mask = SPECIES.indexInRange(i, currentSize);
      final DoubleVector va = DoubleVector.fromArray(SPECIES, leftInput, i, mask);
      final DoubleVector vb = DoubleVector.fromArray(SPECIES, rightInput, i, mask);
      va.lanewise(VectorOperators.HYPOT, vb).intoArray(outValues, i, mask);
    }
    SimdBinaryNulls.combineInto(outNulls, leftNulls, rightNulls, currentSize);
  }
}
