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
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;

import java.util.Arrays;

/**
 * SIMD specialization of {@code (double[], double[]) -> double[]} addition. The op is hardcoded to
 * {@link DoubleVector#add} so the JIT statically resolves it to the platform's double-add intrinsic.
 */
public final class SimdDoubleDoubleAddProcessor extends SimdDoubleDoubleProcessor
{
  public SimdDoubleDoubleAddProcessor(
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
    final boolean hasLeftNulls = leftNulls != null;
    final boolean hasRightNulls = rightNulls != null;
    final int laneCount = SPECIES.length();
    final int upperBound = SPECIES.loopBound(currentSize);
    int i = 0;
    if (!hasLeftNulls && !hasRightNulls) {
      for (; i < upperBound; i += laneCount) {
        final DoubleVector va = DoubleVector.fromArray(SPECIES, leftInput, i);
        final DoubleVector vb = DoubleVector.fromArray(SPECIES, rightInput, i);
        va.add(vb).intoArray(outValues, i);
      }
      for (; i < currentSize; i++) {
        outValues[i] = scalarFallback.process(leftInput[i], rightInput[i]);
      }
      Arrays.fill(outNulls, 0, currentSize, false);
    } else {
      for (; i < upperBound; i += laneCount) {
        final VectorMask<Double> nm;
        if (hasLeftNulls && hasRightNulls) {
          nm = VectorMask.fromArray(SPECIES, leftNulls, i)
                         .or(VectorMask.fromArray(SPECIES, rightNulls, i));
        } else if (hasLeftNulls) {
          nm = VectorMask.fromArray(SPECIES, leftNulls, i);
        } else {
          nm = VectorMask.fromArray(SPECIES, rightNulls, i);
        }
        final DoubleVector va = DoubleVector.fromArray(SPECIES, leftInput, i);
        final DoubleVector vb = DoubleVector.fromArray(SPECIES, rightInput, i);
        va.add(vb).intoArray(outValues, i);
        nm.intoArray(outNulls, i);
      }
      for (; i < currentSize; i++) {
        final boolean isNull = (hasLeftNulls && leftNulls[i]) || (hasRightNulls && rightNulls[i]);
        outNulls[i] = isNull;
        if (!isNull) {
          outValues[i] = scalarFallback.process(leftInput[i], rightInput[i]);
        }
      }
    }
  }
}
