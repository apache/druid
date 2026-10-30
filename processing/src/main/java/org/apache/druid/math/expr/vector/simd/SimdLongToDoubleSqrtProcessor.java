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
import jdk.incubator.vector.VectorOperators;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;

import java.util.Arrays;

/**
 * SIMD specialization of {@code (long[]) -> double[]} square root. The long input is widened lane-by-lane to a
 * {@link DoubleVector} via {@code castShape} and then {@code lanewise(VectorOperators.SQRT)} is emitted with the
 * operator literally inline so the JIT statically resolves it to the platform's double-sqrt intrinsic (e.g.
 * {@code vsqrtpd} on x86).
 */
public final class SimdLongToDoubleSqrtProcessor extends SimdLongToDoubleUnaryProcessor
{
  public SimdLongToDoubleSqrtProcessor(ExprVectorProcessor<?> input, DoubleUnivariateLongFunction scalarFallback)
  {
    super(input, scalarFallback);
  }

  @Override
  protected void processVector(long[] input, boolean[] inputNulls, int currentSize)
  {
    final int laneCount = DOUBLE_SPECIES.length();
    final int upperBound = DOUBLE_SPECIES.loopBound(currentSize);
    int i = 0;
    for (; i < upperBound; i += laneCount) {
      final DoubleVector va =
          (DoubleVector) LongVector.fromArray(LONG_SPECIES, input, i).castShape(DOUBLE_SPECIES, 0);
      va.lanewise(VectorOperators.SQRT).intoArray(outValues, i);
    }
    for (; i < currentSize; i++) {
      outValues[i] = scalarFallback.process(input[i]);
    }
    if (inputNulls == null) {
      Arrays.fill(outNulls, 0, currentSize, false);
    } else {
      System.arraycopy(inputNulls, 0, outNulls, 0, currentSize);
    }
  }
}
