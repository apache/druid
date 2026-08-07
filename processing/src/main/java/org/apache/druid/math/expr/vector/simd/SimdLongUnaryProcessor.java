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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.CastToTypeVectorProcessor;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.LongUnivariateLongFunction;

import javax.annotation.Nullable;

/**
 * Abstract base for SIMD processors that compute {@code (long[]) -> long[]} unary ops. Each concrete subclass
 * (one per op) overrides {@link #processVector} with a hot loop that calls a statically-resolved {@link LongVector}
 * method (e.g. {@code va.neg()} or {@code va.abs()}) so the JIT emits the corresponding SIMD intrinsic.
 */
abstract class SimdLongUnaryProcessor implements ExprVectorProcessor<long[]>
{
  static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  private final ExprVectorProcessor<long[]> input;
  final LongUnivariateLongFunction scalarFallback;
  final long[] outValues;
  final boolean[] outNulls;

  protected SimdLongUnaryProcessor(
      ExprVectorProcessor<?> input,
      LongUnivariateLongFunction scalarFallback
  )
  {
    this.input = CastToTypeVectorProcessor.cast(input, ExpressionType.LONG);
    this.scalarFallback = scalarFallback;
    this.outValues = new long[this.input.maxVectorSize()];
    this.outNulls = new boolean[this.input.maxVectorSize()];
  }

  @Override
  public final ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<long[]> lhs = input.evalVector(bindings);
    processVector(lhs.values(), lhs.getNullVector(), bindings.getCurrentVectorSize());
    return new ExprEvalLongVector(outValues, outNulls);
  }

  protected abstract void processVector(long[] input, @Nullable boolean[] inputNulls, int currentSize);

  @Override
  public final ExpressionType getOutputType()
  {
    return ExpressionType.LONG;
  }

  @Override
  public final int maxVectorSize()
  {
    return outValues.length;
  }
}
