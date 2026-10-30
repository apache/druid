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
import jdk.incubator.vector.VectorSpecies;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.CastToTypeVectorProcessor;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;

import javax.annotation.Nullable;

/**
 * Abstract base for SIMD processors that compute {@code (long[]) -> double[]} unary ops — the long input is
 * widened lane-by-lane to a {@link DoubleVector} via {@code castShape(DoubleVector.SPECIES_PREFERRED, 0)} in each
 * subclass's hot loop before the op. See {@link SimdLongUnaryProcessor} for the general design rationale.
 */
abstract class SimdLongToDoubleUnaryProcessor implements ExprVectorProcessor<double[]>
{
  static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

  private final ExprVectorProcessor<long[]> input;
  final DoubleUnivariateLongFunction scalarFallback;
  final double[] outValues;
  final boolean[] outNulls;

  protected SimdLongToDoubleUnaryProcessor(
      ExprVectorProcessor<?> input,
      DoubleUnivariateLongFunction scalarFallback
  )
  {
    this.input = CastToTypeVectorProcessor.cast(input, ExpressionType.LONG);
    this.scalarFallback = scalarFallback;
    this.outValues = new double[this.input.maxVectorSize()];
    this.outNulls = new boolean[this.input.maxVectorSize()];
  }

  @Override
  public final ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<long[]> lhs = input.evalVector(bindings);
    processVector(lhs.values(), lhs.getNullVector(), bindings.getCurrentVectorSize());
    return new ExprEvalDoubleVector(outValues, outNulls);
  }

  protected abstract void processVector(long[] input, @Nullable boolean[] inputNulls, int currentSize);

  @Override
  public final ExpressionType getOutputType()
  {
    return ExpressionType.DOUBLE;
  }

  @Override
  public final int maxVectorSize()
  {
    return outValues.length;
  }
}
