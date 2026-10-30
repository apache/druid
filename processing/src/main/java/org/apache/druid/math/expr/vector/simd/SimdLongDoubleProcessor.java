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
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;

import javax.annotation.Nullable;

/**
 * Abstract base for SIMD processors that compute {@code (long[], double[]) -> double[]} ops. The long lane is
 * widened to {@link DoubleVector} via {@code castShape(DoubleVector.SPECIES_PREFERRED, 0)} in each subclass's hot
 * loop. See {@link SimdLongLongProcessor} for the design rationale.
 */
abstract class SimdLongDoubleProcessor implements ExprVectorProcessor<double[]>
{
  static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

  private final ExprVectorProcessor<long[]> left;
  private final ExprVectorProcessor<double[]> right;
  final DoubleBivariateLongDoubleFunction scalarFallback;
  final double[] outValues;
  final boolean[] outNulls;

  protected SimdLongDoubleProcessor(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      DoubleBivariateLongDoubleFunction scalarFallback
  )
  {
    this.left = CastToTypeVectorProcessor.cast(left, ExpressionType.LONG);
    this.right = CastToTypeVectorProcessor.cast(right, ExpressionType.DOUBLE);
    this.scalarFallback = scalarFallback;
    this.outValues = new double[this.left.maxVectorSize()];
    this.outNulls = new boolean[this.left.maxVectorSize()];
  }

  @Override
  public final ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<long[]> lhs = left.evalVector(bindings);
    final ExprEvalVector<double[]> rhs = right.evalVector(bindings);
    processVector(
        lhs.values(),
        rhs.values(),
        lhs.getNullVector(),
        rhs.getNullVector(),
        bindings.getCurrentVectorSize()
    );
    return new ExprEvalDoubleVector(outValues, outNulls);
  }

  protected abstract void processVector(
      long[] leftInput,
      double[] rightInput,
      @Nullable boolean[] leftNulls,
      @Nullable boolean[] rightNulls,
      int currentSize
  );

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
