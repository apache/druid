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
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;

import javax.annotation.Nullable;

/**
 * Abstract base for SIMD processors that compute {@code (long[], long[]) -> long[]} ops. Each concrete subclass
 * (one per op) overrides {@link #processVector} with a hot loop that calls a statically-resolved {@link LongVector}
 * method (e.g. {@code va.add(vb)}) so the JIT emits the corresponding SIMD intrinsic.
 */
abstract class SimdLongLongProcessor implements ExprVectorProcessor<long[]>
{
  static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  private final ExprVectorProcessor<long[]> left;
  private final ExprVectorProcessor<long[]> right;
  final LongBivariateLongsFunction scalarFallback;
  final long[] outValues;
  final boolean[] outNulls;

  protected SimdLongLongProcessor(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      LongBivariateLongsFunction scalarFallback
  )
  {
    this.left = CastToTypeVectorProcessor.cast(left, ExpressionType.LONG);
    this.right = CastToTypeVectorProcessor.cast(right, ExpressionType.LONG);
    this.scalarFallback = scalarFallback;
    this.outValues = new long[this.left.maxVectorSize()];
    this.outNulls = new boolean[this.left.maxVectorSize()];
  }

  @Override
  public final ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
  {
    final ExprEvalVector<long[]> lhs = left.evalVector(bindings);
    final ExprEvalVector<long[]> rhs = right.evalVector(bindings);
    processVector(
        lhs.values(),
        rhs.values(),
        lhs.getNullVector(),
        rhs.getNullVector(),
        bindings.getCurrentVectorSize()
    );
    return new ExprEvalLongVector(outValues, outNulls);
  }

  protected abstract void processVector(
      long[] leftInput,
      long[] rightInput,
      @Nullable boolean[] leftNulls,
      @Nullable boolean[] rightNulls,
      int currentSize
  );

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
