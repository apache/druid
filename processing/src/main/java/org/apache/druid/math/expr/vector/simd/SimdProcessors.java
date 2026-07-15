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

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;

/**
 * Dispatch table from a {@link SimdSupportedBinaryOp} identifier to a concrete, op-specialized SIMD processor.
 * One class per op and type-combo so the JIT sees a monomorphic call site for the SIMD operation in each hot loop.
 */
public final class SimdProcessors
{
  private SimdProcessors()
  {
  }

  public static ExprVectorProcessor<long[]> makeLongLong(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      SimdSupportedBinaryOp op,
      LongBivariateLongsFunction scalarFallback
  )
  {
    return switch (op) {
      case ADD -> new SimdLongLongAddProcessor(left, right, scalarFallback);
      case SUB -> new SimdLongLongSubProcessor(left, right, scalarFallback);
      case MUL -> new SimdLongLongMulProcessor(left, right, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD binary op[%s]", op);
    };
  }

  public static ExprVectorProcessor<double[]> makeDoubleDouble(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      SimdSupportedBinaryOp op,
      DoubleBivariateDoublesFunction scalarFallback
  )
  {
    return switch (op) {
      case ADD -> new SimdDoubleDoubleAddProcessor(left, right, scalarFallback);
      case SUB -> new SimdDoubleDoubleSubProcessor(left, right, scalarFallback);
      case MUL -> new SimdDoubleDoubleMulProcessor(left, right, scalarFallback);
      case DIV -> new SimdDoubleDoubleDivProcessor(left, right, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD binary op[%s]", op);
    };
  }

  public static ExprVectorProcessor<double[]> makeLongDouble(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      SimdSupportedBinaryOp op,
      DoubleBivariateLongDoubleFunction scalarFallback
  )
  {
    return switch (op) {
      case ADD -> new SimdLongDoubleAddProcessor(left, right, scalarFallback);
      case SUB -> new SimdLongDoubleSubProcessor(left, right, scalarFallback);
      case MUL -> new SimdLongDoubleMulProcessor(left, right, scalarFallback);
      case DIV -> new SimdLongDoubleDivProcessor(left, right, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD binary op[%s]", op);
    };
  }

  public static ExprVectorProcessor<double[]> makeDoubleLong(
      ExprVectorProcessor<?> left,
      ExprVectorProcessor<?> right,
      SimdSupportedBinaryOp op,
      DoubleBivariateDoubleLongFunction scalarFallback
  )
  {
    return switch (op) {
      case ADD -> new SimdDoubleLongAddProcessor(left, right, scalarFallback);
      case SUB -> new SimdDoubleLongSubProcessor(left, right, scalarFallback);
      case MUL -> new SimdDoubleLongMulProcessor(left, right, scalarFallback);
      case DIV -> new SimdDoubleLongDivProcessor(left, right, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD binary op[%s]", op);
    };
  }
}
