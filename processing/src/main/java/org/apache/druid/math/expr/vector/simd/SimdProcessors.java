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
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;
import org.apache.druid.math.expr.vector.functional.LongUnivariateLongFunction;

/**
 * Dispatch table from {@link SimdSupportedBinaryOp} / {@link SimdSupportedUnaryOp} identifiers to concrete,
 * op-specialized SIMD processors. One class per op and type-combo so the JIT sees a monomorphic call site for
 * the SIMD operation in each hot loop.
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

  public static ExprVectorProcessor<long[]> makeLongUnary(
      ExprVectorProcessor<?> input,
      SimdSupportedUnaryOp op,
      LongUnivariateLongFunction scalarFallback
  )
  {
    return switch (op) {
      case NEG -> new SimdLongNegProcessor(input, scalarFallback);
      case ABS -> new SimdLongAbsProcessor(input, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD unary op[%s]", op);
    };
  }

  public static ExprVectorProcessor<double[]> makeDoubleUnary(
      ExprVectorProcessor<?> input,
      SimdSupportedUnaryOp op,
      DoubleUnivariateDoubleFunction scalarFallback
  )
  {
    return switch (op) {
      case NEG -> new SimdDoubleNegProcessor(input, scalarFallback);
      case ABS -> new SimdDoubleAbsProcessor(input, scalarFallback);
      case SQRT -> new SimdDoubleSqrtProcessor(input, scalarFallback);
      case LOG -> new SimdDoubleLogProcessor(input, scalarFallback);
      case EXP -> new SimdDoubleExpProcessor(input, scalarFallback);
      case LOG10 -> new SimdDoubleLog10Processor(input, scalarFallback);
      case LOG1P -> new SimdDoubleLog1pProcessor(input, scalarFallback);
      case EXPM1 -> new SimdDoubleExpm1Processor(input, scalarFallback);
      case CBRT -> new SimdDoubleCbrtProcessor(input, scalarFallback);
      case SIN -> new SimdDoubleSinProcessor(input, scalarFallback);
      case COS -> new SimdDoubleCosProcessor(input, scalarFallback);
      case TAN -> new SimdDoubleTanProcessor(input, scalarFallback);
      case ASIN -> new SimdDoubleAsinProcessor(input, scalarFallback);
      case ACOS -> new SimdDoubleAcosProcessor(input, scalarFallback);
      case ATAN -> new SimdDoubleAtanProcessor(input, scalarFallback);
      case SINH -> new SimdDoubleSinhProcessor(input, scalarFallback);
      case COSH -> new SimdDoubleCoshProcessor(input, scalarFallback);
      case TANH -> new SimdDoubleTanhProcessor(input, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD unary op[%s]", op);
    };
  }

  public static ExprVectorProcessor<double[]> makeLongToDoubleUnary(
      ExprVectorProcessor<?> input,
      SimdSupportedUnaryOp op,
      DoubleUnivariateLongFunction scalarFallback
  )
  {
    return switch (op) {
      case SQRT -> new SimdLongToDoubleSqrtProcessor(input, scalarFallback);
      case LOG -> new SimdLongToDoubleLogProcessor(input, scalarFallback);
      case EXP -> new SimdLongToDoubleExpProcessor(input, scalarFallback);
      case LOG10 -> new SimdLongToDoubleLog10Processor(input, scalarFallback);
      case LOG1P -> new SimdLongToDoubleLog1pProcessor(input, scalarFallback);
      case EXPM1 -> new SimdLongToDoubleExpm1Processor(input, scalarFallback);
      case CBRT -> new SimdLongToDoubleCbrtProcessor(input, scalarFallback);
      case SIN -> new SimdLongToDoubleSinProcessor(input, scalarFallback);
      case COS -> new SimdLongToDoubleCosProcessor(input, scalarFallback);
      case TAN -> new SimdLongToDoubleTanProcessor(input, scalarFallback);
      case ASIN -> new SimdLongToDoubleAsinProcessor(input, scalarFallback);
      case ACOS -> new SimdLongToDoubleAcosProcessor(input, scalarFallback);
      case ATAN -> new SimdLongToDoubleAtanProcessor(input, scalarFallback);
      case SINH -> new SimdLongToDoubleSinhProcessor(input, scalarFallback);
      case COSH -> new SimdLongToDoubleCoshProcessor(input, scalarFallback);
      case TANH -> new SimdLongToDoubleTanhProcessor(input, scalarFallback);
      default -> throw DruidException.defensive("Unsupported SIMD unary op[%s] for long->double", op);
    };
  }
}
