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

import org.apache.druid.math.expr.ExpressionProcessing;

/**
 * Identifies which unary math operations have a {@code jdk.incubator.vector} (SIMD) specialization. Used by
 * {@link org.apache.druid.math.expr.vector.SimpleVectorMathUnivariateProcessorFactory} subclasses to declare that
 * their operation can be dispatched to a SIMD variant when the user enables
 * {@link org.apache.druid.math.expr.ExpressionProcessingConfig#USE_VECTOR_API}.
 *
 * <p>Deliberately does not reference any {@code jdk.incubator.vector} types so that callers wiring the enum into
 * factories do not need the incubator module visible.
 *
 * <p>Ops annotated as <em>VO_MATHLIB code path</em> below are backed by a vectorized math library (Intel SVML on
 * x86, SLEEF on Arm) via {@code VectorOperators.<OP>}, not a direct hardware intrinsic. Performance is
 * JVM/build/hardware dependent, typically 1.5x-4x faster than the scalar {@link Math} equivalent on JVMs with
 * SVML/SLEEF wired up, but can fall back to a scalar-loop implementation on builds without that wiring. The
 * remaining ops (NEG, ABS, SQRT) dispatch to direct hardware FP intrinsics and are consistently faster than
 * scalar across every JVM/hardware combo.
 */
public enum SimdSupportedUnaryOp
{
  NEG(false),
  ABS(false),
  SQRT(false),
  /**
   * Natural log. VO_MATHLIB code path.
   */
  LOG(true),
  /**
   * Natural exponentiation. VO_MATHLIB code path.
   */
  EXP(true),
  /**
   * Base-10 logarithm. VO_MATHLIB code path.
   */
  LOG10(true),
  /**
   * {@code log(1+x)}. VO_MATHLIB code path.
   */
  LOG1P(true),
  /**
   * {@code exp(x)-1}. VO_MATHLIB code path.
   */
  EXPM1(true),
  /**
   * Cube root. VO_MATHLIB code path.
   */
  CBRT(true),
  /**
   * Sine. VO_MATHLIB code path.
   */
  SIN(true),
  /**
   * Cosine. VO_MATHLIB code path.
   */
  COS(true),
  /**
   * Tangent. VO_MATHLIB code path.
   */
  TAN(true),
  /**
   * Arc sine. VO_MATHLIB code path.
   */
  ASIN(true),
  /**
   * Arc cosine. VO_MATHLIB code path.
   */
  ACOS(true),
  /**
   * Arc tangent. VO_MATHLIB code path.
   */
  ATAN(true),
  /**
   * Hyperbolic sine. VO_MATHLIB code path.
   */
  SINH(true),
  /**
   * Hyperbolic cosine. VO_MATHLIB code path.
   */
  COSH(true),
  /**
   * Hyperbolic tangent. VO_MATHLIB code path.
   */
  TANH(true);

  private final boolean mathLib;

  SimdSupportedUnaryOp(boolean mathLib)
  {
    this.mathLib = mathLib;
  }

  /**
   * Whether this op's SIMD path routes through the JDK's VO_MATHLIB (SVML/SLEEF) dispatch rather than a direct
   * hardware FP intrinsic. Callers that gate on
   * {@link org.apache.druid.math.expr.ExpressionProcessingConfig#USE_VECTOR_MATH_API} should consult this to
   * decide whether the extra flag applies.
   */
  public boolean isMathLib()
  {
    return mathLib;
  }

  /**
   * Whether SIMD dispatch is currently enabled for this op according to the runtime
   * {@link ExpressionProcessing#useVectorApi()} / {@link ExpressionProcessing#useVectorMathApi()} flags.
   */
  public boolean isSimdEnabled()
  {
    if (!ExpressionProcessing.useVectorApi()) {
      return false;
    }
    return !mathLib || ExpressionProcessing.useVectorMathApi();
  }
}
