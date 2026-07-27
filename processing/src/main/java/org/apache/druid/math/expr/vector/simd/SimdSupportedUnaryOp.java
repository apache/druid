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
  NEG,
  ABS,
  SQRT,
  /**
   * Natural log. VO_MATHLIB code path.
   */
  LOG,
  /**
   * Natural exponentiation. VO_MATHLIB code path.
   */
  EXP,
  /**
   * Base-10 logarithm. VO_MATHLIB code path.
   */
  LOG10,
  /**
   * {@code log(1+x)}. VO_MATHLIB code path.
   */
  LOG1P,
  /**
   * {@code exp(x)-1}. VO_MATHLIB code path.
   */
  EXPM1,
  /**
   * Cube root. VO_MATHLIB code path.
   */
  CBRT,
  /**
   * Sine. VO_MATHLIB code path.
   */
  SIN,
  /**
   * Cosine. VO_MATHLIB code path.
   */
  COS,
  /**
   * Tangent. VO_MATHLIB code path.
   */
  TAN,
  /**
   * Arc sine. VO_MATHLIB code path.
   */
  ASIN,
  /**
   * Arc cosine. VO_MATHLIB code path.
   */
  ACOS,
  /**
   * Arc tangent. VO_MATHLIB code path.
   */
  ATAN,
  /**
   * Hyperbolic sine. VO_MATHLIB code path.
   */
  SINH,
  /**
   * Hyperbolic cosine. VO_MATHLIB code path.
   */
  COSH,
  /**
   * Hyperbolic tangent. VO_MATHLIB code path.
   */
  TANH
}
