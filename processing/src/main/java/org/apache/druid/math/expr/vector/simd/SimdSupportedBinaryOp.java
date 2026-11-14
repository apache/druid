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
 * Identifies which binary math operations have a {@code jdk.incubator.vector} (SIMD) specialization. Used by
 * {@link org.apache.druid.math.expr.vector.SimpleVectorMathBivariateProcessorFactory} subclasses to declare that
 * their operation can be dispatched to a SIMD variant when the user enables
 * {@link org.apache.druid.math.expr.ExpressionProcessingConfig#USE_VECTOR_API}.
 *
 * Deliberately does not reference any {@code jdk.incubator.vector} types so that callers wiring the enum into
 * factories do not need the incubator module visible.
 *
 * <p>Ops annotated as <em>VO_MATHLIB code path</em> below are backed by a vectorized math library (Intel SVML on
 * x86, SLEEF on Arm) via {@code VectorOperators.<OP>}, not a direct hardware intrinsic. See the same annotation
 * on {@link SimdSupportedUnaryOp} for the full accuracy caveats — the {@code useVectorMathApi} opt-in gates
 * these ops for the same reason.
 */
public enum SimdSupportedBinaryOp
{
  ADD(true, false),
  SUB(true, false),
  MUL(true, false),
  /**
   * {@code jdk.incubator.vector} has no long-integer division intrinsic (SIMD hardware lacks it), and long
   * division by zero would throw {@link ArithmeticException} in the middle of a chunk. So the {@code long × long}
   * combo falls through to the scalar processor for DIV; only the double-output combos are SIMD-specialized.
   */
  DIV(false, false),
  /**
   * Exponentiation. VO_MATHLIB code path. {@code VectorOperators.POW} returns a
   * {@link jdk.incubator.vector.DoubleVector} only, so the {@code long × long -> long} combo (produced by
   * {@code LongMath.pow} in the scalar factory) is not SIMD-specialized here; the three double-returning combos are.
   */
  POW(false, true),
  /**
   * {@code atan2(y, x)}. VO_MATHLIB code path. Returns double for every input shape so all four combos SIMD.
   */
  ATAN2(false, true),
  /**
   * {@code hypot(x, y) = sqrt(x*x + y*y)} without intermediate overflow/underflow. VO_MATHLIB code path. Returns
   * double for every input shape so all four combos SIMD.
   */
  HYPOT(false, true);

  private final boolean supportsLongLong;
  private final boolean mathLib;

  SimdSupportedBinaryOp(boolean supportsLongLong, boolean mathLib)
  {
    this.supportsLongLong = supportsLongLong;
    this.mathLib = mathLib;
  }

  /**
   * Whether this op has a SIMD specialization for the {@code long × long -> long} type combo. Callers can use
   * this to decide whether to route the {@code longsProcessor} path to SIMD or fall back to scalar.
   */
  public boolean supportsLongLong()
  {
    return supportsLongLong;
  }

  /**
   * Whether this op's SIMD path routes through the JDK's VO_MATHLIB (SVML/SLEEF) dispatch rather than a direct
   * hardware FP intrinsic. See {@link SimdSupportedUnaryOp#isMathLib()}.
   */
  public boolean isMathLib()
  {
    return mathLib;
  }

  /**
   * Whether SIMD dispatch is currently enabled for this op according to the runtime
   * {@link ExpressionProcessing#useVectorApi()} / {@link ExpressionProcessing#useVectorMathApi()} flags. Rolls up
   * the base "SIMD on?" check with the VO_MATHLIB-specific opt-in so factories only see one call site.
   */
  public boolean isSimdEnabled()
  {
    if (!ExpressionProcessing.useVectorApi()) {
      return false;
    }
    return !mathLib || ExpressionProcessing.useVectorMathApi();
  }
}
