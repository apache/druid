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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the SIMD processors for VO_MATHLIB unary ops (SVML/SLEEF-backed transcendentals) are
 * <ol>
 *   <li>bit-stable across invocations once the JIT has promoted the loop to C2, and</li>
 *   <li>within a small ulp bound of the scalar {@link Math} equivalent on in-domain inputs.</li>
 * </ol>
 *
 * <p>on JDK 25/AArch64 the C2-compiled SVML/SLEEF dispatch was observed to differ from {@code Math.sin} in ~8% of
 * lanes, and to change bits across the C1→C2 tier transition. This test does not attempt to prove
 * no-divergence-vs-{@code Math} (there is some, and we accept that for {@code useVectorMathApi=true}); it establishes
 * that (a) once the loop is fully warmed, further invocations do not shift again, and (b) the divergence is bounded
 * so we can catch regressions if a future JDK/hardware combo drifts substantially.
 */
public class SimdVoMathlibParityTest
{
  // > any tiered-compilation threshold in HotSpot; ensures C2 has compiled the vector loop before
  // we start capturing "final" output.
  private static final int WARMUP_ITERATIONS = 30_000;
  private static final int STABILITY_ITERATIONS = 500;
  // Small integer multiple of any reasonable DoubleVector.SPECIES_PREFERRED length (2/4/8) so the SIMD
  // loop takes multiple iterations per invocation.
  private static final int INPUT_LANES = 64;
  // Small ulp bound is looser than Java's spec for Math (usually 1 ulp) but tight enough to catch a
  // meaningful regression. sinh/cosh/tanh are spec'd for 2.5 ulps in Math itself, so we give a bit of
  // margin for the SIMD dispatch to match.
  private static final int MAX_ULPS = 4;

  private static final Expr.VectorInputBinding STUB_BINDING = new StubBinding(INPUT_LANES);

  @Test
  public void doubleInputStabilityAndParity()
  {
    for (SimdSupportedUnaryOp op : SimdSupportedUnaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleUnivariateDoubleFunction scalar = doubleScalarFor(op);
      final double[] inputs = inDomainDoubleInputs(op);
      final ExprVectorProcessor<double[]> processor =
          SimdProcessors.makeDoubleUnary(fixedDoubleInput(inputs), op, scalar);

      // warm up
      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable(op, warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityDouble(op, inputs, warmed, scalar);
    }
  }

  @Test
  public void longInputStabilityAndParity()
  {
    for (SimdSupportedUnaryOp op : SimdSupportedUnaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleUnivariateLongFunction scalar = longScalarFor(op);
      final long[] inputs = inDomainLongInputs(op);
      final ExprVectorProcessor<double[]> processor =
          SimdProcessors.makeLongToDoubleUnary(fixedLongInput(inputs), op, scalar);

      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable(op, warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityLong(op, inputs, warmed, scalar);
    }
  }

  private static void assertStable(SimdSupportedUnaryOp op, double[] warmed, ValuesSupplier rerun)
  {
    for (int i = 0; i < STABILITY_ITERATIONS; i++) {
      final double[] again = rerun.get();
      for (int j = 0; j < warmed.length; j++) {
        Assert.assertEquals(
            "op=" + op + " lane=" + j + " bit-unstable after warmup at iter=" + i,
            Double.doubleToRawLongBits(warmed[j]),
            Double.doubleToRawLongBits(again[j])
        );
      }
    }
  }

  private static void assertParityDouble(
      SimdSupportedUnaryOp op,
      double[] inputs,
      double[] simd,
      DoubleUnivariateDoubleFunction scalar
  )
  {
    for (int i = 0; i < inputs.length; i++) {
      final double expected = scalar.process(inputs[i]);
      final double actual = simd[i];
      assertWithinUlps(op, i, inputs[i], expected, actual);
    }
  }

  private static void assertParityLong(
      SimdSupportedUnaryOp op,
      long[] inputs,
      double[] simd,
      DoubleUnivariateLongFunction scalar
  )
  {
    for (int i = 0; i < inputs.length; i++) {
      final double expected = scalar.process(inputs[i]);
      final double actual = simd[i];
      assertWithinUlps(op, i, (double) inputs[i], expected, actual);
    }
  }

  private static void assertWithinUlps(SimdSupportedUnaryOp op, int lane, double input, double expected, double actual)
  {
    if (Double.isNaN(expected) && Double.isNaN(actual)) {
      return;
    }
    if (Double.doubleToRawLongBits(expected) == Double.doubleToRawLongBits(actual)) {
      return;
    }
    final double ulp = Math.ulp(expected);
    final double diff = Math.abs(actual - expected);
    if (Double.isNaN(diff) || diff > MAX_ULPS * ulp) {
      Assert.fail(String.format(
          "op=%s lane=%d input=%s: SIMD result %s differs from Math=%s by %s ulps (>%d allowed)",
          op, lane, input, actual, expected, diff / ulp, MAX_ULPS
      ));
    }
  }

  private static DoubleUnivariateDoubleFunction doubleScalarFor(SimdSupportedUnaryOp op)
  {
    return switch (op) {
      case LOG -> Math::log;
      case EXP -> Math::exp;
      case LOG10 -> Math::log10;
      case LOG1P -> Math::log1p;
      case EXPM1 -> Math::expm1;
      case CBRT -> Math::cbrt;
      case SIN -> Math::sin;
      case COS -> Math::cos;
      case TAN -> Math::tan;
      case ASIN -> Math::asin;
      case ACOS -> Math::acos;
      case ATAN -> Math::atan;
      case SINH -> Math::sinh;
      case COSH -> Math::cosh;
      case TANH -> Math::tanh;
      default -> throw new IllegalStateException("not a VO_MATHLIB op: " + op);
    };
  }

  private static DoubleUnivariateLongFunction longScalarFor(SimdSupportedUnaryOp op)
  {
    final DoubleUnivariateDoubleFunction dbl = doubleScalarFor(op);
    return x -> dbl.process((double) x);
  }

  /**
   * A fixed input grid that stays inside the domain of every VO_MATHLIB op we ship (positive, magnitude ≤1
   * so ASIN/ACOS don't degenerate to NaN, plus a couple of edge points). Deterministic — same values
   * every run — so parity failures reproduce.
   */
  private static double[] inDomainDoubleInputs(SimdSupportedUnaryOp op)
  {
    final double[] xs = new double[INPUT_LANES];
    // Bias toward small positive; include 0, small, ~1, negative-but-in-domain-for-atan/sin/cos family.
    for (int i = 0; i < INPUT_LANES; i++) {
      xs[i] = 0.01 + 0.9 * ((double) i / INPUT_LANES);
    }
    // Sprinkle a few explicit interesting points at fixed lanes.
    xs[0] = 0.0;
    xs[1] = 1.0;
    xs[2] = 0.5;
    xs[3] = Math.PI / 4;
    // For ATAN/ATAN2/EXP/CBRT-family which accept any real, include negatives (still in-domain for those).
    if (op == SimdSupportedUnaryOp.ATAN
        || op == SimdSupportedUnaryOp.EXP
        || op == SimdSupportedUnaryOp.EXPM1
        || op == SimdSupportedUnaryOp.CBRT
        || op == SimdSupportedUnaryOp.SIN
        || op == SimdSupportedUnaryOp.COS
        || op == SimdSupportedUnaryOp.TAN
        || op == SimdSupportedUnaryOp.SINH
        || op == SimdSupportedUnaryOp.COSH
        || op == SimdSupportedUnaryOp.TANH) {
      xs[4] = -0.7;
      xs[5] = -Math.PI / 3;
    }
    // ASIN/ACOS also want ± values within [-1, 1].
    if (op == SimdSupportedUnaryOp.ASIN || op == SimdSupportedUnaryOp.ACOS) {
      xs[4] = -0.7;
      xs[5] = -0.99;
    }
    return xs;
  }

  private static long[] inDomainLongInputs(SimdSupportedUnaryOp op)
  {
    // For long->double the input widens to double, so scale is arbitrary. Small positive integers keep
    // LOG-family in-domain. ATAN/EXP-family also fine on small integers. ASIN/ACOS specifically need
    // inputs in [-1, 1], which longs can only represent as 0 and ±1 — cover just those.
    if (op == SimdSupportedUnaryOp.ASIN || op == SimdSupportedUnaryOp.ACOS) {
      final long[] xs = new long[INPUT_LANES];
      for (int i = 0; i < INPUT_LANES; i++) {
        xs[i] = i % 2 == 0 ? 0L : (i % 3 == 0 ? 1L : -1L);
      }
      return xs;
    }
    final long[] xs = new long[INPUT_LANES];
    for (int i = 0; i < INPUT_LANES; i++) {
      xs[i] = 1L + i;
    }
    return xs;
  }

  private static ExprVectorProcessor<double[]> fixedDoubleInput(double[] values)
  {
    return new ExprVectorProcessor<>()
    {
      @Override
      public ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding bindings)
      {
        return new ExprEvalDoubleVector(values, null);
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.DOUBLE;
      }

      @Override
      public int maxVectorSize()
      {
        return values.length;
      }
    };
  }

  private static ExprVectorProcessor<long[]> fixedLongInput(long[] values)
  {
    return new ExprVectorProcessor<>()
    {
      @Override
      public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding bindings)
      {
        return new ExprEvalLongVector(values, null);
      }

      @Override
      public ExpressionType getOutputType()
      {
        return ExpressionType.LONG;
      }

      @Override
      public int maxVectorSize()
      {
        return values.length;
      }
    };
  }

  @FunctionalInterface
  private interface ValuesSupplier
  {
    double[] get();
  }

  private static final class StubBinding implements Expr.VectorInputBinding
  {
    private final int size;

    StubBinding(int size)
    {
      this.size = size;
    }

    @Override
    public int getMaxVectorSize()
    {
      return size;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return size;
    }

    @Override
    public int getCurrentVectorId()
    {
      return 0;
    }

    @Override
    public Object[] getObjectVector(String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongVector(String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleVector(String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] getNullVector(String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExpressionType getType(String name)
    {
      return null;
    }
  }
}
