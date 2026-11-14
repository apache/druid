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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.VectorTestAssertions;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongsFunction;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.DoubleBinaryOperator;

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
  // Per-op ulp bound for the SIMD-vs-scalar comparison. Most transcendentals are spec'd tight — Java's Math
  // is within 1 ulp of the correctly-rounded result for sin/cos/tan/log/exp/asin/acos/atan/cbrt/etc., and
  // the SIMD path targets the same, so the SIMD-vs-scalar delta can be up to 2 ulps from both landing at
  // opposite ends of their independent 1-ulp windows. sinh/cosh/tanh however are only spec'd to 2.5 ulps in
  // Math, and VectorOperators offers no tighter guarantee, so their SIMD-vs-scalar delta can plausibly reach
  // ~5 ulps by the same triangle-inequality argument. Math.atan2 is spec'd to 2 ulps so its SIMD-vs-Math
  // bound is 4 ulps; Math.pow and Math.hypot are both 1 ulp like the common unary set, so bound is 2 ulps.
  private static final int DEFAULT_MAX_ULPS = 2;
  private static final int HYPERBOLIC_MAX_ULPS = 5;
  private static final int ATAN2_MAX_ULPS = 4;

  private static int maxUlpsFor(SimdSupportedUnaryOp op)
  {
    return switch (op) {
      case SINH, COSH, TANH -> HYPERBOLIC_MAX_ULPS;
      default -> DEFAULT_MAX_ULPS;
    };
  }

  private static int maxUlpsFor(SimdSupportedBinaryOp op)
  {
    return switch (op) {
      case ATAN2 -> ATAN2_MAX_ULPS;
      case POW, HYPOT -> DEFAULT_MAX_ULPS;
      default -> throw new IllegalStateException("not a VO_MATHLIB op: " + op);
    };
  }

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
  public void doubleInputTailBypassesScalarFallback()
  {
    final DoubleUnivariateDoubleFunction poison = x -> {
      throw new IllegalStateException("scalar fallback called for x=" + x);
    };
    for (SimdSupportedUnaryOp op : SimdSupportedUnaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final double[] inputs = inDomainDoubleInputs(op);
      final ExprVectorProcessor<double[]> processor =
          SimdProcessors.makeDoubleUnary(fixedDoubleInput(inputs), op, poison);
      for (int size : partialTailSizes()) {
        final Expr.VectorInputBinding partial = new StubBinding(INPUT_LANES, size);
        // Would throw here if the tail path invoked the poisoned scalarFallback for any lane.
        final double[] out = processor.evalVector(partial).values();
        Assertions.assertEquals(INPUT_LANES, out.length, "op=" + op + " size=" + size + " unexpected output length");
      }
    }
  }

  @Test
  public void longInputTailBypassesScalarFallback()
  {
    final DoubleUnivariateLongFunction poison = x -> {
      throw new IllegalStateException("scalar fallback called for x=" + x);
    };
    for (SimdSupportedUnaryOp op : SimdSupportedUnaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final long[] inputs = inDomainLongInputs(op);
      final ExprVectorProcessor<double[]> processor =
          SimdProcessors.makeLongToDoubleUnary(fixedLongInput(inputs), op, poison);
      for (int size : partialTailSizes()) {
        final Expr.VectorInputBinding partial = new StubBinding(INPUT_LANES, size);
        final double[] out = processor.evalVector(partial).values();
        Assertions.assertEquals(INPUT_LANES, out.length, "op=" + op + " size=" + size + " unexpected output length");
      }
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
    assertStable("op=" + op, warmed, rerun);
  }

  private static void assertStable(String label, double[] warmed, ValuesSupplier rerun)
  {
    for (int i = 0; i < STABILITY_ITERATIONS; i++) {
      final double[] again = rerun.get();
      for (int j = 0; j < warmed.length; j++) {
        Assertions.assertEquals(
            Double.doubleToRawLongBits(warmed[j]),
            Double.doubleToRawLongBits(again[j]),
            label + " lane=" + j + " bit-unstable after warmup at iter=" + i
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
      VectorTestAssertions.assertDoublesEquivalent(
          StringUtils.format("op=%s lane=%d input=%s SIMD-vs-Math", op, i, inputs[i]),
          scalar.process(inputs[i]),
          simd[i],
          maxUlpsFor(op)
      );
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
      VectorTestAssertions.assertDoublesEquivalent(
          StringUtils.format("op=%s lane=%d input=%s SIMD-vs-Math", op, i, inputs[i]),
          scalar.process(inputs[i]),
          simd[i],
          maxUlpsFor(op)
      );
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

  @Test
  public void binaryDoubleDoubleStabilityAndParity()
  {
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleBinaryOperator scalar = binaryDoubleScalarFor(op);
      final double[] leftIn = inDomainLeftDoubles(op);
      final double[] rightIn = inDomainRightDoubles(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeDoubleDouble(
          fixedDoubleInput(leftIn),
          fixedDoubleInput(rightIn),
          op,
          (l, r) -> scalar.applyAsDouble(l, r)
      );

      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable("op=" + op + " shape=double,double", warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityBinary(op, "double,double", leftIn, rightIn, warmed, scalar);
    }
  }

  @Test
  public void binaryDoubleLongStabilityAndParity()
  {
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleBinaryOperator scalar = binaryDoubleScalarFor(op);
      final double[] leftIn = inDomainLeftDoubles(op);
      final long[] rightIn = inDomainRightLongs(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeDoubleLong(
          fixedDoubleInput(leftIn),
          fixedLongInput(rightIn),
          op,
          (l, r) -> scalar.applyAsDouble(l, (double) r)
      );

      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable("op=" + op + " shape=double,long", warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityBinary(op, "double,long", leftIn, longsToDoubles(rightIn), warmed, scalar);
    }
  }

  @Test
  public void binaryLongDoubleStabilityAndParity()
  {
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleBinaryOperator scalar = binaryDoubleScalarFor(op);
      final long[] leftIn = inDomainLeftLongs(op);
      final double[] rightIn = inDomainRightDoubles(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeLongDouble(
          fixedLongInput(leftIn),
          fixedDoubleInput(rightIn),
          op,
          (l, r) -> scalar.applyAsDouble((double) l, r)
      );

      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable("op=" + op + " shape=long,double", warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityBinary(op, "long,double", longsToDoubles(leftIn), rightIn, warmed, scalar);
    }
  }

  @Test
  public void binaryLongLongStabilityAndParity()
  {
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final DoubleBinaryOperator scalar = binaryDoubleScalarFor(op);
      final long[] leftIn = inDomainLeftLongs(op);
      final long[] rightIn = inDomainRightLongs(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeLongLongToDouble(
          fixedLongInput(leftIn),
          fixedLongInput(rightIn),
          op,
          (l, r) -> scalar.applyAsDouble((double) l, (double) r)
      );

      double[] warmed = null;
      for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        warmed = processor.evalVector(STUB_BINDING).values().clone();
      }

      assertStable("op=" + op + " shape=long,long", warmed, () -> processor.evalVector(STUB_BINDING).values());
      assertParityBinary(op, "long,long", longsToDoubles(leftIn), longsToDoubles(rightIn), warmed, scalar);
    }
  }

  @Test
  public void powDoubleDoubleSpecialValuesParity()
  {
    // VectorOperators.POW follows C99 and returns 1.0 for pow(1, NaN) and pow(±1, ±Infinity) where Math.pow returns
    // NaN. SimdPowSpecialCases.withNanFixup patches those lanes, so the SIMD output should match Math.pow exactly on
    // this special-value grid. ulp=0 because the Java spec pins these results deterministically.
    final double inf = Double.POSITIVE_INFINITY;
    final double nan = Double.NaN;
    // Pairs covering: C99-vs-Math divergences (±1 base × NaN/±Inf exponent, pow(1, NaN)); unaffected NaN/Inf
    // combinations that should already match (NaN base, pow(2, NaN), pow(2, Inf), pow(0.5, -Inf), pow(2, -Inf));
    // and a plain finite baseline to confirm the fixup doesn't disturb ordinary values.
    final double[] bases = {1.0, -1.0, 1.0, -1.0, 1.0, nan, 2.0, 2.0, 0.5, 2.0, inf, 3.0};
    final double[] exps =  {nan, nan, inf, inf, -inf, 2.0, nan, inf, -inf, -inf, 2.0, 2.0};
    runPowSpecialValuesParity("double,double", bases, exps);
  }

  @Test
  public void powLongDoubleSpecialValuesParity()
  {
    // Long base widens to a finite double, so we can still hit the C99-vs-Math fixup via the double-side exponent
    // being NaN or ±Infinity paired with base=±1 (and the base=2 pairings exercise the unaffected lanes).
    final double inf = Double.POSITIVE_INFINITY;
    final double nan = Double.NaN;
    final long[] bases   = {1L, -1L, 1L, -1L, 1L, -1L, 2L,  2L,  2L,  3L};
    final double[] exps =  {nan, nan, inf, inf, -inf, -inf, nan, inf, -inf, 2.0};
    runPowLongDoubleSpecialValuesParity(bases, exps);
  }

  @Test
  public void binaryDoubleDoubleTailBypassesScalarFallback()
  {
    final DoubleBivariateDoublesFunction poison = (l, r) -> {
      throw new IllegalStateException(StringUtils.format("scalar fallback called for (%s, %s)", l, r));
    };
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final double[] leftIn = inDomainLeftDoubles(op);
      final double[] rightIn = inDomainRightDoubles(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeDoubleDouble(
          fixedDoubleInput(leftIn),
          fixedDoubleInput(rightIn),
          op,
          poison
      );
      assertTailBypasses(op, "double,double", processor);
    }
  }

  @Test
  public void binaryDoubleLongTailBypassesScalarFallback()
  {
    final DoubleBivariateDoubleLongFunction poison = (l, r) -> {
      throw new IllegalStateException(StringUtils.format("scalar fallback called for (%s, %s)", l, r));
    };
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final double[] leftIn = inDomainLeftDoubles(op);
      final long[] rightIn = inDomainRightLongs(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeDoubleLong(
          fixedDoubleInput(leftIn),
          fixedLongInput(rightIn),
          op,
          poison
      );
      assertTailBypasses(op, "double,long", processor);
    }
  }

  @Test
  public void binaryLongDoubleTailBypassesScalarFallback()
  {
    final DoubleBivariateLongDoubleFunction poison = (l, r) -> {
      throw new IllegalStateException(StringUtils.format("scalar fallback called for (%s, %s)", l, r));
    };
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final long[] leftIn = inDomainLeftLongs(op);
      final double[] rightIn = inDomainRightDoubles(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeLongDouble(
          fixedLongInput(leftIn),
          fixedDoubleInput(rightIn),
          op,
          poison
      );
      assertTailBypasses(op, "long,double", processor);
    }
  }

  @Test
  public void binaryLongLongTailBypassesScalarFallback()
  {
    final DoubleBivariateLongsFunction poison = (l, r) -> {
      throw new IllegalStateException(StringUtils.format("scalar fallback called for (%s, %s)", l, r));
    };
    for (SimdSupportedBinaryOp op : SimdSupportedBinaryOp.values()) {
      if (!op.isMathLib()) {
        continue;
      }
      final long[] leftIn = inDomainLeftLongs(op);
      final long[] rightIn = inDomainRightLongs(op);
      final ExprVectorProcessor<double[]> processor = SimdProcessors.makeLongLongToDouble(
          fixedLongInput(leftIn),
          fixedLongInput(rightIn),
          op,
          poison
      );
      assertTailBypasses(op, "long,long", processor);
    }
  }

  private static void assertTailBypasses(
      SimdSupportedBinaryOp op,
      String shapeLabel,
      ExprVectorProcessor<double[]> processor
  )
  {
    for (int size : partialTailSizes()) {
      final Expr.VectorInputBinding partial = new StubBinding(INPUT_LANES, size);
      // Would throw here if the tail path invoked the poisoned scalarFallback for any lane.
      final double[] out = processor.evalVector(partial).values();
      Assertions.assertEquals(
          INPUT_LANES,
          out.length,
          "op=" + op + " shape=" + shapeLabel + " size=" + size + " unexpected output length"
      );
    }
  }

  private static void assertParityBinary(
      SimdSupportedBinaryOp op,
      String shapeLabel,
      double[] leftAsDouble,
      double[] rightAsDouble,
      double[] simd,
      DoubleBinaryOperator scalar
  )
  {
    for (int i = 0; i < leftAsDouble.length; i++) {
      VectorTestAssertions.assertDoublesEquivalent(
          StringUtils.format(
              "op=%s shape=%s lane=%d left=%s right=%s SIMD-vs-Math",
              op, shapeLabel, i, leftAsDouble[i], rightAsDouble[i]
          ),
          scalar.applyAsDouble(leftAsDouble[i], rightAsDouble[i]),
          simd[i],
          maxUlpsFor(op)
      );
    }
  }

  private static DoubleBinaryOperator binaryDoubleScalarFor(SimdSupportedBinaryOp op)
  {
    return switch (op) {
      case POW -> Math::pow;
      case ATAN2 -> Math::atan2;
      case HYPOT -> Math::hypot;
      default -> throw new IllegalStateException("not a VO_MATHLIB op: " + op);
    };
  }

  /**
   * Deterministic in-domain left operand for the given op. POW keeps the base strictly positive so
   * negative-with-fractional-exponent does not produce NaN. ATAN2 and HYPOT accept any real.
   */
  private static double[] inDomainLeftDoubles(SimdSupportedBinaryOp op)
  {
    final double[] xs = new double[INPUT_LANES];
    for (int i = 0; i < INPUT_LANES; i++) {
      // bias to [0.1, 2.1] — small positive magnitudes that won't overflow pow(x, 5)
      xs[i] = 0.1 + 2.0 * ((double) i / INPUT_LANES);
    }
    xs[0] = 1.0;
    xs[1] = 0.5;
    xs[2] = 2.0;
    xs[3] = Math.E;
    if (op != SimdSupportedBinaryOp.POW) {
      xs[4] = -0.7;
      xs[5] = -3.0;
    }
    return xs;
  }

  private static double[] inDomainRightDoubles(SimdSupportedBinaryOp op)
  {
    final double[] xs = new double[INPUT_LANES];
    for (int i = 0; i < INPUT_LANES; i++) {
      // bias to [-2, 2] for POW to keep magnitudes modest; same range works for ATAN2/HYPOT
      xs[i] = -2.0 + 4.0 * ((double) i / INPUT_LANES);
    }
    xs[0] = 0.5;
    xs[1] = 1.0;
    xs[2] = 2.0;
    xs[3] = -1.5;
    if (op != SimdSupportedBinaryOp.POW) {
      xs[4] = -0.7;
      xs[5] = Math.PI;
    }
    return xs;
  }

  private static long[] inDomainLeftLongs(SimdSupportedBinaryOp op)
  {
    final long[] xs = new long[INPUT_LANES];
    if (op == SimdSupportedBinaryOp.POW) {
      // positive bases keep POW defined for arbitrary (double-cast) exponents
      for (int i = 0; i < INPUT_LANES; i++) {
        xs[i] = 1L + (i % 10);
      }
    } else {
      for (int i = 0; i < INPUT_LANES; i++) {
        xs[i] = i - (INPUT_LANES / 2);
      }
    }
    return xs;
  }

  private static long[] inDomainRightLongs(SimdSupportedBinaryOp op)
  {
    final long[] xs = new long[INPUT_LANES];
    if (op == SimdSupportedBinaryOp.POW) {
      // keep exponents small so pow(x, y) stays within representable range
      for (int i = 0; i < INPUT_LANES; i++) {
        xs[i] = (i % 10) - 5;
      }
    } else {
      for (int i = 0; i < INPUT_LANES; i++) {
        xs[i] = i - (INPUT_LANES / 2);
      }
    }
    return xs;
  }

  private static double[] longsToDoubles(long[] xs)
  {
    final double[] out = new double[xs.length];
    for (int i = 0; i < xs.length; i++) {
      out[i] = (double) xs[i];
    }
    return out;
  }


  /**
   * Sizes chosen so that at least one leaves a partial-tail regardless of whether the runtime picks a
   * 2/4/8-lane SIMD species. Primes avoid accidentally dividing the lane count evenly.
   */
  private static int[] partialTailSizes()
  {
    return new int[]{5, 7, 11, 13, INPUT_LANES - 1};
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

  private static void runPowSpecialValuesParity(String shapeLabel, double[] bases, double[] exps)
  {
    Assertions.assertEquals(bases.length, exps.length);
    // Pad to INPUT_LANES with a no-op (1 ^ 1 = 1) so the SIMD loop takes multiple iterations per invocation.
    final double[] leftIn = new double[INPUT_LANES];
    final double[] rightIn = new double[INPUT_LANES];
    Arrays.fill(leftIn, 1.0);
    Arrays.fill(rightIn, 1.0);
    System.arraycopy(bases, 0, leftIn, 0, bases.length);
    System.arraycopy(exps, 0, rightIn, 0, exps.length);

    final ExprVectorProcessor<double[]> processor = SimdProcessors.makeDoubleDouble(
        fixedDoubleInput(leftIn),
        fixedDoubleInput(rightIn),
        SimdSupportedBinaryOp.POW,
        Math::pow
    );

    double[] warmed = null;
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      warmed = processor.evalVector(STUB_BINDING).values().clone();
    }

    for (int i = 0; i < bases.length; i++) {
      VectorTestAssertions.assertDoublesEquivalent(
          StringUtils.format(
              "op=POW shape=%s lane=%d base=%s exp=%s SIMD-vs-Math",
              shapeLabel, i, leftIn[i], rightIn[i]
          ),
          Math.pow(leftIn[i], rightIn[i]),
          warmed[i],
          0
      );
    }
  }

  private static void runPowLongDoubleSpecialValuesParity(long[] bases, double[] exps)
  {
    Assertions.assertEquals(bases.length, exps.length);
    final long[] leftIn = new long[INPUT_LANES];
    final double[] rightIn = new double[INPUT_LANES];
    Arrays.fill(leftIn, 1L);
    Arrays.fill(rightIn, 1.0);
    System.arraycopy(bases, 0, leftIn, 0, bases.length);
    System.arraycopy(exps, 0, rightIn, 0, exps.length);

    final ExprVectorProcessor<double[]> processor = SimdProcessors.makeLongDouble(
        fixedLongInput(leftIn),
        fixedDoubleInput(rightIn),
        SimdSupportedBinaryOp.POW,
        (l, r) -> Math.pow((double) l, r)
    );

    double[] warmed = null;
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      warmed = processor.evalVector(STUB_BINDING).values().clone();
    }

    for (int i = 0; i < bases.length; i++) {
      VectorTestAssertions.assertDoublesEquivalent(
          StringUtils.format(
              "op=POW shape=long,double lane=%d base=%s exp=%s SIMD-vs-Math",
              i, leftIn[i], rightIn[i]
          ),
          Math.pow((double) leftIn[i], rightIn[i]),
          warmed[i],
          0
      );
    }
  }

  @FunctionalInterface
  private interface ValuesSupplier
  {
    double[] get();
  }

  private static final class StubBinding implements Expr.VectorInputBinding
  {
    private final int maxSize;
    private final int currentSize;

    StubBinding(int size)
    {
      this(size, size);
    }

    StubBinding(int maxSize, int currentSize)
    {
      this.maxSize = maxSize;
      this.currentSize = currentSize;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentSize;
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
