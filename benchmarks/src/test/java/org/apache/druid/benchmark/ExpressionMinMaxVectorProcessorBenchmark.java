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

package org.apache.druid.benchmark;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.DoubleBivariateDoubleLongFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleBivariateDoublesFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.DoubleBivariateLongDoubleFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.ExprEvalDoubleVector;
import org.apache.druid.math.expr.vector.ExprEvalLongVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.LongBivariateLongsFunctionVectorProcessor;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoubleLongFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateDoublesFunction;
import org.apache.druid.math.expr.vector.functional.DoubleBivariateLongDoubleFunction;
import org.apache.druid.math.expr.vector.functional.LongBivariateLongsFunction;
import org.apache.druid.math.expr.vector.simd.SimdDoubleDoubleMaxProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleDoubleMinProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleLongMaxProcessor;
import org.apache.druid.math.expr.vector.simd.SimdDoubleLongMinProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongDoubleMaxProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongDoubleMinProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongLongMaxProcessor;
import org.apache.druid.math.expr.vector.simd.SimdLongLongMinProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "--add-modules=jdk.incubator.vector")
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ExpressionMinMaxVectorProcessorBenchmark
{
  @Param({"128", "512", "1024", "4096"})
  private int vectorSize;

  @Param({"none", "sparse", "alternating"})
  private String nullPattern;

  @Param({"min", "max"})
  private String operation;

  @Param({"longLong", "doubleDouble", "longDouble", "doubleLong"})
  private String inputTypes;

  private Expr.VectorInputBinding bindings;
  private ExprVectorProcessor<?> vectorProcessor;
  private ExprVectorProcessor<?> simdProcessor;

  @Setup
  public void setup()
  {
    final Random random = new Random(0xC0FFEEL);
    final long[] leftLongs = new long[vectorSize];
    final long[] rightLongs = new long[vectorSize];
    final double[] leftDoubles = new double[vectorSize];
    final double[] rightDoubles = new double[vectorSize];
    for (int i = 0; i < vectorSize; i++) {
      leftLongs[i] = random.nextLong(-1_000_000L, 1_000_000L);
      rightLongs[i] = random.nextLong(-1_000_000L, 1_000_000L);
      leftDoubles[i] = (random.nextDouble() - 0.5) * 1_000_000.0;
      rightDoubles[i] = (random.nextDouble() - 0.5) * 1_000_000.0;
    }

    final boolean[] leftNulls = makeNullVector(true);
    final boolean[] rightNulls = makeNullVector(false);
    bindings = new FakeVectorInputBinding(vectorSize);

    switch (inputTypes) {
      case "longLong":
        setupLongLong(leftLongs, rightLongs, leftNulls, rightNulls);
        break;
      case "doubleDouble":
        setupDoubleDouble(leftDoubles, rightDoubles, leftNulls, rightNulls);
        break;
      case "longDouble":
        setupLongDouble(leftLongs, rightDoubles, leftNulls, rightNulls);
        break;
      case "doubleLong":
        setupDoubleLong(leftDoubles, rightLongs, leftNulls, rightNulls);
        break;
      default:
        throw new IllegalStateException("Unsupported input types[" + inputTypes + "]");
    }
  }

  @Benchmark
  public void vectorMinMax(final Blackhole blackhole)
  {
    final ExprEvalVector<?> result = vectorProcessor.evalVector(bindings);
    blackhole.consume(result.values());
    blackhole.consume(result.getNullVector());
  }

  @Benchmark
  public void simdMinMax(final Blackhole blackhole)
  {
    final ExprEvalVector<?> result = simdProcessor.evalVector(bindings);
    blackhole.consume(result.values());
    blackhole.consume(result.getNullVector());
  }

  private void setupLongLong(
      final long[] left,
      final long[] right,
      @Nullable final boolean[] leftNulls,
      @Nullable final boolean[] rightNulls
  )
  {
    final LongBivariateLongsFunction function = "max".equals(operation) ? Math::max : Math::min;
    final ExprVectorProcessor<long[]> leftProcessor = new FakeLongVectorProcessor(left, leftNulls);
    final ExprVectorProcessor<long[]> rightProcessor = new FakeLongVectorProcessor(right, rightNulls);
    vectorProcessor = new LongBivariateLongsFunctionVectorProcessor(leftProcessor, rightProcessor, function);
    simdProcessor = "max".equals(operation)
                    ? new SimdLongLongMaxProcessor(leftProcessor, rightProcessor, function)
                    : new SimdLongLongMinProcessor(leftProcessor, rightProcessor, function);
  }

  private void setupDoubleDouble(
      final double[] left,
      final double[] right,
      @Nullable final boolean[] leftNulls,
      @Nullable final boolean[] rightNulls
  )
  {
    final DoubleBivariateDoublesFunction function = "max".equals(operation) ? Math::max : Math::min;
    final ExprVectorProcessor<double[]> leftProcessor = new FakeDoubleVectorProcessor(left, leftNulls);
    final ExprVectorProcessor<double[]> rightProcessor = new FakeDoubleVectorProcessor(right, rightNulls);
    vectorProcessor = new DoubleBivariateDoublesFunctionVectorProcessor(leftProcessor, rightProcessor, function);
    simdProcessor = "max".equals(operation)
                    ? new SimdDoubleDoubleMaxProcessor(leftProcessor, rightProcessor, function)
                    : new SimdDoubleDoubleMinProcessor(leftProcessor, rightProcessor, function);
  }

  private void setupLongDouble(
      final long[] left,
      final double[] right,
      @Nullable final boolean[] leftNulls,
      @Nullable final boolean[] rightNulls
  )
  {
    final DoubleBivariateLongDoubleFunction function = "max".equals(operation)
                                                       ? (leftValue, rightValue) -> Math.max(leftValue, rightValue)
                                                       : (leftValue, rightValue) -> Math.min(leftValue, rightValue);
    final ExprVectorProcessor<long[]> leftProcessor = new FakeLongVectorProcessor(left, leftNulls);
    final ExprVectorProcessor<double[]> rightProcessor = new FakeDoubleVectorProcessor(right, rightNulls);
    vectorProcessor = new DoubleBivariateLongDoubleFunctionVectorProcessor(leftProcessor, rightProcessor, function);
    simdProcessor = "max".equals(operation)
                    ? new SimdLongDoubleMaxProcessor(leftProcessor, rightProcessor, function)
                    : new SimdLongDoubleMinProcessor(leftProcessor, rightProcessor, function);
  }

  private void setupDoubleLong(
      final double[] left,
      final long[] right,
      @Nullable final boolean[] leftNulls,
      @Nullable final boolean[] rightNulls
  )
  {
    final DoubleBivariateDoubleLongFunction function = "max".equals(operation)
                                                       ? (leftValue, rightValue) -> Math.max(leftValue, rightValue)
                                                       : (leftValue, rightValue) -> Math.min(leftValue, rightValue);
    final ExprVectorProcessor<double[]> leftProcessor = new FakeDoubleVectorProcessor(left, leftNulls);
    final ExprVectorProcessor<long[]> rightProcessor = new FakeLongVectorProcessor(right, rightNulls);
    vectorProcessor = new DoubleBivariateDoubleLongFunctionVectorProcessor(leftProcessor, rightProcessor, function);
    simdProcessor = "max".equals(operation)
                    ? new SimdDoubleLongMaxProcessor(leftProcessor, rightProcessor, function)
                    : new SimdDoubleLongMinProcessor(leftProcessor, rightProcessor, function);
  }

  @Nullable
  private boolean[] makeNullVector(final boolean left)
  {
    return switch (nullPattern) {
      case "none" -> null;
      case "sparse" -> {
        final boolean[] nulls = new boolean[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
          nulls[i] = left ? i % 17 == 0 : i % 19 == 0;
        }
        yield nulls;
      }
      case "alternating" -> {
        final boolean[] nulls = new boolean[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
          nulls[i] = left ? (i & 1) == 0 : i % 5 == 0;
        }
        yield nulls;
      }
      default -> throw new IllegalStateException("Unsupported null pattern[" + nullPattern + "]");
    };
  }

  private static final class FakeLongVectorProcessor implements ExprVectorProcessor<long[]>
  {
    private final ExprEvalLongVector eval;
    private final int size;

    FakeLongVectorProcessor(final long[] values, @Nullable final boolean[] nulls)
    {
      this.eval = new ExprEvalLongVector(values, nulls);
      this.size = values.length;
    }

    @Override
    public ExprEvalVector<long[]> evalVector(final Expr.VectorInputBinding bindings)
    {
      return eval;
    }

    @Override
    public ExpressionType getOutputType()
    {
      return ExpressionType.LONG;
    }

    @Override
    public int maxVectorSize()
    {
      return size;
    }
  }

  private static final class FakeDoubleVectorProcessor implements ExprVectorProcessor<double[]>
  {
    private final ExprEvalDoubleVector eval;
    private final int size;

    FakeDoubleVectorProcessor(final double[] values, @Nullable final boolean[] nulls)
    {
      this.eval = new ExprEvalDoubleVector(values, nulls);
      this.size = values.length;
    }

    @Override
    public ExprEvalVector<double[]> evalVector(final Expr.VectorInputBinding bindings)
    {
      return eval;
    }

    @Override
    public ExpressionType getOutputType()
    {
      return ExpressionType.DOUBLE;
    }

    @Override
    public int maxVectorSize()
    {
      return size;
    }
  }

  private static final class FakeVectorInputBinding implements Expr.VectorInputBinding
  {
    private final int size;

    FakeVectorInputBinding(final int size)
    {
      this.size = size;
    }

    @Nullable
    @Override
    public ExpressionType getType(final String name)
    {
      return null;
    }

    @Override
    public int getMaxVectorSize()
    {
      return size;
    }

    @Override
    public Object[] getObjectVector(final String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongVector(final String name)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleVector(final String name)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public boolean[] getNullVector(final String name)
    {
      throw new UnsupportedOperationException();
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
  }
}
