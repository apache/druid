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

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.mean.DoubleMeanHolder;
import org.apache.druid.query.aggregation.mean.DoubleMeanVectorAggregator;
import org.apache.druid.query.aggregation.simd.SimdDoubleMeanVectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "--add-modules=jdk.incubator.vector")
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DoubleMeanVectorAggregatorBenchmark
{
  @Param({"128", "512", "1024", "4096"})
  private int vectorSize;

  @Param({"none", "sparse", "alternating"})
  private String nullPattern;

  private ByteBuffer scalarBuffer;
  private ByteBuffer simdBuffer;
  private VectorAggregator scalarAggregator;
  private VectorAggregator simdAggregator;

  @Setup(Level.Trial)
  public void setup()
  {
    final double[] values = new double[vectorSize];
    final Random random = new Random(0xC0FFEEL);
    for (int i = 0; i < vectorSize; i++) {
      values[i] = (random.nextDouble() - 0.5) * 1000.0;
    }

    final boolean[] nulls = makeNullVector();
    final VectorValueSelector selector = new FakeVectorValueSelector(vectorSize, values, nulls);
    scalarAggregator = new DoubleMeanVectorAggregator(selector);
    simdAggregator = new SimdDoubleMeanVectorAggregator(selector);
    scalarBuffer = ByteBuffer.allocate(DoubleMeanHolder.MAX_INTERMEDIATE_SIZE);
    simdBuffer = ByteBuffer.allocate(DoubleMeanHolder.MAX_INTERMEDIATE_SIZE);
  }

  @Setup(Level.Invocation)
  public void setupInvocation()
  {
    scalarAggregator.init(scalarBuffer, 0);
    simdAggregator.init(simdBuffer, 0);
  }

  @Benchmark
  public void scalarDoubleMean(final Blackhole blackhole)
  {
    scalarAggregator.aggregate(scalarBuffer, 0, 0, vectorSize);
    blackhole.consume(scalarBuffer.getDouble(0));
    blackhole.consume(scalarBuffer.getLong(Double.BYTES));
  }

  @Benchmark
  public void simdDoubleMean(final Blackhole blackhole)
  {
    simdAggregator.aggregate(simdBuffer, 0, 0, vectorSize);
    blackhole.consume(simdBuffer.getDouble(0));
    blackhole.consume(simdBuffer.getLong(Double.BYTES));
  }

  @Nullable
  private boolean[] makeNullVector()
  {
    return switch (nullPattern) {
      case "none" -> null;
      case "sparse" -> {
        final boolean[] nulls = new boolean[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
          nulls[i] = i % 17 == 0;
        }
        yield nulls;
      }
      case "alternating" -> {
        final boolean[] nulls = new boolean[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
          nulls[i] = (i & 1) == 0;
        }
        yield nulls;
      }
      default -> throw new IllegalStateException("Unsupported null pattern[" + nullPattern + "]");
    };
  }

  private static final class FakeVectorValueSelector implements VectorValueSelector
  {
    private final int size;
    private final double[] doubles;
    @Nullable
    private final boolean[] nulls;

    FakeVectorValueSelector(final int size, final double[] doubles, @Nullable final boolean[] nulls)
    {
      this.size = size;
      this.doubles = doubles;
      this.nulls = nulls;
    }

    @Override
    public long[] getLongVector()
    {
      return null;
    }

    @Override
    public float[] getFloatVector()
    {
      return null;
    }

    @Override
    public double[] getDoubleVector()
    {
      return doubles;
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return nulls;
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
  }
}
