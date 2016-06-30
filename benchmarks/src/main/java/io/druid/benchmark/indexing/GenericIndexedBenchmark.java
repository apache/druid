/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.indexing;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.druid.segment.data.GenericIndexed;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class GenericIndexedBenchmark
{

  int[] ints = new int[0x10000];

  @Setup
  public void setup()
  {
    for (int i = 0; i < 0x10000; i++) {
      ints[i] = i;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmarkFromIterable(Blackhole blackhole)
  {
    blackhole.consume(
        GenericIndexed.fromIterable(
            Iterables.transform(
                Ints.asList(ints),
                new Function<Integer, String>()
                {
                  @Override
                  public String apply(Integer input)
                  {
                    return input.toString();
                  }
                }
            ),
            GenericIndexed.STRING_STRATEGY,
            (byte) 0x1
        )
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmarkFromIterableV2(Blackhole blackhole)
  {
    blackhole.consume(
        GenericIndexed.fromIterable(
            Iterables.transform(
                Ints.asList(ints),
                new Function<Integer, String>()
                {
                  @Override
                  public String apply(Integer input)
                  {
                    return input.toString();
                  }
                }
            ),
            GenericIndexed.STRING_STRATEGY,
            (byte) 0x2
        )
    );
  }

  /**
   * For running the benchmarks directly using IDE
   */
  public static void main(String[] args) throws IOException
  {
    Options opt = new OptionsBuilder()
        .include(GenericIndexedBenchmark.class.getSimpleName())
        .warmupIterations(10)
        .measurementIterations(10)
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MICROSECONDS)
        .forks(1)
        .build();

    try {
      new Runner(opt).run();
    }
    catch (RunnerException e) {
      e.printStackTrace();
    }
  }
}
