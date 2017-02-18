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

package io.druid.benchmark;

import io.druid.segment.data.GenericIndexed;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(GenericIndexedBenchmark.N)
@Warmup(iterations = 5)
@Measurement(iterations = 20)
@Fork(1)
public class GenericIndexedBenchmark
{
  static final int N = 10000;
  private static final GenericIndexed<String> genericIndexed;
  static {
    String[] strings = new String[N];
    for (int i = 0; i < N; i++) {
      strings[i] = String.valueOf(i);
    }
    genericIndexed = GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY);
  }

  @Benchmark
  public void get(Blackhole bh)
  {
    for (int i = 0; i < N; i++) {
      bh.consume(genericIndexed.get(i));
    }
  }
}
