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

package org.apache.druid.benchmark.sequences;

import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequenceTest;
import org.apache.druid.java.util.common.guava.Sequence;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 5)
@Measurement(iterations = 25)
public class ParallelMergeCombiningSequenceThreadedBenchmark extends BaseParallelMergeCombiningSequenceBenchmark
{
  @Param({
      "0",
      "100",
      "500"
  })
  int maxThreadStartDelay;

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Group("consumers")
  @GroupThreads(4)
  public void consumeSmall(Blackhole blackhole)
  {
    consumeSequence(blackhole, this::generateSmallSequence);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Group("consumers")
  @GroupThreads(1)
  public void consumeModeratelyLarge(Blackhole blackhole)
  {
    consumeSequence(blackhole, this::generateModeratelyLargeSequence);
  }

  @Override
  void consumeSequence(Blackhole blackhole, Supplier<Sequence<ParallelMergeCombiningSequenceTest.IntPair>> supplier)
  {
    int delay = maxThreadStartDelay > 0 ? ThreadLocalRandom.current().nextInt(0, maxThreadStartDelay) : 0;
    if (delay > 0) {
      try {
        Thread.sleep(delay);
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    super.consumeSequence(blackhole, supplier);
  }
}
