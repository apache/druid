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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * This benchmark measures a slightly different thing than
 * {@link ParallelMergeCombiningSequenceBenchmark}. While this benchmark measures the
 * average time each thread takes to complete per iteration, the other measures the average time for _all_ threads to
 * complete defined by {@link ParallelMergeCombiningSequenceBenchmark#concurrentSequenceConsumers} using an
 * {@link java.util.concurrent.ExecutorService} thread pool. Additionally, the
 * {@link ParallelMergeCombiningSequenceBenchmark} benchmark is also able to introduce a delay between the concurrent
 * processing threads to simulate other more realistic patterns than a simulataneous burst of concurrent threads.
 *
 * Yes, this benchmark is sort of lame, unfortunately I could not figure out how to parameterize @Threads.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ParallelMergeCombiningSequenceJmhThreadsBenchmark extends BaseParallelMergeCombiningSequenceBenchmark
{
  @Threads(1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execOne(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(2)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execTwo(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(4)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execFour(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(8)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execEight(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(16)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execSixteen(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(24)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execTwentyFour(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(32)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execThirtyTwo(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  @Threads(64)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void execSixtyFour(Blackhole blackhole)
  {
    consumeSequence(blackhole);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ParallelMergeCombiningSequenceJmhThreadsBenchmark.class.getSimpleName())
        .forks(1)
        .syncIterations(true)
        .resultFormat(ResultFormatType.CSV)
        .result("parallel-merge-combining-sequence-threads.csv")
        .build();

    new Runner(opt).run();
  }
}
