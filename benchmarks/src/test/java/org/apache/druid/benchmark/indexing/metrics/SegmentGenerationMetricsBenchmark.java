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

package org.apache.druid.benchmark.indexing.metrics;

import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for SegmentGenerationMetrics focusing on reportMessageGap and reportMaxSegmentHandoffTime methods.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SegmentGenerationMetricsBenchmark
{
  private SegmentGenerationMetrics metrics;

  @Param({"true", "false"})
  private boolean enableMessageGapMetrics;

  // Pre-generated values for benchmarks
  private long messageGapValue;
  private long handoffTimeValue;

  @Setup(Level.Iteration)
  public void setup()
  {
    metrics = new SegmentGenerationMetrics(enableMessageGapMetrics);

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    messageGapValue = random.nextLong(1, 10000);
    handoffTimeValue = random.nextLong(1, 5000);
  }

  /**
   * Benchmark for the reportMessageGap in hot loop.
   */
  @Benchmark
  public void benchmarkMultipleReportMessageGap()
  {
    metrics.reportMessageGap(messageGapValue);
  }

  /**
   * Benchmark for reportMaxSegmentHandoffTime in hot loop.
   */
  @Benchmark
  public void benchmarkMultipleReportMaxSegmentHandoffTime()
  {
    metrics.reportMaxSegmentHandoffTime(handoffTimeValue);
  }


  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(SegmentGenerationMetricsBenchmark.class.getSimpleName())
        .forks(1)
        .warmupIterations(1)
        .warmupTime(TimeValue.seconds(1))
        .measurementIterations(2)
        .measurementTime(TimeValue.seconds(2))
        .build();
    new Runner(opt).run();
  }
}
