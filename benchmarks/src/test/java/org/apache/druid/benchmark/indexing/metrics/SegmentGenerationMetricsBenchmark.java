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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for SegmentGenerationMetrics
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SegmentGenerationMetricsBenchmark
{
  private static final int NUM_ITERATIONS = 10_000;
  private long[] samples;
  private SegmentGenerationMetrics metrics;

  @Setup(Level.Iteration)
  public void setup()
  {
    metrics = new SegmentGenerationMetrics();
    samples = new long[NUM_ITERATIONS];

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      samples[i] = random.nextLong(1, Long.MAX_VALUE);
    }
  }

  /**
   * Benchmark for reportMessageGap in hot loop.
   */
  @Benchmark
  public void benchmarkMultipleReportMessageGap()
  {
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      metrics.reportMessageGap(samples[i]);
    }
  }

  /**
   * Benchmark for reportMaxSegmentHandoffTime in hot loop.
   */
  @Benchmark
  public void benchmarkMultipleReportMaxSegmentHandoffTime()
  {
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      metrics.reportMaxSegmentHandoffTime(samples[i]);
    }
  }

  /**
   * Benchmark for reportMessageMaxTimestamp in hot loop.
   */
  @Benchmark
  public void benchmarkMultipleReportMessageMaxTimestamp()
  {
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      metrics.reportMessageMaxTimestamp(samples[i]);
    }
  }
}
