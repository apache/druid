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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class FixedHistogramAddBenchmark
{
  private static final int LOWER_LIMIT = 0;
  private static final int UPPER_LIMIT = 100000;

  // Number of samples
  @Param({"100000", "1000000"})
  int numEvents;

  // Number of buckets
  @Param({"10", "100", "1000", "10000", "100000"})
  int numBuckets;

  private FixedBucketsHistogram fixedHistogramForAdds;
  private int[] randomValues;

  private float[] normalDistributionValues;

  @Setup
  public void setup()
  {
    randomValues = new int[numEvents];
    Random r = ThreadLocalRandom.current();
    for (int i = 0; i < numEvents; i++) {
      randomValues[i] = r.nextInt(UPPER_LIMIT);
    }

    fixedHistogramForAdds = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    NormalDistribution normalDistribution = new NormalDistribution(50000, 10000);
    normalDistributionValues = new float[numEvents];
    for (int i = 0; i < numEvents; i++) {
      normalDistributionValues[i] = (float) normalDistribution.sample();
    }
  }

  @Benchmark
  public void addFixedHisto(Blackhole bh)
  {
    fixedHistogramForAdds = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    for (int i = 0; i < numEvents; i++) {
      fixedHistogramForAdds.add(randomValues[i]);
    }
    bh.consume(fixedHistogramForAdds);
  }

  @Benchmark
  public void addFixedHistoNormal(Blackhole bh)
  {
    fixedHistogramForAdds = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    for (int i = 0; i < numEvents; i++) {
      fixedHistogramForAdds.add(normalDistributionValues[i]);
    }
    bh.consume(fixedHistogramForAdds);
  }
}
