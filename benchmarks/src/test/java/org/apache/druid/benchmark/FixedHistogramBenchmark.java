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

import org.apache.druid.java.util.common.logger.Logger;
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
public class FixedHistogramBenchmark
{
  private static final Logger log = new Logger(FixedHistogramBenchmark.class);

  private static final int LOWER_LIMIT = 0;
  private static final int UPPER_LIMIT = 100000;

  // Number of samples
  @Param({"1000000"})
  int numEvents;

  // Number of buckets
  @Param({"10", "100", "1000", "10000", "100000"})
  int numBuckets;

  private FixedBucketsHistogram fixedHistogram;
  private FixedBucketsHistogram fixedHistogram2;
  private FixedBucketsHistogram fixedHistogram3;
  private FixedBucketsHistogram fixedHistogramForSparseLower;
  private FixedBucketsHistogram fixedHistogramForSparseUpper;

  private int[] randomValues;

  private byte[] fixedFullSerializedAlready;
  private byte[] fixedSparseLowerSerialized;
  private byte[] fixedSparseUpperSerialized;

  private double[] percentilesForFixed = new double[]{12.5, 25, 50, 75, 98};

  @Setup
  public void setup()
  {
    fixedHistogram = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    fixedHistogram2 = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        (int) Math.round(numBuckets * 1.5),
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    fixedHistogram3 = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    fixedHistogramForSparseLower = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    fixedHistogramForSparseUpper = new FixedBucketsHistogram(
        LOWER_LIMIT,
        UPPER_LIMIT,
        numBuckets,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    randomValues = new int[numEvents];
    Random r = ThreadLocalRandom.current();
    for (int i = 0; i < numEvents; i++) {
      randomValues[i] = r.nextInt(UPPER_LIMIT);
      fixedHistogram.add(randomValues[i]);
      fixedHistogram2.add(randomValues[i]);
      fixedHistogram3.add(randomValues[i]);

      if (randomValues[i] < UPPER_LIMIT * 0.4) {
        fixedHistogramForSparseLower.add(randomValues[i]);
      }


      if (randomValues[i] > UPPER_LIMIT * 0.6) {
        fixedHistogramForSparseUpper.add(randomValues[i]);
      }
    }

    fixedFullSerializedAlready = fixedHistogram.toBytesFull(true);
    fixedSparseLowerSerialized = fixedHistogramForSparseLower.toBytesSparse(fixedHistogram.getNonEmptyBucketCount());
    fixedSparseUpperSerialized = fixedHistogramForSparseUpper.toBytesSparse(fixedHistogram.getNonEmptyBucketCount());
  }

  @Benchmark
  public void mergeFixedDifferentBuckets(Blackhole bh)
  {
    FixedBucketsHistogram copy = fixedHistogram.getCopy();
    copy.combineHistogram(fixedHistogram2);
    bh.consume(copy);
  }

  @Benchmark
  public void mergeFixedSameBuckets(Blackhole bh)
  {
    FixedBucketsHistogram copy = fixedHistogram.getCopy();
    copy.combineHistogram(fixedHistogram3);
    bh.consume(copy);
  }

  @Benchmark
  public void getPercentilesFixed(Blackhole bh)
  {
    float[] percentiles = fixedHistogram.percentilesFloat(percentilesForFixed);
    bh.consume(percentiles);
  }

  @Benchmark
  public void serializeFixedSparseLower(Blackhole bh)
  {
    byte[] sparseSerialized = fixedHistogramForSparseLower.toBytesSparse(fixedHistogramForSparseUpper.getNonEmptyBucketCount());
    bh.consume(sparseSerialized);
  }

  @Benchmark
  public void deserializeFixedSparseLower(Blackhole bh)
  {
    FixedBucketsHistogram fixedBucketsHistogram = FixedBucketsHistogram.fromBytes(fixedSparseLowerSerialized);
    bh.consume(fixedBucketsHistogram);
  }

  @Benchmark
  public void serializeFixedSparseUpper(Blackhole bh)
  {
    byte[] sparseSerialized = fixedHistogramForSparseUpper.toBytesSparse(fixedHistogramForSparseUpper.getNonEmptyBucketCount());
    bh.consume(sparseSerialized);
  }

  @Benchmark
  public void deserializeFixedSparseUpper(Blackhole bh)
  {
    FixedBucketsHistogram fixedBucketsHistogram = FixedBucketsHistogram.fromBytes(fixedSparseUpperSerialized);
    bh.consume(fixedBucketsHistogram);
  }

  @Benchmark
  public void serializeFixedFull(Blackhole bh)
  {
    byte[] fullSerialized = fixedHistogram.toBytesFull(true);
    bh.consume(fullSerialized);
  }

  @Benchmark
  public void deserializeFixedFull(Blackhole bh)
  {
    FixedBucketsHistogram fixedBucketsHistogram = FixedBucketsHistogram.fromBytes(fixedFullSerializedAlready);
    bh.consume(fixedBucketsHistogram);
  }
}
