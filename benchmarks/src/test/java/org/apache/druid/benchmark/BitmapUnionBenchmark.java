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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * Benchmarks of bitmap union copied from {@link BitmapIterationBenchmark}.
 *
 * @see #union(BitmapsForUnion, Blackhole)
 */
@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BitmapUnionBenchmark
{
  @Param({"concise", "roaring"})
  public String bitmapAlgo;

  /**
   * Fraction of set bits in the bitmaps to iterate. For {@link #union(BitmapsForUnion, Blackhole)} and
   * {@link #union(BitmapsForUnion, Blackhole)}, this is the fraction of set bits in the final result of intersection or union.
   */
  @Param({"0.0", "0.001", "0.1", "0.5", "0.99", "1.0"})
  public double prob;

  /**
   * The size of all bitmaps, i. e. the number of rows in a segment for the most bitmap use cases.
   */
  @Param({"1000000", "5000000", "10000000"})
  public int size;

  private BitmapFactory makeFactory()
  {
    switch (bitmapAlgo) {
      case "concise":
        return new ConciseBitmapFactory();
      case "roaring":
        return new RoaringBitmapFactory();
      default:
        throw new IllegalStateException();
    }
  }

  private BitmapFactory factory;

  @Setup
  public void setup()
  {
    factory = makeFactory();
  }

  @State(Scope.Benchmark)
  public static class BitmapsForUnion
  {
    /**
     * Number of bitmaps to union.
     */
    @Param({"10", "30", "100", "1000", "10000", "100000"})
    public int n;

    private ImmutableBitmap[] bitmaps;

    @Setup
    public void setup(BitmapUnionBenchmark state)
    {
      double prob = Math.pow(state.prob, 1.0 / n);
      MutableBitmap[] mutableBitmaps = new MutableBitmap[n];
      for (int i = 0; i < n; i++) {
        mutableBitmaps[i] = state.factory.makeEmptyMutableBitmap();
      }
      Random r = ThreadLocalRandom.current();
      for (int i = 0; i < state.size; i++) {
        // unions are usually search/filter/select of multiple values of one dimension, so making bitmaps disjoint will
        // make benchmarks closer to actual workloads
        MutableBitmap bitmap = mutableBitmaps[r.nextInt(n)];
        // In one selected bitmap, set the bit with probability=prob, to have the same fraction of set bit in the union
        if (r.nextDouble() < prob) {
          bitmap.add(i);
        }
      }
      bitmaps = new ImmutableBitmap[n];
      for (int i = 0; i < n; i++) {
        bitmaps[i] = state.factory.makeImmutableBitmap(mutableBitmaps[i]);
      }
    }
  }

  @Benchmark
  public void union(BitmapsForUnion state, Blackhole blackhole)
  {
    ImmutableBitmap union = factory.union(Arrays.asList(state.bitmaps));
    blackhole.consume(union);
  }

}
