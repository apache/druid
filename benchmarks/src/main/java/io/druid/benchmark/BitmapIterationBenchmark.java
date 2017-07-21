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

import io.druid.collections.bitmap.BitSetBitmapFactory;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
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
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * Benchmarks of bitmap iteration and iteration + something (cumulative cost), the latter is useful for comparing total
 * "usage cost" of different {@link io.druid.segment.data.BitmapSerdeFactory}.
 *
 * @see #iter(IterState)
 * @see #constructAndIter(ConstructAndIterState)
 * @see #intersectionAndIter(BitmapsForIntersection)
 * @see #unionAndIter(BitmapsForUnion)
 */
@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BitmapIterationBenchmark
{
  @Param({"bitset", "concise", "roaring"})
  public String bitmapAlgo;

  /**
   * Fraction of set bits in the bitmaps to iterate. For {@link #intersectionAndIter} and
   * {@link #unionAndIter}, this is the fraction of set bits in the final result of intersection or union.
   */
  @Param({"0.0", "0.001", "0.1", "0.5", "0.99", "1.0"})
  public double prob;

  /**
   * The size of all bitmaps, i. e. the number of rows in a segment for the most bitmap use cases.
   */
  @Param({"1000000"})
  public int size;

  private BitmapFactory makeFactory()
  {
    switch (bitmapAlgo) {
      case "bitset":
        return new BitSetBitmapFactory();
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

  private ImmutableBitmap makeBitmap(double prob)
  {
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    Random random = ThreadLocalRandom.current();
    for (int bit = 0; bit < size; bit++) {
      if (random.nextDouble() < prob) {
        mutableBitmap.add(bit);
      }
    }
    return factory.makeImmutableBitmap(mutableBitmap);
  }

  @State(Scope.Benchmark)
  public static class IterState
  {
    private ImmutableBitmap bitmap;

    @Setup
    public void setup(BitmapIterationBenchmark state)
    {
      bitmap = state.makeBitmap(state.prob);
    }
  }

  /**
   * General benchmark of bitmap iteration, this is a part of {@link io.druid.segment.IndexMerger#merge} and
   * query processing on both realtime and historical nodes.
   */
  @Benchmark
  public int iter(IterState state)
  {
    ImmutableBitmap bitmap = state.bitmap;
    return iter(bitmap);
  }

  private static int iter(ImmutableBitmap bitmap)
  {
    int consume = 0;
    for (IntIterator it = bitmap.iterator(); it.hasNext();) {
      consume ^= it.next();
    }
    return consume;
  }

  @State(Scope.Benchmark)
  public static class ConstructAndIterState
  {
    private int dataSize;
    private int[] data;

    @Setup
    public void setup(BitmapIterationBenchmark state)
    {
      data = new int[(int) (state.size * state.prob) * 2];
      dataSize = 0;
      Random random = ThreadLocalRandom.current();
      for (int bit = 0; bit < state.size; bit++) {
        if (random.nextDouble() < state.prob) {
          data[dataSize] = bit;
          dataSize++;
        }
      }
    }
  }

  /**
   * Benchmark of cumulative cost of construction of an immutable bitmap and then iterating over it. This is a pattern
   * from realtime nodes, see {@link io.druid.segment.StringDimensionIndexer#fillBitmapsFromUnsortedEncodedKeyComponent}.
   * However this benchmark is yet approximate and to be improved to better reflect actual workloads of realtime nodes.
   */
  @Benchmark
  public int constructAndIter(ConstructAndIterState state)
  {
    int dataSize = state.dataSize;
    int[] data = state.data;
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < dataSize; i++) {
      mutableBitmap.add(data[i]);
    }
    ImmutableBitmap bitmap = factory.makeImmutableBitmap(mutableBitmap);
    return iter(bitmap);
  }

  @State(Scope.Benchmark)
  public static class BitmapsForIntersection
  {
    /**
     * Number of bitmaps to intersect.
     */
    @Param({"2", "10", "100"})
    public int n;

    private ImmutableBitmap[] bitmaps;

    @Setup
    public void setup(BitmapIterationBenchmark state)
    {
      // prob of intersection = product (probs of intersected bitmaps), prob = intersectedBitmapProb ^ n
      double intersectedBitmapProb = Math.pow(state.prob, 1.0 / n);
      bitmaps = new ImmutableBitmap[n];
      for (int i = 0; i < n; i++) {
        bitmaps[i] = state.makeBitmap(intersectedBitmapProb);
      }
    }
  }

  /**
   * Benchmark of cumulative cost of bitmap intersection with subsequent iteration over the result. This is a pattern
   * from query processing of historical nodes, when {@link io.druid.segment.filter.AndFilter} is used.
   */
  @Benchmark
  public int intersectionAndIter(BitmapsForIntersection state)
  {
    ImmutableBitmap intersection = factory.intersection(Arrays.asList(state.bitmaps));
    return iter(intersection);
  }

  @State(Scope.Benchmark)
  public static class BitmapsForUnion
  {
    /**
     * Number of bitmaps to union.
     */
    @Param({"2", "10", "100"})
    public int n;

    private ImmutableBitmap[] bitmaps;

    @Setup
    public void setup(BitmapIterationBenchmark state)
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

  /**
   * Benchmark of cumulative cost of bitmap union with subsequent iteration over the result. This is a pattern from
   * query processing on historical nodes, when filters like {@link io.druid.segment.filter.DimensionPredicateFilter},
   * {@link io.druid.query.filter.RegexDimFilter}, {@link io.druid.query.filter.SearchQueryDimFilter} and similar are
   * used.
   */
  @Benchmark
  public int unionAndIter(BitmapsForUnion state)
  {
    ImmutableBitmap intersection = factory.union(Arrays.asList(state.bitmaps));
    return iter(intersection);
  }

  /**
   * This main() is for debugging from the IDE.
   */
  public static void main(String[] args)
  {
    BitmapIterationBenchmark state = new BitmapIterationBenchmark();
    state.bitmapAlgo = "concise";
    state.prob = 0.001;
    state.size = 1000000;
    state.setup();

    BitmapsForIntersection state2 = new BitmapsForIntersection();
    state2.setup(state);
    state.intersectionAndIter(state2);
  }
}
