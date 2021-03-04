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

package org.apache.druid.benchmark.bitmap;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BitmapBenchmarkWithVaryingOrder
{
  private static final Random RANDOM = new Random(0);

  @Param({"roaring", "concise"})
  private String type;

  @Param("10000")
  private int numBitmaps;

  @Param("500000")
  private int bitmapLength;

  @Param("50")
  private int minIntersect;

  // sorted by an order of increasing density
  private List<ImmutableBitmap> bitmaps;
  // sorted by an order of decreasing density
  private List<ImmutableBitmap> reverseBitmaps;
  private BitmapFactory bitmapFactory;

  static {
    NullHandling.initializeForTests();
  }

  @Setup(Level.Trial)
  public void setup() throws IOException
  {
    switch (type) {
      case "concise":
        bitmapFactory = new ConciseBitmapFactory();
        break;
      case "roaring":
        bitmapFactory = new RoaringBitmapFactory();
        break;
      default:
        throw new IAE("Unknown bitmap type[%s]", type);
    }
    bitmaps = new ArrayList<>(numBitmaps);

    // Bitmaps usually have a short circuit to early return an empty bitmap if it finds no intersection
    // during an AND operation. We want to let them iterate all bitmaps instead, so add some bits that
    // will be set for all bitmaps we create.
    final int[] knownTrue = new int[minIntersect];
    for (int i = 0; i < knownTrue.length; ++i) {
      knownTrue[i] = RANDOM.nextInt(bitmapLength);
    }
    for (int i = 0; i < numBitmaps; ++i) {
      final int bitCount = (int) (i * 0.1); // the later the bitmap is created, the higher its density is.
      IntSet ints = new IntOpenHashSet(bitCount);
      for (int j = 0; j < bitCount; j++) {
        int offset;
        do {
          offset = RANDOM.nextInt(bitmapLength);
        } while (ints.contains(offset));
        ints.add(offset);
      }
      final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
      ints.iterator().forEachRemaining((IntConsumer) mutableBitmap::add);
      for (int k : knownTrue) {
        mutableBitmap.add(k);
      }

      bitmaps.add(BitmapBenchmarkUtils.toOffheap(bitmapFactory.makeImmutableBitmap(mutableBitmap)));
    }

    reverseBitmaps = Lists.reverse(bitmaps);
  }

  @Benchmark
  public void union(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.union(bitmaps));
  }

  @Benchmark
  public void unionReverse(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.union(reverseBitmaps));
  }

  @Benchmark
  public void intersection(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.intersection(bitmaps));
  }

  @Benchmark
  public void intersectionReverse(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.intersection(reverseBitmaps));
  }
}
