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

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RangeBitmapBenchmark
{
  private static final Random RANDOM = new Random(0);

  @Param({"concise", "roaring"})
  private String type;

  @Param("10000")
  private int numBitmaps;

  @Param("500000")
  private int bitmapLength;

  @Param("0.001")
  private double density;

  @Param("50")
  private int minIntersect;

  private List<ImmutableBitmap> bitmaps;
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

    for (int i = 0; i < numBitmaps; ++i) {
      final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();

      int k = 0;
      boolean fill = true;
      while (k < bitmapLength) {
        int runLength = (int) (bitmapLength * density) + RANDOM.nextInt((int) (bitmapLength * density));
        for (int j = k; fill && j < bitmapLength && j < k + runLength; ++j) {
          mutableBitmap.add(j);
        }
        k += runLength;
        fill = !fill;
      }

      for (k = bitmapLength / 2; k < bitmapLength / 2 + minIntersect; ++k) {
        mutableBitmap.add(k);
      }

      bitmaps.add(BitmapBenchmarkUtils.toOffheap(bitmapFactory.makeImmutableBitmap(mutableBitmap)));
    }

    final long totalSizeBytes = bitmaps.stream().mapToLong(bitmap -> bitmap.toBytes().length).sum();
    BitmapBenchmarkUtils.printSizeStats(type, density, bitmaps.size(), totalSizeBytes);
  }

  @Benchmark
  public void union(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.union(bitmaps));
  }

  @Benchmark
  public void intersection(Blackhole blackhole)
  {
    blackhole.consume(bitmapFactory.intersection(bitmaps));
  }
}
