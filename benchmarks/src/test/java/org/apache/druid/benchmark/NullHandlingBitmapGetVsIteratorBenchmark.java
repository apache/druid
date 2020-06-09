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

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.extendedset.intset.ConciseSet;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.SECONDS)
public class NullHandlingBitmapGetVsIteratorBenchmark
{
  @Param({
      //"concise",
      "roaring"
  })
  String bitmapType;

  @Param({"500000"})
  private int numRows;

  @Param({
      "0.001",
      "0.01",
      "0.1",
      "0.25",
      "0.5",
      "0.75",
      "0.99999"
  })
  private double filterMatch;

  @Param({
      "0",
      "0.1",
      "0.25",
      "0.5",
      "0.75",
      "0.99"
  })
  private double nullDensity;

  ImmutableBitmap bitmap;
  BitSet pretendFilterOffsets;

  @Setup
  public void setup()
  {
    pretendFilterOffsets = new BitSet(numRows);
    switch (bitmapType) {
      case "concise":
        ConciseSet conciseSet = new ConciseSet();
        for (int i = 0; i < numRows; i++) {
          double rando = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
          if (filterMatch == 1.0 || rando < filterMatch) {
            pretendFilterOffsets.set(i);
          }
          if (rando < nullDensity) {
            conciseSet.add(i);
          }
        }
        bitmap = new WrappedImmutableConciseBitmap(ImmutableConciseSet.newImmutableFromMutable(conciseSet));
        break;
      case "roaring":
        MutableRoaringBitmap roaringBitmap = new MutableRoaringBitmap();

        for (int i = 0; i < numRows; i++) {
          double rando = ThreadLocalRandom.current().nextDouble(0.0, 1.0);
          if (filterMatch == 1.0 || rando < filterMatch) {
            pretendFilterOffsets.set(i);
          }
          if (rando < nullDensity) {
            roaringBitmap.add(i);
          }
        }
        bitmap = new WrappedImmutableRoaringBitmap(roaringBitmap.toImmutableRoaringBitmap());
        break;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void get(final Blackhole blackhole)
  {
    for (int i = pretendFilterOffsets.nextSetBit(0); i >= 0; i = pretendFilterOffsets.nextSetBit(i + 1)) {
      final boolean isNull = bitmap.get(i);
      if (isNull) {
        blackhole.consume(isNull);
      } else {
        blackhole.consume(i);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void iterator(final Blackhole blackhole)
  {
    IntIterator nullIterator = bitmap.iterator();
    int offsetMark = -1;
    int nullMark = -1;
    for (int i = pretendFilterOffsets.nextSetBit(0); i >= 0; i = pretendFilterOffsets.nextSetBit(i + 1)) {
      // this is totally useless, hopefully this doesn't get optimized out, try to mimic what the selector is doing
      if (i < offsetMark) {
        nullMark = -1;
        nullIterator = bitmap.iterator();
      }
      offsetMark = i;
      while (nullMark < i && nullIterator.hasNext()) {
        nullMark = nullIterator.next();
      }
      final boolean isNull = nullMark == offsetMark;
      if (isNull) {
        blackhole.consume(isNull);
      } else {
        blackhole.consume(offsetMark);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void peekableIterator(final Blackhole blackhole)
  {
    PeekableIntIterator nullIterator = bitmap.peekableIterator();
    int offsetMark = -1;
    int nullMark = -1;
    for (int i = pretendFilterOffsets.nextSetBit(0); i >= 0; i = pretendFilterOffsets.nextSetBit(i + 1)) {
      // this is totally useless, hopefully this doesn't get optimized out, try to mimic what the selector is doing
      if (i < offsetMark) {
        nullMark = -1;
        nullIterator = bitmap.peekableIterator();
      }
      offsetMark = i;
      if (nullMark < i) {
        nullIterator.advanceIfNeeded(i);
        if (nullIterator.hasNext()) {
          nullMark = nullIterator.next();
        }
      }
      final boolean isNull = nullMark == offsetMark;
      if (isNull) {
        blackhole.consume(isNull);
      } else {
        blackhole.consume(offsetMark);
      }
    }
  }
}
