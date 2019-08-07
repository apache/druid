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

package org.apache.druid.collections.bitmap;

import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.druid.collections.test.annotation.Benchmark;
import org.apache.druid.extendedset.intset.ConciseSet;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.BitSet;

/**
 * TODO rewrite this benchmark to JMH
 */
@Category({Benchmark.class})
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class RangeBitmapBenchmarkTest extends BitmapBenchmark
{

  public static final double DENSITY = 0.001;
  public static final int MIN_INTERSECT = 50;

  @BeforeClass
  public static void prepareRandomRanges() throws Exception
  {
    System.setProperty("jub.customkey", StringUtils.format("%06.5f", DENSITY));
    reset();

    final BitSet expectedUnion = new BitSet();
    for (int i = 0; i < SIZE; ++i) {
      ConciseSet c = new ConciseSet();
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      {
        int k = 0;
        boolean fill = true;
        while (k < LENGTH) {
          int runLength = (int) (LENGTH * DENSITY) + rand.nextInt((int) (LENGTH * DENSITY));
          for (int j = k; fill && j < LENGTH && j < k + runLength; ++j) {
            c.add(j);
            r.add(j);
            expectedUnion.set(j);
          }
          k += runLength;
          fill = !fill;
        }
      }
      minIntersection = MIN_INTERSECT;
      for (int k = LENGTH / 2; k < LENGTH / 2 + minIntersection; ++k) {
        c.add(k);
        r.add(k);
        expectedUnion.set(k);
      }
      CONCISE[i] = ImmutableConciseSet.newImmutableFromMutable(c);
      OFF_HEAP_CONCISE[i] = makeOffheapConcise(CONCISE[i]);
      ROARING[i] = r;
      IMMUTABLE_ROARING[i] = makeImmutableRoaring(r);
      OFF_HEAP_ROARING[i] = makeOffheapRoaring(r);
      GENERIC_CONCISE[i] = new WrappedImmutableConciseBitmap(OFF_HEAP_CONCISE[i]);
      GENERIC_ROARING[i] = new WrappedImmutableRoaringBitmap(OFF_HEAP_ROARING[i]);
    }
    unionCount = expectedUnion.cardinality();
    printSizeStats(DENSITY, "Random Alternating Bitmap");
  }
}
