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

package io.druid.collections.bitmap;

import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.druid.extendedset.intset.ConciseSet;
import io.druid.extendedset.intset.ImmutableConciseSet;
import io.druid.java.util.common.StringUtils;
import io.druid.test.annotation.Benchmark;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.BitSet;

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
      concise[i] = ImmutableConciseSet.newImmutableFromMutable(c);
      offheapConcise[i] = makeOffheapConcise(concise[i]);
      roaring[i] = r;
      immutableRoaring[i] = makeImmutableRoaring(r);
      offheapRoaring[i] = makeOffheapRoaring(r);
      genericConcise[i] = new WrappedImmutableConciseBitmap(offheapConcise[i]);
      genericRoaring[i] = new WrappedImmutableRoaringBitmap(offheapRoaring[i]);
    }
    unionCount = expectedUnion.cardinality();
    printSizeStats(DENSITY, "Random Alternating Bitmap");
  }
}
