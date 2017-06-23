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
public class UniformBitmapBenchmarkTest extends BitmapBenchmark
{

  public static final double DENSITY = 0.01;
  public static final int MIN_INTERSECT = 50;

  @BeforeClass
  public static void prepareMostlyUniform() throws Exception
  {
    System.setProperty("jub.customkey", StringUtils.format("%05.4f", DENSITY));
    reset();

    final BitSet expectedUnion = new BitSet();
    final int[] knownTrue = new int[MIN_INTERSECT];
    for (int i = 0; i < knownTrue.length; ++i) {
      knownTrue[i] = rand.nextInt(LENGTH);
    }
    for (int i = 0; i < SIZE; ++i) {
      ConciseSet c = new ConciseSet();
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      for (int k = 0; k < LENGTH; ++k) {
        if (rand.nextDouble() < DENSITY) {
          c.add(k);
          r.add(k);
          expectedUnion.set(k);
        }
      }
      for (int k : knownTrue) {
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
    minIntersection = knownTrue.length;
    printSizeStats(DENSITY, "Uniform Bitmap");
  }
}
