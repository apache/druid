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

import org.apache.druid.extendedset.intset.ConciseSet;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.util.BitSet;

public class BitmapOperationAgainstUniformDistributionTest extends BitmapOperationTestBase
{
  public static final double DENSITY = 0.01;
  public static final int MIN_INTERSECT = 50;

  @BeforeClass
  public static void prepareMostlyUniform() throws Exception
  {
    reset();

    final BitSet expectedUnion = new BitSet();
    final int[] knownTrue = new int[MIN_INTERSECT];
    for (int i = 0; i < knownTrue.length; ++i) {
      knownTrue[i] = rand.nextInt(BITMAP_LENGTH);
    }
    for (int i = 0; i < NUM_BITMAPS; ++i) {
      ConciseSet c = new ConciseSet();
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      for (int k = 0; k < BITMAP_LENGTH; ++k) {
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
      CONCISE[i] = ImmutableConciseSet.newImmutableFromMutable(c);
      OFF_HEAP_CONCISE[i] = makeOffheapConcise(CONCISE[i]);
      ROARING[i] = r;
      IMMUTABLE_ROARING[i] = makeImmutableRoaring(r);
      OFF_HEAP_ROARING[i] = makeOffheapRoaring(r);
      GENERIC_CONCISE[i] = new WrappedImmutableConciseBitmap(OFF_HEAP_CONCISE[i].get());
      GENERIC_ROARING[i] = new WrappedImmutableRoaringBitmap(OFF_HEAP_ROARING[i].get());
    }
    unionCount = expectedUnion.cardinality();
    minIntersection = knownTrue.length;
    printSizeStats(DENSITY, "Uniform Bitmap");
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    reset();
  }
}
