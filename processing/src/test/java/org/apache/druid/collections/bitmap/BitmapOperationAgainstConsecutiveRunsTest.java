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

public class BitmapOperationAgainstConsecutiveRunsTest extends BitmapOperationTestBase
{

  public static final double DENSITY = 0.001;
  public static final int MIN_INTERSECT = 50;

  @BeforeClass
  public static void prepareRandomRanges() throws Exception
  {
    reset();

    final BitSet expectedUnion = new BitSet();
    for (int i = 0; i < NUM_BITMAPS; ++i) {
      ConciseSet c = new ConciseSet();
      MutableRoaringBitmap r = new MutableRoaringBitmap();
      {
        int k = 0;
        boolean fill = true;
        while (k < BITMAP_LENGTH) {
          int runLength = (int) (BITMAP_LENGTH * DENSITY) + rand.nextInt((int) (BITMAP_LENGTH * DENSITY));
          for (int j = k; fill && j < BITMAP_LENGTH && j < k + runLength; ++j) {
            c.add(j);
            r.add(j);
            expectedUnion.set(j);
          }
          k += runLength;
          fill = !fill;
        }
      }
      minIntersection = MIN_INTERSECT;
      for (int k = BITMAP_LENGTH / 2; k < BITMAP_LENGTH / 2 + minIntersection; ++k) {
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
    printSizeStats(DENSITY, "Random Alternating Bitmap");
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    reset();
  }
}
