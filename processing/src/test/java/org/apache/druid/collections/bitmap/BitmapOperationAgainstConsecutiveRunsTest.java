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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.extendedset.intset.ConciseSet;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.apache.druid.utils.CloseableUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

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
      final ResourceHolder<ImmutableConciseSet> offheapConciseHolder = makeOffheapConcise(CONCISE[i]);
      OFF_HEAP_CONCISE[i] = offheapConciseHolder.get();
      ROARING[i] = r;
      IMMUTABLE_ROARING[i] = makeImmutableRoaring(r);

      final ResourceHolder<ImmutableRoaringBitmap> offheapRoaringHolder = makeOffheapRoaring(r);
      OFF_HEAP_ROARING[i] = offheapRoaringHolder.get();
      GENERIC_CONCISE[i] = new WrappedImmutableConciseBitmap(OFF_HEAP_CONCISE[i]);
      GENERIC_ROARING[i] = new WrappedImmutableRoaringBitmap(OFF_HEAP_ROARING[i]);

      synchronized (CLOSEABLES) {
        final List<Closeable> closeables = CLOSEABLES.computeIfAbsent(
            BitmapOperationAgainstConsecutiveRunsTest.class,
            k -> new ArrayList<>()
        );

        closeables.add(offheapConciseHolder);
        closeables.add(offheapRoaringHolder);
      }
    }
    unionCount = expectedUnion.cardinality();
    printSizeStats(DENSITY, "Random Alternating Bitmap");
  }

  @AfterClass
  public static void closeCloseables()
  {
    synchronized (CLOSEABLES) {
      try {
        final List<Closeable> theCloseables = CLOSEABLES.remove(BitmapOperationAgainstConsecutiveRunsTest.class);
        if (theCloseables != null) {
          CloseableUtils.closeAll(theCloseables);
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
