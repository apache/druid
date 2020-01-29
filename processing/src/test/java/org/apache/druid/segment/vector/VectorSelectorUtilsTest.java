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

package org.apache.druid.segment.vector;

import com.google.common.collect.Sets;
import org.apache.druid.collections.IntSetTestUtility;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.WrappedBitSetBitmap;
import org.apache.druid.collections.bitmap.WrappedConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.PeekableIntIterator;

import java.util.ArrayList;
import java.util.Set;

public class VectorSelectorUtilsTest
{
  private static final Set<Integer> NULLS = IntSetTestUtility.getSetBits();
  private static final Set<Integer> NULLS_PATTERN = alternatngPattern(10, 12);

  @Test
  public void testBitSetNullVector()
  {
    final WrappedBitSetBitmap bitmap = new WrappedBitSetBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    final WrappedBitSetBitmap bitmap2 = new WrappedBitSetBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testConciseMutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    final WrappedConciseBitmap bitmap2 = new WrappedConciseBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testConciseImmutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap, NULLS);
    final ImmutableBitmap immutable = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap.getBitmap())
    );
    assertNullVector(immutable, NULLS);

    final WrappedConciseBitmap bitmap2 = new WrappedConciseBitmap();
    populate(bitmap2, NULLS_PATTERN);
    final ImmutableBitmap immutable2 = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap2.getBitmap())
    );
    assertNullVector(immutable2, NULLS_PATTERN);
  }

  @Test
  public void testRoaringMutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap, NULLS);

    WrappedRoaringBitmap bitmap2 = new WrappedRoaringBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2, NULLS_PATTERN);
  }

  @Test
  public void testRoaringImmutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap, NULLS);
    assertNullVector(bitmap.toImmutableBitmap(), NULLS);

    WrappedRoaringBitmap bitmap2 = new WrappedRoaringBitmap();
    populate(bitmap2, NULLS_PATTERN);
    assertNullVector(bitmap2.toImmutableBitmap(), NULLS_PATTERN);
  }

  public static void populate(MutableBitmap bitmap, Set<Integer> setBits)
  {
    for (int i : setBits) {
      bitmap.add(i);
    }
  }

  private static Set<Integer> alternatngPattern(int smallSize, int rowCount)
  {
    ArrayList<Integer> bits = new ArrayList<>();
    boolean flipped = true;
    for (int i = 0; i < rowCount; i++) {
      if (i > 0 && i % smallSize == 0) {
        flipped = !flipped;
      }
      if (flipped) {
        bits.add(i);
      }
    }
    return Sets.newTreeSet(bits);
  }

  private void assertNullVector(ImmutableBitmap bitmap, Set<Integer> nulls)
  {
    // test entire set in one vector
    PeekableIntIterator iterator = bitmap.peekableIterator();
    final int vectorSize = 32;
    final boolean[] nullVector = new boolean[vectorSize];
    ReadableVectorOffset someOffset = new NoFilterVectorOffset(vectorSize, 0, vectorSize);

    VectorSelectorUtils.populateNullVector(nullVector, someOffset, iterator);
    for (int i = 0; i < vectorSize; i++) {
      Assert.assertEquals(nulls.contains(i), nullVector[i]);
    }

    // test entire set split into 4 chunks with smaller vectors
    iterator = bitmap.peekableIterator();
    final int smallerVectorSize = 8;
    boolean[] smallVector = null;
    for (int offset = 0; offset < smallerVectorSize * 4; offset += smallerVectorSize) {
      ReadableVectorOffset smallOffset = new NoFilterVectorOffset(smallerVectorSize, offset, offset + smallerVectorSize);
      smallVector = VectorSelectorUtils.populateNullVector(smallVector, smallOffset, iterator);
      for (int i = 0; i < smallerVectorSize; i++) {
        if (smallVector == null) {
          Assert.assertFalse(nulls.contains(offset + i));
        } else {
          Assert.assertEquals(nulls.contains(offset + i), smallVector[i]);
        }
      }
    }

    // a magical vector perfectly sized to the number of nulls with a bitmap vector offset of just the nulls
    iterator = bitmap.peekableIterator();
    ReadableVectorOffset allTheNulls = new BitmapVectorOffset(nulls.size(), bitmap, 0, 32);
    smallVector = VectorSelectorUtils.populateNullVector(smallVector, allTheNulls, iterator);
    for (int i = 0; i < nulls.size(); i++) {
      Assert.assertTrue(smallVector[i]);
    }
  }
}
