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

import java.util.Set;

public class VectorSelectorUtilsTest
{
  @Test
  public void testBitSetNullVector()
  {
    final WrappedBitSetBitmap bitmap = new WrappedBitSetBitmap();
    populate(bitmap);
    assertNullVector(bitmap);
  }

  @Test
  public void testConciseMutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap);
    assertNullVector(bitmap);
  }

  @Test
  public void testConciseImmutableNullVector()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap);
    final ImmutableBitmap immutable = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap.getBitmap())
    );
    assertNullVector(immutable);
  }

  @Test
  public void testRoaringMutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap);
    assertNullVector(bitmap);
  }

  @Test
  public void testRoaringImmutableNullVector()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap);
    assertNullVector(bitmap.toImmutableBitmap());
  }

  public static void populate(MutableBitmap bitmap)
  {
    for (int i : IntSetTestUtility.getSetBits()) {
      bitmap.add(i);
    }
  }

  private void assertNullVector(ImmutableBitmap bitmap)
  {
    PeekableIntIterator iterator = bitmap.peekableIterator();
    Set<Integer> nulls = IntSetTestUtility.getSetBits();
    final int vectorSize = 32;
    final boolean[] nullVector = new boolean[vectorSize];
    ReadableVectorOffset someOffset = new NoFilterVectorOffset(vectorSize, 0, vectorSize);

    VectorSelectorUtils.populateNullVector(nullVector, someOffset, iterator);
    for (int i = 0; i < vectorSize; i++) {
      Assert.assertEquals(nulls.contains(i), nullVector[i]);
    }

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
      smallVector = null;
    }

    iterator = bitmap.peekableIterator();
    ReadableVectorOffset allTheNulls = new BitmapVectorOffset(8, bitmap, 0, 22);
    smallVector = VectorSelectorUtils.populateNullVector(smallVector, allTheNulls, iterator);
    for (int i = 0; i < nulls.size(); i++) {
      Assert.assertTrue(smallVector[i]);
    }
  }
}
