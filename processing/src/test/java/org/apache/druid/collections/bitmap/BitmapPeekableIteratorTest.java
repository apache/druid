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

import org.apache.druid.collections.IntSetTestUtility;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.PeekableIntIterator;

public class BitmapPeekableIteratorTest
{
  @Test
  public void testBitSet()
  {
    final WrappedBitSetBitmap bitmap = new WrappedBitSetBitmap();
    populate(bitmap);
    assertPeekable(bitmap.peekableIterator());
  }

  @Test
  public void testConciseMutable()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap);
    assertPeekable(bitmap.peekableIterator());
  }

  @Test
  public void testConciseImmutable()
  {
    final WrappedConciseBitmap bitmap = new WrappedConciseBitmap();
    populate(bitmap);
    final ImmutableBitmap immutable = new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(bitmap.getBitmap())
    );
    assertPeekable(immutable.peekableIterator());
  }

  @Test
  public void testRoaringMutable()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap);
    assertPeekable(bitmap.peekableIterator());
  }

  @Test
  public void testRoaringImmutable()
  {
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    populate(bitmap);
    assertPeekable(bitmap.toImmutableBitmap().peekableIterator());
  }

  private void populate(MutableBitmap bitmap)
  {
    for (int i : IntSetTestUtility.getSetBits()) {
      bitmap.add(i);
    }
  }

  private void assertPeekable(PeekableIntIterator iterator)
  {
    // extra calls are to make sure things that are not expected to have apparent side-effects have none
    Assert.assertTrue(iterator.hasNext());
    int mark = -1;
    for (int i : IntSetTestUtility.getSetBits()) {
      Assert.assertTrue(iterator.hasNext());
      iterator.advanceIfNeeded(i);
      Assert.assertTrue(iterator.hasNext());
      iterator.advanceIfNeeded(i); // this should do nothing
      Assert.assertTrue(iterator.hasNext());
      if (iterator.hasNext()) {
        Assert.assertEquals(i, iterator.peekNext());
        mark = iterator.next();
      }
      Assert.assertEquals(i, mark);
    }
    Assert.assertFalse(iterator.hasNext());
  }
}
