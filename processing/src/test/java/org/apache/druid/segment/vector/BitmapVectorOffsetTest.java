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

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.concurrent.ThreadLocalRandom;

public class BitmapVectorOffsetTest
{
  private static final int VECTOR_SIZE = 128;
  private static final int ROWS = VECTOR_SIZE * VECTOR_SIZE;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testContiguousGetOffsetsIsExplode()
  {
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < ROWS; i++) {
      wrapped.add(i);
    }

    ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());
    BitmapVectorOffset offset = new BitmapVectorOffset(VECTOR_SIZE, bitmap, 0, ROWS);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("is contiguous");
    offset.getOffsets();
  }

  @Test
  public void testNotContiguousGetStartOffsetIsExplode()
  {
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < ROWS; i++) {
      if (i % 2 != 0) {
        wrapped.add(i);
      }
    }

    ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());
    BitmapVectorOffset offset = new BitmapVectorOffset(VECTOR_SIZE, bitmap, 0, ROWS);

    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("not contiguous");
    offset.getStartOffset();
  }

  @Test
  public void testContiguous()
  {
    // every bit is set, start from every offset and ensure all batches are contiguous
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < ROWS; i++) {
      wrapped.add(i);
    }

    ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());
    for (int startOffset = 0; startOffset < ROWS; startOffset++) {
      BitmapVectorOffset offset = new BitmapVectorOffset(VECTOR_SIZE, bitmap, startOffset, ROWS);

      while (!offset.isDone()) {
        if (offset.getCurrentVectorSize() > 1) {
          Assert.assertTrue(offset.isContiguous());
        }
        offset.advance();
      }
    }
  }

  @Test
  public void testNeverContiguous()
  {
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < ROWS; i++) {
      if (i % 2 != 0) {
        wrapped.add(i);
      }
    }

    ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());
    for (int startOffset = 0; startOffset < ROWS; startOffset++) {
      BitmapVectorOffset offset = new BitmapVectorOffset(VECTOR_SIZE, bitmap, startOffset, ROWS);
      while (!offset.isDone()) {
        Assert.assertFalse(offset.isContiguous());
        offset.advance();
      }
    }
  }

  @Test
  public void testSometimesContiguous()
  {
    // this test is sort of vague
    // set a lot of the rows so that there will be some contiguous and always at least 1 non-contiguous group
    // (i imagine this is somewhat dependent on underlying bitmap iterator implementation)
    MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < ROWS - VECTOR_SIZE + 1; i++) {
      int set = ThreadLocalRandom.current().nextInt(0, ROWS);
      while (wrapped.contains(set)) {
        set = ThreadLocalRandom.current().nextInt(0, ROWS);
      }
      wrapped.add(set);
    }

    ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());

    int contiguousCount = 0;
    int nonContiguousCount = 0;
    int noContiguous = 0;
    int allContiguous = 0;
    for (int startOffset = 0; startOffset < ROWS; startOffset++) {
      BitmapVectorOffset offset = new BitmapVectorOffset(VECTOR_SIZE, bitmap, startOffset, ROWS);

      boolean none = true;
      boolean all = true;
      while (!offset.isDone()) {
        if (offset.isContiguous()) {
          contiguousCount++;
          none = false;
        } else {
          nonContiguousCount++;
          all = false;
        }
        offset.advance();
      }
      if (none) {
        noContiguous++;
      }
      if (all) {
        allContiguous++;
      }
    }

    Assert.assertTrue(contiguousCount > 0);
    Assert.assertTrue(nonContiguousCount > 0);
    // depending on the distribution of set bits and starting offset, there are some which are never contiguous
    Assert.assertTrue(noContiguous > 0);
    Assert.assertEquals(0, allContiguous);
  }
}
