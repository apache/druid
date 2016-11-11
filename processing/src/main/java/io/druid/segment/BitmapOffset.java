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

package io.druid.segment;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.data.Offset;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.roaringbitmap.IntIterator;

/**
 */
public class BitmapOffset implements Offset
{
  private static final int INVALID_VALUE = -1;
  private static final BitmapFactory ROARING_BITMAP_FACTORY = new RoaringBitmapSerdeFactory(false).getBitmapFactory();

  private final IntIterator itr;
  private final BitmapFactory bitmapFactory;
  private final ImmutableBitmap bitmapIndex;
  private final boolean descending;

  private volatile int val;

  public static IntIterator getReverseBitmapOffsetIterator(ImmutableBitmap bitmapIndex)
  {
    ImmutableBitmap roaringBitmap = bitmapIndex;
    if (!(bitmapIndex instanceof WrappedImmutableRoaringBitmap)) {
      final MutableBitmap bitmap = ROARING_BITMAP_FACTORY.makeEmptyMutableBitmap();
      final IntIterator iterator = bitmapIndex.iterator();
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      roaringBitmap = ROARING_BITMAP_FACTORY.makeImmutableBitmap(bitmap);
    }
    return ((WrappedImmutableRoaringBitmap) roaringBitmap).getBitmap().getReverseIntIterator();
  }

  public BitmapOffset(BitmapFactory bitmapFactory, ImmutableBitmap bitmapIndex, boolean descending)
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmapIndex = bitmapIndex;
    this.descending = descending;
    this.itr = newIterator();
    increment();
  }

  private IntIterator newIterator()
  {
    if (!descending) {
      return bitmapIndex.iterator();
    } else {
      return getReverseBitmapOffsetIterator(bitmapIndex);
    }
  }

  private BitmapOffset(BitmapOffset otherOffset)
  {
    this.bitmapFactory = otherOffset.bitmapFactory;
    this.bitmapIndex = otherOffset.bitmapIndex;
    this.descending = otherOffset.descending;
    this.itr = otherOffset.itr.clone();
    this.val = otherOffset.val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    if (bitmapIndex == null || bitmapIndex.size() == 0) {
      return new BitmapOffset(bitmapFactory, bitmapFactory.makeEmptyImmutableBitmap(), descending);
    }

    return new BitmapOffset(this);
  }

  @Override
  public int getOffset()
  {
    return val;
  }
}
