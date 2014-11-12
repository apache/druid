/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import com.metamx.collections.bitmap.ImmutableBitmap;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 */
public class BitmapCompressedIndexedInts implements IndexedInts, Comparable<ImmutableBitmap>
{
  private static Ordering<ImmutableBitmap> comparator = new Ordering<ImmutableBitmap>()
  {
    @Override
    public int compare(
        ImmutableBitmap set, ImmutableBitmap set1
    )
    {
      if (set.size() == 0 && set1.size() == 0) {
        return 0;
      }
      if (set.size() == 0) {
        return -1;
      }
      if (set1.size() == 0) {
        return 1;
      }
      return set.compareTo(set1);
    }
  }.nullsFirst();

  private final ImmutableBitmap immutableBitmap;

  public BitmapCompressedIndexedInts(ImmutableBitmap immutableBitmap)
  {
    this.immutableBitmap = immutableBitmap;
  }

  @Override
  public int compareTo(@Nullable ImmutableBitmap otherBitmap)
  {
    return comparator.compare(immutableBitmap, otherBitmap);
  }

  @Override
  public int size()
  {
    return immutableBitmap.size();
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException("This is really slow, so it's just not supported.");
  }

  public ImmutableBitmap getImmutableBitmap()
  {
    return immutableBitmap;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      IntIterator baseIterator = immutableBitmap.iterator();

      @Override
      public boolean hasNext()
      {
        return baseIterator.hasNext();
      }

      @Override
      public Integer next()
      {
        return baseIterator.next();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
