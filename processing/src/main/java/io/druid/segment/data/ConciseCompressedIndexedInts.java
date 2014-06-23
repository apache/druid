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
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 */
public class ConciseCompressedIndexedInts implements IndexedInts, Comparable<ConciseCompressedIndexedInts>
{
  public static ObjectStrategy<ImmutableConciseSet> objectStrategy =
      new ImmutableConciseSetObjectStrategy();

  private static Ordering<ImmutableConciseSet> comparator = new Ordering<ImmutableConciseSet>()
  {
    @Override
    public int compare(
        @Nullable ImmutableConciseSet conciseSet, @Nullable ImmutableConciseSet conciseSet1
    )
    {
      if (conciseSet.size() == 0 && conciseSet1.size() == 0) {
        return 0;
      }
      if (conciseSet.size() == 0) {
        return -1;
      }
      if (conciseSet1.size() == 0) {
        return 1;
      }
      return conciseSet.compareTo(conciseSet1);
    }
  }.nullsFirst();

  private final ImmutableConciseSet immutableConciseSet;

  public ConciseCompressedIndexedInts(ImmutableConciseSet conciseSet)
  {
    this.immutableConciseSet = conciseSet;
  }

  @Override
  public int compareTo(ConciseCompressedIndexedInts conciseCompressedIndexedInts)
  {
    return immutableConciseSet.compareTo(conciseCompressedIndexedInts.getImmutableConciseSet());
  }

  @Override
  public int size()
  {
    return immutableConciseSet.size();
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException("This is really slow, so it's just not supported.");
  }

  public ImmutableConciseSet getImmutableConciseSet()
  {
    return immutableConciseSet;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      IntSet.IntIterator baseIterator = immutableConciseSet.iterator();

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

  private static class ImmutableConciseSetObjectStrategy
      implements ObjectStrategy<ImmutableConciseSet>
  {
    @Override
    public Class<? extends ImmutableConciseSet> getClazz()
    {
      return ImmutableConciseSet.class;
    }

    @Override
    public ImmutableConciseSet fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return new ImmutableConciseSet(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(ImmutableConciseSet val)
    {
      if (val == null || val.size() == 0) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(ImmutableConciseSet o1, ImmutableConciseSet o2)
    {
      return comparator.compare(o1, o2);
    }
  }
}
