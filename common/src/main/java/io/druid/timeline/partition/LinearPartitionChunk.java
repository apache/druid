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

package io.druid.timeline.partition;

import com.google.common.collect.Ordering;

import java.util.Comparator;

public class LinearPartitionChunk <T> implements PartitionChunk<T>
{
  Comparator<Integer> comparator = Ordering.<Integer>natural().nullsFirst();

  private final int chunkNumber;
  private final T object;

  public static <T> LinearPartitionChunk<T> make(int chunkNumber, T obj)
  {
    return new LinearPartitionChunk<T>(chunkNumber, obj);
  }

  public LinearPartitionChunk(
      int chunkNumber,
      T object
  )
  {
    this.chunkNumber = chunkNumber;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> chunk)
  {
    return true; // always complete
  }

  @Override
  public boolean isStart()
  {
    return true; // always complete
  }

  @Override

  public boolean isEnd()
  {
    return true; // always complete
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    if (chunk instanceof LinearPartitionChunk) {
      LinearPartitionChunk<T> linearChunk = (LinearPartitionChunk<T>) chunk;

      return comparator.compare(chunkNumber, linearChunk.chunkNumber);
    }
    throw new IllegalArgumentException("Cannot compare against something that is not a LinearPartitionChunk.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return compareTo((LinearPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return chunkNumber;
  }
}
