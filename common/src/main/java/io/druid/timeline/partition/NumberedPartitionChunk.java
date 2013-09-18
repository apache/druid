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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

public class NumberedPartitionChunk<T> implements PartitionChunk<T>
{
  private final int chunkNumber;
  private final int chunks;
  private final T object;

  public static <T> NumberedPartitionChunk<T> make(
      int chunkNumber,
      int chunks,
      T obj
  )
  {
    return new NumberedPartitionChunk<T>(chunkNumber, chunks, obj);
  }

  public NumberedPartitionChunk(
      int chunkNumber,
      int chunks,
      T object
  )
  {
    Preconditions.checkArgument(chunkNumber >= 0, "chunkNumber >= 0");
    Preconditions.checkArgument(chunkNumber < chunks, "chunkNumber < chunks");
    this.chunkNumber = chunkNumber;
    this.chunks = chunks;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(final PartitionChunk<T> other)
  {
    return other instanceof NumberedPartitionChunk && other.getChunkNumber() == chunkNumber + 1;
  }

  @Override
  public boolean isStart()
  {
    return chunkNumber == 0;
  }

  @Override
  public boolean isEnd()
  {
    return chunkNumber == chunks - 1;
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> other)
  {
    if (other instanceof NumberedPartitionChunk) {
      final NumberedPartitionChunk castedOther = (NumberedPartitionChunk) other;
      return ComparisonChain.start()
                            .compare(chunks, castedOther.chunks)
                            .compare(chunkNumber, castedOther.chunkNumber)
                            .result();
    } else {
      throw new IllegalArgumentException("Cannot compare against something that is not a NumberedPartitionChunk.");
    }
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

    return compareTo((NumberedPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(chunks, chunkNumber);
  }
}
