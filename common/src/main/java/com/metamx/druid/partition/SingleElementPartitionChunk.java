/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.partition;

/**
 */
public class SingleElementPartitionChunk<T> implements PartitionChunk<T>
{
  private final T element;

  public SingleElementPartitionChunk
  (
      T element
  )
  {
    this.element = element;
  }

  @Override
  public T getObject()
  {
    return element;
  }

  @Override
  public boolean abuts(PartitionChunk<T> tPartitionChunk)
  {
    return false;
  }

  @Override
  public boolean isStart()
  {
    return true;
  }

  @Override
  public boolean isEnd()
  {
    return true;
  }

  @Override
  public int getChunkNumber()
  {
    return 0;
  }

  /**
   * The ordering of PartitionChunks is determined entirely by the partition boundaries and has nothing to do
   * with the object.  Thus, if there are two SingleElementPartitionChunks, they are equal because they both
   * represent the full partition space.
   *
   * SingleElementPartitionChunks are currently defined as less than every other type of PartitionChunk.  There
   * is no good reason for it, nor is there a bad reason, that's just the way it is.  This is subject to change.
   *
   * @param chunk
   * @return
   */
  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    return chunk instanceof SingleElementPartitionChunk ? 0 : -1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return element != null ? element.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "SingleElementPartitionChunk{" +
           "element=" + element +
           '}';
  }
}
