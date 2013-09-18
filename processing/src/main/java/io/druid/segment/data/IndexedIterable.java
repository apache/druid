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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class IndexedIterable<T> implements Iterable<T>
{
  private final Indexed<T> indexed;

  public static <T> IndexedIterable<T> create(Indexed<T> indexed)
  {
    return new IndexedIterable<T>(indexed);
  }

  public IndexedIterable(
    Indexed<T> indexed
  )
  {
    this.indexed = indexed;
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      private int currIndex = 0;

      @Override
      public boolean hasNext()
      {
        return currIndex < indexed.size();
      }

      @Override
      public T next()
      {
        if (! hasNext()) {
          throw new NoSuchElementException();
        }
        return indexed.get(currIndex++);
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
