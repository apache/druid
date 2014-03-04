/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.client;

import java.util.Iterator;

/**
 */
public class RangeIterable implements Iterable<Integer>
{
  private final int end;
  private final int start;
  private final int increment;

  public RangeIterable(
      int end
  )
  {
    this(0, end);
  }

  public RangeIterable(
      int start,
      int end
  )
  {
    this(start, end, 1);
  }

  public RangeIterable(
      int start,
      int end,
      final int i
  )
  {
    this.start = start;
    this.end = end;
    this.increment = i;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      private int curr = start;

      @Override
      public boolean hasNext()
      {
        return curr < end;
      }

      @Override
      public Integer next()
      {
        try {
          return curr;
        }
        finally {
          curr += increment;
        }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
