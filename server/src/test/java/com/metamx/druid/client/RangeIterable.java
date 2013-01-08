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

package com.metamx.druid.client;

import java.util.Iterator;

/**
 */
public class RangeIterable implements Iterable<Integer>
{
  private final int startValue;
  private final int endValue;
  private final int increment;

  public RangeIterable(int endValue)
  {
    this(0, endValue);
  }

  public RangeIterable(int startValue, int endValue)
  {
    this(startValue, endValue, 1);
  }

  public RangeIterable(int startValue, int endValue, int increment)
  {
    this.startValue = startValue;
    this.endValue = endValue;
    this.increment = increment;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      int value = startValue;

      @Override
      public boolean hasNext()
      {
        return value < endValue;
      }

      @Override
      public Integer next()
      {
        try {
          return value;
        }
        finally {
          value += increment;
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
