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

package io.druid.query.aggregation;

import io.druid.segment.LongColumnSelector;

/**
 */
public class TestLongColumnSelector implements LongColumnSelector
{
  private final long[] longs;

  private int index = 0;

  public TestLongColumnSelector(long[] longs)
  {
    this.longs = longs;
  }

  @Override
  public long get()
  {
    return longs[index];
  }

  public void increment()
  {
    ++index;
  }
}
