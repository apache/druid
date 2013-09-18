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

/**
 */
public class ArrayBasedOffset implements Offset
{
  private final int[] ints;
  private int currIndex;

  public ArrayBasedOffset(
      int[] ints
  )
  {
    this(ints, 0);
  }

  public ArrayBasedOffset(
      int[] ints,
      int startIndex
  )
  {
    this.ints = ints;
    this.currIndex = startIndex;
  }

  @Override
  public int getOffset()
  {
    return ints[currIndex];
  }

  @Override
  public void increment()
  {
    ++currIndex;
  }

  @Override
  public boolean withinBounds()
  {
    return currIndex < ints.length;
  }

  @Override
  public Offset clone()
  {
    final ArrayBasedOffset retVal = new ArrayBasedOffset(ints);
    retVal.currIndex = currIndex;
    return retVal;
  }
}
