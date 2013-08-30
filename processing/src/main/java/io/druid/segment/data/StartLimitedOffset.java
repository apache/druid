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
public class StartLimitedOffset implements Offset
{
  private final Offset baseOffset;
  private final int limit;

  public StartLimitedOffset(
      Offset baseOffset,
      int limit
  )
  {
    this.baseOffset = baseOffset;
    this.limit = limit;

    while (baseOffset.withinBounds() && baseOffset.getOffset() < limit) {
      baseOffset.increment();
    }
  }

  @Override
  public void increment()
  {
    baseOffset.increment();
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public Offset clone()
  {
    return new StartLimitedOffset(baseOffset.clone(), limit);
  }

  @Override
  public int getOffset()
  {
    return baseOffset.getOffset();
  }
}
