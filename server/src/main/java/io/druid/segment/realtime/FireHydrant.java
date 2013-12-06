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

package io.druid.segment.realtime;

import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;

/**
*/
public class FireHydrant
{
  private volatile IncrementalIndex index;
  private volatile Segment adapter;
  private final int count;

  public FireHydrant(
      IncrementalIndex index,
      int count,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.adapter = new IncrementalIndexSegment(index, segmentIdentifier);
    this.count = count;
  }

  public FireHydrant(
      Segment adapter,
      int count
  )
  {
    this.index = null;
    this.adapter = adapter;
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public Segment getSegment()
  {
    return adapter;
  }

  public int getCount()
  {
    return count;
  }

  public boolean hasSwapped()
  {
    return index == null;
  }

  public void swapSegment(Segment adapter)
  {
    this.adapter = adapter;
    this.index = null;
  }

  @Override
  public String toString()
  {
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter +
           ", count=" + count +
           '}';
  }
}
