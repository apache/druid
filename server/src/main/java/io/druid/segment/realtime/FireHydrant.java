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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.IOException;

/**
 */
public class FireHydrant
{
  private final int count;
  private volatile IncrementalIndex index;
  private volatile ReferenceCountingSegment adapter;

  public FireHydrant(
      IncrementalIndex index,
      int count,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.adapter = new ReferenceCountingSegment(new IncrementalIndexSegment(index, segmentIdentifier));
    this.count = count;
  }

  public FireHydrant(
      Segment adapter,
      int count
  )
  {
    this.index = null;
    this.adapter = new ReferenceCountingSegment(adapter);
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public ReferenceCountingSegment getSegment()
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
    if (this.adapter != null) {
      try {
        this.adapter.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    this.adapter = new ReferenceCountingSegment(adapter);
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
