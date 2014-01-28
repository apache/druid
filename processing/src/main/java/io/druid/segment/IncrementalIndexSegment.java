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

package io.druid.segment;

import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class IncrementalIndexSegment implements Segment
{
  private final IncrementalIndex index;
  private final String segmentIdentifier;

  public IncrementalIndexSegment(
      IncrementalIndex index,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.segmentIdentifier = segmentIdentifier;
  }

  @Override
  public String getIdentifier()
  {
    return segmentIdentifier;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new IncrementalIndexStorageAdapter(index);
  }

  @Override
  public void close() throws IOException
  {
    // do nothing
  }
}
