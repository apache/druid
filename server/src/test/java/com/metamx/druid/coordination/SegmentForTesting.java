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

package com.metamx.druid.coordination;

import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.Segment;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class SegmentForTesting implements Segment
{
  private final String version;
  private final Interval interval;

  private final Object lock = new Object();

  private volatile boolean closed = false;

  SegmentForTesting(
      String version,
      Interval interval
  )
  {
    this.version = version;
    this.interval = interval;
  }

  public String getVersion()
  {
    return version;
  }

  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public String getIdentifier()
  {
    return version;
  }

  public boolean isClosed()
  {
    return closed;
  }

  @Override
  public Interval getDataInterval()
  {
    return interval;
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    synchronized (lock) {
      closed = true;
    }
  }
}
