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

package com.metamx.druid.index;

import com.metamx.druid.StorageAdapter;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;

import java.io.IOException;

public class ReferenceCountingSegment implements Segment
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private final Segment baseSegment;

  private final Object lock = new Object();

  private volatile int numReferences = 0;
  private volatile boolean isClosed = false;

  public ReferenceCountingSegment(Segment baseSegment)
  {
    this.baseSegment = baseSegment;
  }

  public Segment getBaseSegment()
  {
    return baseSegment;
  }

  public boolean isClosed()
  {
    return isClosed;
  }

  @Override
  public String getIdentifier()
  {
    return baseSegment.getIdentifier();
  }

  @Override
  public Interval getDataInterval()
  {
    return baseSegment.getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return baseSegment.asQueryableIndex();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return baseSegment.asStorageAdapter();
  }

  @Override
  public void close() throws IOException
  {
    baseSegment.close();
  }

  public void increment()
  {
    synchronized (lock) {
      if (!isClosed) {
        numReferences++;
      }
    }
  }

  public void decrement()
  {
    synchronized (lock) {
      if (!isClosed) {
        if (--numReferences < 0) {
          try {
            close();
          }
          catch (Exception e) {
            log.error("Unable to close queryable index %s", getIdentifier());
          }
          finally {
            isClosed = true;
          }
        }
      }
    }
  }
}