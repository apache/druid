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

package io.druid.segment.loading;

import com.google.common.collect.Sets;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Set;

/**
*/
class StorageLocation
{
  private final File path;
  private final long maxSize;
  private final Set<DataSegment> segments;

  private volatile long currSize = 0;

  StorageLocation(File path, long maxSize)
  {
    this.path = path;
    this.maxSize = maxSize;

    this.segments = Sets.newHashSet();
  }

  File getPath()
  {
    return path;
  }

  long getMaxSize()
  {
    return maxSize;
  }

  synchronized void addSegment(DataSegment segment)
  {
    if (segments.add(segment)) {
      currSize += segment.getSize();
    }
  }

  synchronized void removeSegment(DataSegment segment)
  {
    if (segments.remove(segment)) {
      currSize -= segment.getSize();
    }
  }

  boolean canHandle(long size)
  {
    return available() >= size;
  }

  synchronized long available()
  {
    return maxSize - currSize;
  }

  StorageLocation mostEmpty(StorageLocation other)
  {
    return available() > other.available() ? this : other;
  }
}
