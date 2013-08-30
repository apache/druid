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

package io.druid.timeline;

import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

/**
*/
public class TimelineObjectHolder<VersionType, ObjectType> implements LogicalSegment
{
  private final Interval interval;
  private final VersionType version;
  private final PartitionHolder<ObjectType> object;

  public TimelineObjectHolder(
      Interval interval,
      VersionType version,
      PartitionHolder<ObjectType> object
  )
  {
    this.interval = interval;
    this.version = version;
    this.object = object;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  public VersionType getVersion()
  {
    return version;
  }

  public PartitionHolder<ObjectType> getObject()
  {
    return object;
  }

  @Override
  public String toString()
  {
    return "TimelineObjectHolder{" +
           "interval=" + interval +
           ", version=" + version +
           ", object=" + object +
           '}';
  }
}
