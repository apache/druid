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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;


public class UnionTimeLineLookup<VersionType, ObjectType> implements TimelineLookup<VersionType, ObjectType>
{
  Iterable<TimelineLookup<VersionType, ObjectType>> delegates;

  public UnionTimeLineLookup(Iterable<TimelineLookup<VersionType, ObjectType>> delegates)
  {
    this.delegates = delegates;
  }

  @Override
  public Iterable<TimelineObjectHolder<VersionType, ObjectType>> lookup(final Interval interval)
  {
    return Iterables.concat(
        Iterables.transform(
            delegates,
            new Function<TimelineLookup<VersionType, ObjectType>, Iterable<TimelineObjectHolder<VersionType, ObjectType>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<VersionType, ObjectType>> apply(TimelineLookup<VersionType, ObjectType> input)
              {
                return input.lookup(interval);
              }
            }
        )
    );
  }

  public PartitionHolder<ObjectType> findEntry(Interval interval, VersionType version)
  {
    for (TimelineLookup<VersionType, ObjectType> delegate : delegates) {
      final PartitionHolder<ObjectType> entry = delegate.findEntry(interval, version);
      if (entry != null) {
        return entry;
      }
    }
    return null;
  }
}
