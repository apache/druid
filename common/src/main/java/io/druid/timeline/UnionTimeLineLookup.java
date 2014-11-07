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

import com.google.common.collect.Lists;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.List;


public class UnionTimeLineLookup<VersionType, ObjectType> implements TimelineLookup<VersionType, ObjectType>
{
  Iterable<TimelineLookup<VersionType,ObjectType>> delegates;
  public UnionTimeLineLookup( Iterable<TimelineLookup<VersionType,ObjectType>> delegates){
    this.delegates = delegates;
  }
  @Override
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval)
  {
    List<TimelineObjectHolder<VersionType, ObjectType>> rv = Lists.newArrayList();
    for(TimelineLookup<VersionType,ObjectType> delegate : delegates){
      rv.addAll(delegate.lookup(interval));
    }
    return rv;
  }

  public PartitionHolder<ObjectType> findEntry(Interval interval, VersionType version){
    for(TimelineLookup<VersionType,ObjectType> delegate : delegates){
      final PartitionHolder<ObjectType> entry = delegate.findEntry(interval, version);
      if(entry != null){
        return entry;
      }
    }
    return null;
  }
}
