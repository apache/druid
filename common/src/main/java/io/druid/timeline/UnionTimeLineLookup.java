/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
