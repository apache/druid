/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.timeline;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.Objects;

/**
*/
public class TimelineObjectHolder<VersionType, ObjectType extends Overshadowable<ObjectType>> implements LogicalSegment
{
  private final Interval interval;
  private final Interval trueInterval;
  private final VersionType version;
  private final PartitionHolder<ObjectType> object;

  @VisibleForTesting
  public TimelineObjectHolder(Interval interval, VersionType version, PartitionHolder<ObjectType> object)
  {
    this(interval, interval, version, object);
  }

  public TimelineObjectHolder(
      Interval interval,
      Interval trueInterval,
      VersionType version,
      PartitionHolder<ObjectType> object
  )
  {
    this.interval = interval;
    this.trueInterval = trueInterval;
    this.version = version;
    this.object = object;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public Interval getTrueInterval()
  {
    return trueInterval;
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimelineObjectHolder<?, ?> that = (TimelineObjectHolder<?, ?>) o;
    return Objects.equals(interval, that.interval) &&
           Objects.equals(version, that.version) &&
           Objects.equals(object, that.object);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, version, object);
  }

  @Override
  public String toString()
  {
    return "TimelineObjectHolder{" +
           "interval=" + interval +
           ", trueInterval=" + trueInterval +
           ", version=" + version +
           ", object=" + object +
           '}';
  }
}
