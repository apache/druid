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

package org.apache.druid.java.util.common.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;

/**
 * DurationGranularity buckets data based on the length of a duration
 */
public class DurationGranularity extends Granularity
{
  private final long duration;
  private final long origin;

  @JsonCreator
  public DurationGranularity(
      @JsonProperty("duration") long duration,
      @JsonProperty("origin") DateTime origin
  )
  {
    this(duration, origin == null ? 0 : origin.getMillis());
  }

  public DurationGranularity(long duration, long origin)
  {
    Preconditions.checkArgument(duration > 0, "duration should be greater than 0!");
    this.duration = duration;
    this.origin = origin % duration;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return DateTimes.utc(origin);
  }

  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return time.plus(getDuration());
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    long t = time.getMillis();
    final long duration = getDurationMillis();
    long offset = t % duration - origin;
    if (offset < 0) {
      offset += duration;
    }
    return new DateTime(t - offset, time.getChronology());
  }

  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public boolean isAligned(Interval interval)
  {
    if (interval.toDurationMillis() == duration) {
      return (interval.getStartMillis() - origin) % duration == 0;
    }
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(2 * Long.BYTES).putLong(duration).putLong(origin).array();
  }

  public long getDurationMillis()
  {
    return duration;
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

    DurationGranularity that = (DurationGranularity) o;

    if (duration != that.duration) {
      return false;
    }
    if (origin != that.origin) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (int) (duration ^ (duration >>> 32));
    result = 31 * result + (int) (origin ^ (origin >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "DurationGranularity{" +
           "duration=" + duration +
           ", origin=" + origin +
           '}';
  }
}
