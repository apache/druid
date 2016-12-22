/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

// This class was earlier called PeriodGranularity.
// Renaming this to be more specific and inline with PeriodSegmentGranularity
// If you want to use granualrity for non-segment or non-query purposes, the please use PeriodGranularity
public class PeriodQueryGranularity extends BaseQueryGranularity
{
  private final PeriodGranularity periodGranularity;

  @JsonCreator
  public PeriodQueryGranularity(
      @JsonProperty("period") Period period,
      @JsonProperty("origin") DateTime origin,
      @JsonProperty("timeZone") DateTimeZone tz
  )
  {
    this.periodGranularity = new PeriodGranularity(period, origin, tz);
  }

  @JsonProperty("period")
  public Period getPeriod()
  {
    return periodGranularity.getPeriod();
  }

  @JsonProperty("timeZone")
  public DateTimeZone getTimeZone()
  {
    return periodGranularity.getTimeZone();
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return periodGranularity.getOrigin();
  }

  @Override
  public DateTime toDateTime(long t)
  {
    return new DateTime(t, periodGranularity.getTimeZone());
  }

  @Override
  public long next(long t)
  {
    return periodGranularity.increment(t);
  }

  @Override
  public long truncate(long t)
  {
    return periodGranularity.truncate(t);
  }

  @Override
  public byte[] cacheKey()
  {
    return StringUtils.toUtf8(periodGranularity.getPeriod().toString() + ":" +
                              periodGranularity.getTimeZone().toString() + ":" + periodGranularity.getOrigin());
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

    PeriodQueryGranularity that = (PeriodQueryGranularity) o;

    return periodGranularity.equals(that.periodGranularity);

  }

  @Override
  public int hashCode()
  {
    return periodGranularity.hashCode();
  }

  @Override
  public String toString()
  {
    return "{type=period, " +
           "period=" + getPeriod() +
           ", timeZone=" + getTimeZone() +
           ", origin=" + getOrigin() +
           '}';
  }
}
