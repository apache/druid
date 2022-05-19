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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Objects;

public class PeriodBroadcastDistributionRule extends BroadcastDistributionRule
{
  static final String TYPE = "broadcastByPeriod";
  private static final boolean DEFAULT_INCLUDE_FUTURE = true;

  private final Period period;
  private final boolean includeFuture;

  @JsonCreator
  public PeriodBroadcastDistributionRule(
      @JsonProperty("period") Period period,
      @JsonProperty("includeFuture") Boolean includeFuture
  )
  {
    this.period = period;
    this.includeFuture = includeFuture == null ? DEFAULT_INCLUDE_FUTURE : includeFuture;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return Rules.eligibleForLoad(period, interval, referenceTimestamp, includeFuture);
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty
  public boolean isIncludeFuture()
  {
    return includeFuture;
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
    PeriodBroadcastDistributionRule that = (PeriodBroadcastDistributionRule) o;
    return isIncludeFuture() == that.isIncludeFuture() &&
           Objects.equals(getPeriod(), that.getPeriod());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getPeriod(), isIncludeFuture());
  }
}
