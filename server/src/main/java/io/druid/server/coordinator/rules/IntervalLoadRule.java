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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public class IntervalLoadRule extends LoadRule
{
  private static final Logger log = new Logger(IntervalLoadRule.class);

  private final Interval interval;
  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public IntervalLoadRule(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants
  )
  {
    validateTieredReplicants(tieredReplicants);
    this.interval = interval;
    this.tieredReplicants = tieredReplicants;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByInterval";
  }

  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @Override
  public int getNumReplicants(String tier)
  {
    final Integer retVal = tieredReplicants.get(tier);
    return retVal == null ? 0 : retVal;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval theInterval, DateTime referenceTimestamp)
  {
    return interval.contains(theInterval);
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

    IntervalLoadRule that = (IntervalLoadRule) o;

    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }
    if (tieredReplicants != null ? !tieredReplicants.equals(that.tieredReplicants) : that.tieredReplicants != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = interval != null ? interval.hashCode() : 0;
    result = 31 * result + (tieredReplicants != null ? tieredReplicants.hashCode() : 0);
    return result;
  }
}
