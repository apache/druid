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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

public class IntervalBroadcastDistributionRule extends BroadcastDistributionRule
{
  static final String TYPE = "broadcastByInterval";
  private final Interval interval;
  private final List<String> colocatedDataSources;

  @JsonCreator
  public IntervalBroadcastDistributionRule(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("colocatedDataSources") List<String> colocatedDataSources
  )
  {
    this.interval = interval;
    this.colocatedDataSources = colocatedDataSources;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @Override
  @JsonProperty
  public List<String> getColocatedDataSources()
  {
    return colocatedDataSources;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return Rules.eligibleForLoad(this.interval, interval);
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || o.getClass() != getClass()) {
      return false;
    }

    IntervalBroadcastDistributionRule that = (IntervalBroadcastDistributionRule) o;

    if (!Objects.equals(interval, that.interval)) {
      return false;
    }

    return Objects.equals(colocatedDataSources, that.colocatedDataSources);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getType(), interval, colocatedDataSources);
  }
}
