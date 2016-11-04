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
import com.google.common.collect.ImmutableMap;

import io.druid.client.DruidServer;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Map;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);

  private final Period period;
  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") Period period,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants
  )
  {
    this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : tieredReplicants;
    validateTieredReplicants(this.tieredReplicants);
    this.period = period;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @Override
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

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    final Interval currInterval = new Interval(period, referenceTimestamp);
    return currInterval.overlaps(interval) && interval.getStartMillis() >= currInterval.getStartMillis();
  }
}
