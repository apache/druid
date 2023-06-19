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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Map;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);
  static final boolean DEFAULT_INCLUDE_FUTURE = true;

  private final Period period;
  private final boolean includeFuture;
  private final Map<String, Integer> tieredReplicants;
  /**
   * Compatibility flag for load rules. If the flag is false or not present, null or empty {@link #tieredReplicants}
   * will be understood as the default load rule of {@link DruidServer#DEFAULT_NUM_REPLICANTS} replicants
   * for @{@link DruidServer#DEFAULT_TIER}.
   * <br>
   * If the flag is true, it will be kept as an empty map. This will enable the new behaviour of not loading segments
   * to a historical unless the tier and number of replicants are explicitly specified.
   */
  private final boolean allowEmptyTieredReplicants;

  public PeriodLoadRule(Period period, Boolean includeFuture, Map<String, Integer> tieredReplicants)
  {
    this(period, includeFuture, tieredReplicants, null);
  }

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") Period period,
      @JsonProperty("includeFuture") Boolean includeFuture,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants,
      @JsonProperty("allowEmptyTieredReplicants") Boolean allowEmptyTieredReplicants
  )
  {
    this.allowEmptyTieredReplicants = allowEmptyTieredReplicants != null && allowEmptyTieredReplicants;

    if (!this.allowEmptyTieredReplicants) {
      this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : tieredReplicants;
    } else {
      this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of() : tieredReplicants;
    }
    validateTieredReplicants(this.tieredReplicants);
    this.period = period;
    this.includeFuture = includeFuture == null ? DEFAULT_INCLUDE_FUTURE : includeFuture;
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

  @JsonProperty
  public boolean isIncludeFuture()
  {
    return includeFuture;
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

  @JsonProperty
  public boolean isAllowEmptyTieredReplicants()
  {
    return allowEmptyTieredReplicants;
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
}
