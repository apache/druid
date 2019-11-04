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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);
  static final boolean DEFAULT_INCLUDE_FUTURE = true;

  private final Period period;
  private final Optional<String> identifierPrefix;
  private final boolean includeFuture;
  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") Period period,
      @Nullable @JsonProperty("identifierPrefix") String identifierPrefix,
      @JsonProperty("includeFuture") Boolean includeFuture,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants
  )
  {
    this.tieredReplicants = tieredReplicants == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : tieredReplicants;
    validateTieredReplicants(this.tieredReplicants);
    this.period = period;
    this.identifierPrefix = identifierPrefix == null || identifierPrefix.isEmpty()
                            ? Optional.absent()
                            : Optional.of(identifierPrefix);
    this.includeFuture = includeFuture == null ? DEFAULT_INCLUDE_FUTURE : includeFuture;
  }

  public PeriodLoadRule(
      Period period,
      Boolean includeFuture,
      Map<String, Integer> tieredReplicants
  )
  {
    this(period, null, includeFuture, tieredReplicants);
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByPeriod";
  }

  @Nullable
  @JsonProperty
  public String getIdentifierPrefix()
  {
    return identifierPrefix.orNull();
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

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return (!identifierPrefix.isPresent() || (segment.getShardSpec()
                                                     .getIdentifier()
                                                     .toString()
                                                     .startsWith(identifierPrefix.get())))
           && appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    return Rules.eligibleForLoad(period, interval, referenceTimestamp, includeFuture);
  }
}
