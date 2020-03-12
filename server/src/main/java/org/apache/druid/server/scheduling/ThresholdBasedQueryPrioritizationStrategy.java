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

package org.apache.druid.server.scheduling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryPrioritizationStrategy;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

/**
 * Lowers query priority when any of the configured thresholds is exceeded
 */
public class ThresholdBasedQueryPrioritizationStrategy implements QueryPrioritizationStrategy
{
  private static final int DEFAULT_SEGMENT_THRESHOLD = Integer.MAX_VALUE;
  private static final int DEFAULT_ADJUSTMENT = 5;

  private final int segmentCountThreshold;
  private final int adjustment;

  private final Optional<Duration> periodThreshold;
  private final Optional<Duration> durationThreshold;

  @JsonCreator
  public ThresholdBasedQueryPrioritizationStrategy(
      @JsonProperty("periodThreshold") @Nullable String periodThresholdString,
      @JsonProperty("durationThreshold") @Nullable String durationThresholdString,
      @JsonProperty("segmentCountThreshold") @Nullable Integer segmentCountThreshold,
      @JsonProperty("adjustment") @Nullable Integer adjustment
  )
  {
    this.segmentCountThreshold = segmentCountThreshold == null ? DEFAULT_SEGMENT_THRESHOLD : segmentCountThreshold;
    this.adjustment = adjustment == null ? DEFAULT_ADJUSTMENT : adjustment;
    this.periodThreshold = periodThresholdString == null
                           ? Optional.empty()
                           : Optional.of(new Period(periodThresholdString).toDurationFrom(DateTimes.nowUtc()));
    this.durationThreshold = durationThresholdString == null
                             ? Optional.empty()
                             : Optional.of(new Period(durationThresholdString).toStandardDuration());
    Preconditions.checkArgument(
        segmentCountThreshold != null || periodThreshold.isPresent() || durationThreshold.isPresent(),
        "periodThreshold, durationThreshold, or segmentCountThreshold must be set"
    );
  }

  @Override
  public <T> Optional<Integer> computePriority(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    Query<T> theQuery = query.getQuery();
    final boolean violatesPeriodThreshold = periodThreshold.map(duration -> {
      final DateTime periodThresholdStartDate = DateTimes.nowUtc().minus(duration);
      return theQuery.getIntervals()
                     .stream()
                     .anyMatch(interval -> interval.getStart().isBefore(periodThresholdStartDate));
    }).orElse(false);
    final boolean violatesDurationThreshold =
        durationThreshold.map(duration -> theQuery.getDuration().isLongerThan(duration)).orElse(false);
    boolean violatesSegmentThreshold = segments.size() > segmentCountThreshold;

    if (violatesPeriodThreshold || violatesDurationThreshold || violatesSegmentThreshold) {
      final int adjustedPriority = QueryContexts.getPriority(theQuery) - adjustment;
      return Optional.of(adjustedPriority);
    }
    return Optional.empty();
  }
}
