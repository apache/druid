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
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.base.AbstractInterval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Query laning strategy that scores queries by how many configured thresholds they breach,
 * then assigns them to the most restrictive matching lane. This provides more nuanced lane
 * assignment than {@link HiLoQueryLaningStrategy}, which uses a binary high/low split.
 *
 * <p>Configuration example:
 * <pre>{@code
 * {
 *   "strategy": "weighted",
 *   "periodThreshold": "P1M",
 *   "segmentCountThreshold": 1000,
 *   "lanes": {
 *     "low": { "minScore": 1, "maxPercent": 30 },
 *     "very-low": { "minScore": 3, "maxPercent": 10 }
 *   }
 * }
 * }</pre>
 */
public class WeightedQueryLaningStrategy implements QueryLaningStrategy
{
  private static final int DEFAULT_SEGMENT_THRESHOLD = Integer.MAX_VALUE;

  private final int segmentCountThreshold;
  @Nullable
  private final Period periodThreshold;
  @Nullable
  private final Duration durationThreshold;
  @Nullable
  private final Duration segmentRangeThreshold;

  @JsonProperty
  private final Map<String, LaneConfig> lanes;

  @JsonCreator
  public WeightedQueryLaningStrategy(
      @JsonProperty("periodThreshold") @Nullable String periodThreshold,
      @JsonProperty("durationThreshold") @Nullable String durationThreshold,
      @JsonProperty("segmentCountThreshold") @Nullable Integer segmentCountThreshold,
      @JsonProperty("segmentRangeThreshold") @Nullable String segmentRangeThreshold,
      @JsonProperty("lanes") Map<String, LaneConfig> lanes
  )
  {
    Preconditions.checkArgument(
        segmentCountThreshold != null || periodThreshold != null || durationThreshold != null || segmentRangeThreshold != null,
        "At least one of periodThreshold, durationThreshold, segmentCountThreshold, or segmentRangeThreshold must be set"
    );
    Preconditions.checkArgument(
        lanes != null && !lanes.isEmpty(),
        "At least one lane must be defined"
    );

    this.segmentCountThreshold = segmentCountThreshold == null ? DEFAULT_SEGMENT_THRESHOLD : segmentCountThreshold;
    this.periodThreshold = periodThreshold == null ? null : new Period(periodThreshold);
    this.durationThreshold = durationThreshold == null
                             ? null
                             : new Period(durationThreshold).toStandardDuration();
    this.segmentRangeThreshold = segmentRangeThreshold == null
                                 ? null
                                 : new Period(segmentRangeThreshold).toStandardDuration();
    this.lanes = lanes;
  }

  @Override
  public Object2IntMap<String> getLaneLimits(int totalLimit)
  {
    Object2IntMap<String> limits = new Object2IntArrayMap<>(lanes.size());
    for (Map.Entry<String, LaneConfig> entry : lanes.entrySet()) {
      limits.put(entry.getKey(), computeLimitFromPercent(totalLimit, entry.getValue().maxPercent));
    }
    return limits;
  }

  @Override
  public <T> Optional<String> computeLane(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    final String existingLane = query.getQuery().context().getLane();
    if (existingLane != null) {
      return Optional.of(existingLane);
    }

    int score = computeScore(query.getQuery(), segments);
    if (score == 0) {
      return Optional.empty();
    }

    // Find the lane with the highest minScore that this query meets
    String bestLane = null;
    int bestMinScore = 0;
    for (Map.Entry<String, LaneConfig> entry : lanes.entrySet()) {
      int minScore = entry.getValue().minScore;
      if (score >= minScore && minScore > bestMinScore) {
        bestLane = entry.getKey();
        bestMinScore = minScore;
      }
    }
    return Optional.ofNullable(bestLane);
  }

  private <T> int computeScore(Query<T> query, Set<SegmentServerSelector> segments)
  {
    int score = 0;

    if (periodThreshold != null) {
      final DateTime now = DateTimes.nowUtc();
      final DateTime cutoff = now.minus(periodThreshold.toDurationFrom(now));
      if (query.getIntervals().stream().anyMatch(interval -> interval.getStart().isBefore(cutoff))) {
        score++;
      }
    }

    if (durationThreshold != null && query.getDuration().isLongerThan(durationThreshold)) {
      score++;
    }

    if (segments.size() > segmentCountThreshold) {
      score++;
    }

    if (segmentRangeThreshold != null) {
      long segmentRangeMs = segments.stream()
                                    .filter(s -> s.getSegmentDescriptor() != null)
                                    .map(s -> s.getSegmentDescriptor().getInterval())
                                    .distinct()
                                    .mapToLong(AbstractInterval::toDurationMillis)
                                    .sum();
      if (new Duration(segmentRangeMs).isLongerThan(segmentRangeThreshold)) {
        score++;
      }
    }

    return score;
  }

  public static class LaneConfig
  {
    private final int minScore;
    private final int maxPercent;

    @JsonCreator
    public LaneConfig(
        @JsonProperty("minScore") int minScore,
        @JsonProperty("maxPercent") int maxPercent
    )
    {
      Preconditions.checkArgument(minScore > 0, "minScore must be > 0, got [%s]", minScore);
      Preconditions.checkArgument(
          maxPercent > 0 && maxPercent <= 100,
          "maxPercent must be in the range 1 to 100, got [%s]", maxPercent
      );
      this.minScore = minScore;
      this.maxPercent = maxPercent;
    }

    @JsonProperty
    public int getMinScore()
    {
      return minScore;
    }

    @JsonProperty
    public int getMaxPercent()
    {
      return maxPercent;
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
      LaneConfig that = (LaneConfig) o;
      return minScore == that.minScore && maxPercent == that.maxPercent;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(minScore, maxPercent);
    }
  }
}
