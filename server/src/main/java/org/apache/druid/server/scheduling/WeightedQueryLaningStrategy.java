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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;
import org.apache.druid.server.QueryScheduler;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.base.AbstractInterval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Query laning strategy that assigns a cost to queries based on how many configured thresholds
 * they breach, then assigns them to the most restrictive matching lane. This provides more nuanced
 * lane assignment than {@link HiLoQueryLaningStrategy}, which uses a binary high/low split.
 *
 * <p>Configuration example:
 * <pre>{@code
 * {
 *   "strategy": "weighted",
 *   "periodThreshold": "P1M",
 *   "segmentCountThreshold": 1000,
 *   "lanes": {
 *     "low": { "minCost": 1, "maxPercent": 30 },
 *     "very-low": { "minCost": 3, "maxPercent": 10 }
 *   }
 * }
 * }</pre>
 */
public class WeightedQueryLaningStrategy implements QueryLaningStrategy
{
  @JsonProperty
  @Nullable
  private final Integer segmentCountThreshold;
  // Stored as the original config strings so that Jackson round-trip serde is consistent:
  // the @JsonCreator constructor accepts String parameters, so serialization must also emit
  // strings. Storing Period/Duration objects directly with @JsonProperty would cause Jackson
  // to serialize them as complex JSON objects that the constructor cannot deserialize back.
  @JsonProperty("periodThreshold")
  @Nullable
  private final String periodThresholdString;
  @JsonProperty("durationThreshold")
  @Nullable
  private final String durationThresholdString;
  @JsonProperty("segmentRangeThreshold")
  @Nullable
  private final String segmentRangeThresholdString;

  // Parsed from the string fields above at construction time; not serialized.
  @Nullable
  private final Period periodThreshold;
  @Nullable
  private final Duration durationThreshold;
  @Nullable
  private final Duration segmentRangeThreshold;

  @JsonProperty
  private final Map<String, LaneConfig> lanes;

  // Optional per-threshold weight: how much a breach of that threshold adds to the query's cost. Null means the
  // default weight of 1 (the original flat scoring). Lets a breach on one dimension count for more than another.
  @JsonProperty("periodWeight")
  @Nullable
  private final Integer periodWeight;
  @JsonProperty("durationWeight")
  @Nullable
  private final Integer durationWeight;
  @JsonProperty("segmentCountWeight")
  @Nullable
  private final Integer segmentCountWeight;
  @JsonProperty("segmentRangeWeight")
  @Nullable
  private final Integer segmentRangeWeight;

  @JsonCreator
  public WeightedQueryLaningStrategy(
      @JsonProperty("periodThreshold") @Nullable String periodThresholdString,
      @JsonProperty("durationThreshold") @Nullable String durationThresholdString,
      @JsonProperty("segmentCountThreshold") @Nullable Integer segmentCountThreshold,
      @JsonProperty("segmentRangeThreshold") @Nullable String segmentRangeThresholdString,
      @JsonProperty("lanes") Map<String, LaneConfig> lanes,
      @JsonProperty("periodWeight") @Nullable Integer periodWeight,
      @JsonProperty("durationWeight") @Nullable Integer durationWeight,
      @JsonProperty("segmentCountWeight") @Nullable Integer segmentCountWeight,
      @JsonProperty("segmentRangeWeight") @Nullable Integer segmentRangeWeight
  )
  {
    final Period parsedPeriod;
    if (periodThresholdString == null) {
      parsedPeriod = null;
    } else {
      try {
        parsedPeriod = new Period(periodThresholdString);
      }
      catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "periodThreshold is not a valid ISO 8601 period, got [" + periodThresholdString + "]"
        );
      }
    }
    final Duration parsedDuration;
    if (durationThresholdString == null) {
      parsedDuration = null;
    } else {
      try {
        parsedDuration = new Period(durationThresholdString).toStandardDuration();
      }
      catch (IllegalArgumentException | UnsupportedOperationException e) {
        throw new IllegalArgumentException(
            "durationThreshold is not a valid ISO 8601 duration, "
            + "got [" + durationThresholdString + "]"
        );
      }
    }
    final Duration parsedSegmentRange;
    if (segmentRangeThresholdString == null) {
      parsedSegmentRange = null;
    } else {
      try {
        parsedSegmentRange = new Period(segmentRangeThresholdString).toStandardDuration();
      }
      catch (IllegalArgumentException | UnsupportedOperationException e) {
        throw new IllegalArgumentException(
            "segmentRangeThreshold is not a valid ISO 8601 duration, "
            + "got [" + segmentRangeThresholdString + "]"
        );
      }
    }

    Preconditions.checkArgument(
        segmentCountThreshold != null || parsedPeriod != null || parsedDuration != null || parsedSegmentRange != null,
        "At least one of periodThreshold, durationThreshold, segmentCountThreshold, or segmentRangeThreshold must be set"
    );
    Preconditions.checkArgument(
        segmentCountThreshold == null || segmentCountThreshold > 0,
        "segmentCountThreshold must be > 0, got [%s]", segmentCountThreshold
    );
    if (parsedDuration != null) {
      Preconditions.checkArgument(parsedDuration.getMillis() > 0, "durationThreshold must be positive, got [%s]", durationThresholdString);
    }
    if (parsedSegmentRange != null) {
      Preconditions.checkArgument(parsedSegmentRange.getMillis() > 0, "segmentRangeThreshold must be positive, got [%s]", segmentRangeThresholdString);
    }
    if (parsedPeriod != null) {
      DateTime now = DateTimes.nowUtc();
      Preconditions.checkArgument(
          now.minus(parsedPeriod.toDurationFrom(now)).isBefore(now),
          "periodThreshold must be positive, got [%s]", periodThresholdString
      );
    }
    Preconditions.checkArgument(
        lanes != null && !lanes.isEmpty(),
        "At least one lane must be defined"
    );
    Preconditions.checkArgument(
        !lanes.containsKey(QueryScheduler.TOTAL),
        "Lane cannot be named '%s'", QueryScheduler.TOTAL
    );
    Preconditions.checkArgument(
        !lanes.containsKey(QueryScheduler.DEFAULT),
        "Lane cannot be named '%s'", QueryScheduler.DEFAULT
    );
    long distinctCosts = lanes.values().stream().mapToInt(LaneConfig::getMinCost).distinct().count();
    Preconditions.checkArgument(
        distinctCosts == lanes.size(),
        "Each lane must have a unique minCost so that lane selection is deterministic "
        + "(a query is assigned to the lane with the highest minCost it meets; equal costs "
        + "produce non-deterministic results). Found duplicate minCost values in lanes: [%s]",
        lanes
    );
    // Each weight, if set, must be >= 1 and must correspond to a threshold that is actually configured
    // (otherwise the weight is silently inert, which is almost certainly a misconfiguration).
    validateWeight("periodWeight", periodWeight, parsedPeriod != null, "periodThreshold");
    validateWeight("durationWeight", durationWeight, parsedDuration != null, "durationThreshold");
    validateWeight("segmentCountWeight", segmentCountWeight, segmentCountThreshold != null, "segmentCountThreshold");
    validateWeight("segmentRangeWeight", segmentRangeWeight, parsedSegmentRange != null, "segmentRangeThreshold");

    this.segmentCountThreshold = segmentCountThreshold;
    this.periodThresholdString = periodThresholdString;
    this.durationThresholdString = durationThresholdString;
    this.segmentRangeThresholdString = segmentRangeThresholdString;
    this.periodThreshold = parsedPeriod;
    this.durationThreshold = parsedDuration;
    this.segmentRangeThreshold = parsedSegmentRange;
    this.lanes = lanes;
    this.periodWeight = periodWeight;
    this.durationWeight = durationWeight;
    this.segmentCountWeight = segmentCountWeight;
    this.segmentRangeWeight = segmentRangeWeight;
  }

  /**
   * Convenience constructor without per-threshold weights (every breach adds 1). Retained so existing callers
   * and tests need not pass the optional weight parameters.
   */
  @VisibleForTesting
  public WeightedQueryLaningStrategy(
      @Nullable String periodThresholdString,
      @Nullable String durationThresholdString,
      @Nullable Integer segmentCountThreshold,
      @Nullable String segmentRangeThresholdString,
      Map<String, LaneConfig> lanes
  )
  {
    this(periodThresholdString, durationThresholdString, segmentCountThreshold, segmentRangeThresholdString, lanes,
         null, null, null, null);
  }

  private static void validateWeight(String name, @Nullable Integer weight, boolean thresholdSet, String thresholdName)
  {
    if (weight == null) {
      return;
    }
    Preconditions.checkArgument(weight >= 1, "%s must be >= 1, got [%s]", name, weight);
    Preconditions.checkArgument(
        thresholdSet,
        "%s is set but %s is not configured, so the weight would have no effect", name, thresholdName
    );
  }

  /**
   * Weight (cost contribution) for a breached threshold; defaults to 1 when not configured.
   */
  private static int weightOrDefault(@Nullable Integer weight)
  {
    return weight == null ? 1 : weight;
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

    int cost = computeCost(query.getQuery(), segments);
    if (cost == 0) {
      return Optional.empty();
    }

    // Find the lane with the highest minCost that this query meets
    String highestLane = null;
    int highestMinCost = 0;
    for (Map.Entry<String, LaneConfig> entry : lanes.entrySet()) {
      int minCost = entry.getValue().minCost;
      if (cost >= minCost && minCost > highestMinCost) {
        highestLane = entry.getKey();
        highestMinCost = minCost;
      }
    }
    return Optional.ofNullable(highestLane);
  }

  private <T> int computeCost(Query<T> query, Set<SegmentServerSelector> segments)
  {
    int cost = 0;

    if (periodThreshold != null) {
      final DateTime now = DateTimes.nowUtc();
      final DateTime cutoff = now.minus(periodThreshold.toDurationFrom(now));
      // Query intervals are condensed and sorted ascending by start (see JodaUtils.condenseIntervals, applied by
      // every QuerySegmentSpec), so the earliest start is the first interval. Checking only it avoids scanning a
      // query with hundreds of intervals.
      final List<Interval> intervals = query.getIntervals();
      if (!intervals.isEmpty() && intervals.get(0).getStart().isBefore(cutoff)) {
        cost += weightOrDefault(periodWeight);
      }
    }

    if (durationThreshold != null && query.getDuration().isLongerThan(durationThreshold)) {
      cost += weightOrDefault(durationWeight);
    }

    if (segmentCountThreshold != null && segments.size() > segmentCountThreshold) {
      cost += weightOrDefault(segmentCountWeight);
    }

    if (segmentRangeThreshold != null) {
      long segmentRangeMs = segments.stream()
                                    .filter(s -> s.getSegmentDescriptor() != null)
                                    .map(s -> s.getSegmentDescriptor().getInterval())
                                    .distinct()
                                    .mapToLong(AbstractInterval::toDurationMillis)
                                    .sum();
      if (segmentRangeMs > segmentRangeThreshold.getMillis()) {
        cost += weightOrDefault(segmentRangeWeight);
      }
    }

    return cost;
  }

  public static class LaneConfig
  {
    private final int minCost;
    private final int maxPercent;

    @JsonCreator
    public LaneConfig(
        @JsonProperty("minCost") int minCost,
        @JsonProperty("maxPercent") int maxPercent
    )
    {
      Preconditions.checkArgument(minCost > 0, "minCost must be > 0, got [%s]", minCost);
      Preconditions.checkArgument(
          maxPercent > 0 && maxPercent <= 100,
          "maxPercent must be in the range 1 to 100, got [%s]", maxPercent
      );
      this.minCost = minCost;
      this.maxPercent = maxPercent;
    }

    @JsonProperty
    public int getMinCost()
    {
      return minCost;
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
      return minCost == that.minCost && maxPercent == that.maxPercent;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(minCost, maxPercent);
    }
  }
}
