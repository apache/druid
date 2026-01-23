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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rule provider that returns a static list of rules defined inline in the configuration.
 * <p>
 * This is the simplest provider implementation, suitable for testing and use cases where the number of rules is
 * relatively small and can be defined directly in the supervisor spec.
 * <p>
 * When filtering rules by interval, this provider only returns rules where {@link ReindexingRule#appliesTo(Interval, DateTime)}
 * returns {@link ReindexingRule.AppliesToMode#FULL}. Rules with partial or no overlap are excluded.
 * <p>
 * For non-additive rule types, when multiple rules fully match an interval, only the rule with the oldest threshold
 * (largest period) is returned. For example, if both a P30D and P90D granularity rule match an interval, the P90D
 * rule is selected because it has the oldest threshold (now - 90 days is older than now - 30 days).
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "type": "inline",
 *   "reindexingFilterRules": [
 *     {
 *       "id": "remove-bots-90d",
 *       "period": "P90D",
 *       "filter": {
 *         "type": "not",
 *         "field": {
 *           "type": "selector",
 *           "dimension": "is_bot",
 *           "value": "true"
 *         }
 *       },
 *       "description": "Remove bot traffic from segments older than 90 days"
 *     },
 *     {
 *       "id": "remove-low-priority-180d",
 *       "period": "P180D",
 *       "filter": {
 *         "type": "not",
 *         "field": {
 *           "type": "in",
 *           "dimension": "priority",
 *           "values": ["low", "spam"]
 *         }
 *       },
 *       "description": "Remove low-priority data from segments older than 180 days"
 *     }
 *   ]
 * }
 * }</pre>
 */
public class InlineReindexingRuleProvider implements ReindexingRuleProvider
{
  public static final String TYPE = "inline";

  private final List<ReindexingFilterRule> reindexingFilterRules;
  private final List<ReindexingMetricsRule> reindexingMetricsRules;
  private final List<ReindexingDimensionsRule> reindexingDimensionsRules;
  private final List<ReindexingIOConfigRule> reindexingIOConfigRules;
  private final List<ReindexingProjectionRule> reindexingProjectionRules;
  private final List<ReindexingGranularityRule> reindexingGranularityRules;
  private final List<ReindexingTuningConfigRule> reindexingTuningConfigRules;


  @JsonCreator
  public InlineReindexingRuleProvider(
      @JsonProperty("reindexingFilterRules") @Nullable List<ReindexingFilterRule> reindexingFilterRules,
      @JsonProperty("reindexingMetricsRules") @Nullable List<ReindexingMetricsRule> reindexingMetricsRules,
      @JsonProperty("reindexingDimensionsRules") @Nullable List<ReindexingDimensionsRule> reindexingDimensionsRules,
      @JsonProperty("reindexingIOConfigRules") @Nullable List<ReindexingIOConfigRule> reindexingIOConfigRules,
      @JsonProperty("reindexingProjectionRules") @Nullable List<ReindexingProjectionRule> reindexingProjectionRules,
      @JsonProperty("reindexingGranularityRules") @Nullable List<ReindexingGranularityRule> reindexingGranularityRules,
      @JsonProperty("reindexingTuningConfigRules") @Nullable List<ReindexingTuningConfigRule> reindexingTuningConfigRules
  )
  {
    this.reindexingFilterRules = Configs.valueOrDefault(reindexingFilterRules, Collections.emptyList());
    this.reindexingMetricsRules = Configs.valueOrDefault(reindexingMetricsRules, Collections.emptyList());
    this.reindexingDimensionsRules = Configs.valueOrDefault(reindexingDimensionsRules, Collections.emptyList());
    this.reindexingIOConfigRules = Configs.valueOrDefault(reindexingIOConfigRules, Collections.emptyList());
    this.reindexingProjectionRules = Configs.valueOrDefault(reindexingProjectionRules, Collections.emptyList());
    this.reindexingGranularityRules = Configs.valueOrDefault(reindexingGranularityRules, Collections.emptyList());
    this.reindexingTuningConfigRules = Configs.valueOrDefault(reindexingTuningConfigRules, Collections.emptyList());
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return TYPE;
  }

  @Override
  @JsonProperty("reindexingFilterRules")
  public List<ReindexingFilterRule> getFilterRules()
  {
    return reindexingFilterRules;
  }

  @Override
  @JsonProperty("reindexingMetricsRules")
  public List<ReindexingMetricsRule> getMetricsRules()
  {
    return reindexingMetricsRules;
  }

  @Override
  @JsonProperty("reindexingDimensionsRules")
  public List<ReindexingDimensionsRule> getDimensionsRules()
  {
    return reindexingDimensionsRules;
  }

  @Override
  @JsonProperty("reindexingIOConfigRules")
  public List<ReindexingIOConfigRule> getIOConfigRules()
  {
    return reindexingIOConfigRules;
  }

  @Override
  @JsonProperty("reindexingProjectionRules")
  public List<ReindexingProjectionRule> getProjectionRules()
  {
    return reindexingProjectionRules;
  }

  @Override
  @JsonProperty("reindexingGranularityRules")
  public List<ReindexingGranularityRule> getGranularityRules()
  {
    return reindexingGranularityRules;
  }

  @Override
  @JsonProperty("reindexingTuningConfigRules")
  public List<ReindexingTuningConfigRule> getTuningConfigRules()
  {
    return reindexingTuningConfigRules;
  }

  @Override
  @Nonnull
  public List<Period> getCondensedAndSortedPeriods(DateTime referenceTime)
  {
    return Stream.of(
                     reindexingFilterRules,
                     reindexingMetricsRules,
                     reindexingDimensionsRules,
                     reindexingIOConfigRules,
                     reindexingProjectionRules,
                     reindexingGranularityRules,
                     reindexingTuningConfigRules
                 )
                 .flatMap(List::stream)
                 .map(ReindexingRule::getPeriod)
                 .distinct()
                 .sorted(Comparator.comparingLong(period -> {
                   DateTime endTime = referenceTime.plus(period);
                   return new Duration(referenceTime, endTime).getMillis();
                 }))
                 .collect(Collectors.toList());

  }

  @Override
  public List<ReindexingFilterRule> getFilterRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(reindexingFilterRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingMetricsRule getMetricsRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(reindexingMetricsRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingDimensionsRule getDimensionsRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(reindexingDimensionsRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingIOConfigRule getIOConfigRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(reindexingIOConfigRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingProjectionRule> getProjectionRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(reindexingProjectionRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingGranularityRule getGranularityRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(reindexingGranularityRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingTuningConfigRule getTuningConfigRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(reindexingTuningConfigRules, interval, referenceTime);
  }

  /**
   * Returns the list of rules that apply to the given interval.
   * <p>
   * This provider implementation only returns rules that fully apply to the given interval.
   * <p>
   */
  private <T extends ReindexingRule> List<T> getApplicableRules(List<T> rules, Interval interval, DateTime referenceTime)
  {
    List<T> applicableRules = new ArrayList<>();
    for (T rule : rules) {
      if (rule.appliesTo(interval, referenceTime) == ReindexingRule.AppliesToMode.FULL) {
        applicableRules.add(rule);
      }
    }
    return applicableRules;
  }

  /**
   * Returns the single most applicable rule for the given interval.
   * <p>
   * "most applicable" means if multiple rules match, the one returned is the one with the oldest
   * threshold (i.e., the largest period into the past from "now").
   */
  @Nullable
  private <T extends ReindexingRule> T getApplicableRule(List<T> rules, Interval interval, DateTime referenceTime)
  {
    List<T> applicableRules = new ArrayList<>();
    for (T rule : rules) {
      if (rule.appliesTo(interval, referenceTime) == ReindexingRule.AppliesToMode.FULL) {
        applicableRules.add(rule);
      }
    }

    if (applicableRules.isEmpty()) {
      return null;
    }

    return Collections.min(
        applicableRules,
        Comparator.comparingLong(r -> {
          DateTime threshold = referenceTime.minus(r.getPeriod());
          return threshold.getMillis();
        })
    );
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
    InlineReindexingRuleProvider that = (InlineReindexingRuleProvider) o;
    return Objects.equals(reindexingFilterRules, that.reindexingFilterRules)
           && Objects.equals(reindexingMetricsRules, that.reindexingMetricsRules)
           && Objects.equals(reindexingDimensionsRules, that.reindexingDimensionsRules)
           && Objects.equals(reindexingIOConfigRules, that.reindexingIOConfigRules)
           && Objects.equals(reindexingProjectionRules, that.reindexingProjectionRules)
           && Objects.equals(reindexingGranularityRules, that.reindexingGranularityRules)
           && Objects.equals(reindexingTuningConfigRules, that.reindexingTuningConfigRules);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        reindexingFilterRules,
        reindexingMetricsRules,
        reindexingDimensionsRules,
        reindexingIOConfigRules,
        reindexingProjectionRules,
        reindexingGranularityRules,
        reindexingTuningConfigRules
    );
  }

  @Override
  public String toString()
  {
    return "InlineReindexingRuleProvider{"
           + "reindexingFilterRules=" + reindexingFilterRules
           + ", reindexingMetricsRules=" + reindexingMetricsRules
           + ", reindexingDimensionsRules=" + reindexingDimensionsRules
           + ", reindexingIOConfigRules=" + reindexingIOConfigRules
           + ", reindexingProjectionRules=" + reindexingProjectionRules
           + ", reindexingGranularityRules=" + reindexingGranularityRules
           + ", reindexingTuningConfigRules=" + reindexingTuningConfigRules
           + '}';
  }

  public static class Builder
  {
    private List<ReindexingFilterRule> reindexingFilterRules;
    private List<ReindexingMetricsRule> reindexingMetricsRules;
    private List<ReindexingDimensionsRule> reindexingDimensionsRules;
    private List<ReindexingIOConfigRule> reindexingIOConfigRules;
    private List<ReindexingProjectionRule> reindexingProjectionRules;
    private List<ReindexingGranularityRule> reindexingGranularityRules;
    private List<ReindexingTuningConfigRule> reindexingTuningConfigRules;

    public Builder filterRules(List<ReindexingFilterRule> reindexingFilterRules)
    {
      this.reindexingFilterRules = reindexingFilterRules;
      return this;
    }

    public Builder metricsRules(List<ReindexingMetricsRule> reindexingMetricsRules)
    {
      this.reindexingMetricsRules = reindexingMetricsRules;
      return this;
    }

    public Builder dimensionsRules(List<ReindexingDimensionsRule> reindexingDimensionsRules)
    {
      this.reindexingDimensionsRules = reindexingDimensionsRules;
      return this;
    }

    public Builder ioConfigRules(List<ReindexingIOConfigRule> reindexingIOConfigRules)
    {
      this.reindexingIOConfigRules = reindexingIOConfigRules;
      return this;
    }

    public Builder projectionRules(List<ReindexingProjectionRule> reindexingProjectionRules)
    {
      this.reindexingProjectionRules = reindexingProjectionRules;
      return this;
    }

    public Builder granularityRules(List<ReindexingGranularityRule> reindexingGranularityRules)
    {
      this.reindexingGranularityRules = reindexingGranularityRules;
      return this;
    }

    public Builder tuningConfigRules(List<ReindexingTuningConfigRule> reindexingTuningConfigRules)
    {
      this.reindexingTuningConfigRules = reindexingTuningConfigRules;
      return this;
    }

    public InlineReindexingRuleProvider build()
    {
      return new InlineReindexingRuleProvider(
          reindexingFilterRules,
          reindexingMetricsRules,
          reindexingDimensionsRules,
          reindexingIOConfigRules,
          reindexingProjectionRules,
          reindexingGranularityRules,
          reindexingTuningConfigRules
      );
    }
  }
}
