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
 * relatively small and can be defined directly in the compaction config.
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
 *   "compactionFilterRules": [
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

  private final List<ReindexingFilterRule> compactionFilterRules;
  private final List<ReindexingMetricsRule> compactionMetricsRules;
  private final List<ReindexingDimensionsRule> compactionDimensionsRules;
  private final List<ReindexingIOConfigRule> compactionIOConfigRules;
  private final List<ReindexingProjectionRule> compactionProjectionRules;
  private final List<ReindexingGranularityRule> compactionGranularityRules;
  private final List<ReindexingTuningConfigRule> compactionTuningConfigRules;


  @JsonCreator
  public InlineReindexingRuleProvider(
      @JsonProperty("compactionFilterRules") @Nullable List<ReindexingFilterRule> compactionFilterRules,
      @JsonProperty("compactionMetricsRules") @Nullable List<ReindexingMetricsRule> compactionMetricsRules,
      @JsonProperty("compactionDimensionsRules") @Nullable List<ReindexingDimensionsRule> compactionDimensionsRules,
      @JsonProperty("compactionIOConfigRules") @Nullable List<ReindexingIOConfigRule> compactionIOConfigRules,
      @JsonProperty("compactionProjectionRules") @Nullable List<ReindexingProjectionRule> compactionProjectionRules,
      @JsonProperty("compactionGranularityRules") @Nullable List<ReindexingGranularityRule> compactionGranularityRules,
      @JsonProperty("compactionTuningConfigRules") @Nullable List<ReindexingTuningConfigRule> compactionTuningConfigRules
  )
  {
    this.compactionFilterRules = Configs.valueOrDefault(compactionFilterRules, Collections.emptyList());
    this.compactionMetricsRules = Configs.valueOrDefault(compactionMetricsRules, Collections.emptyList());
    this.compactionDimensionsRules = Configs.valueOrDefault(compactionDimensionsRules, Collections.emptyList());
    this.compactionIOConfigRules = Configs.valueOrDefault(compactionIOConfigRules, Collections.emptyList());
    this.compactionProjectionRules = Configs.valueOrDefault(compactionProjectionRules, Collections.emptyList());
    this.compactionGranularityRules = Configs.valueOrDefault(compactionGranularityRules, Collections.emptyList());
    this.compactionTuningConfigRules = Configs.valueOrDefault(compactionTuningConfigRules, Collections.emptyList());
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
  @JsonProperty("compactionFilterRules")
  public List<ReindexingFilterRule> getFilterRules()
  {
    return compactionFilterRules;
  }

  @Override
  @JsonProperty("compactionMetricsRules")
  public List<ReindexingMetricsRule> getMetricsRules()
  {
    return compactionMetricsRules;
  }

  @Override
  @JsonProperty("compactionDimensionsRules")
  public List<ReindexingDimensionsRule> getDimensionsRules()
  {
    return compactionDimensionsRules;
  }

  @Override
  @JsonProperty("compactionIOConfigRules")
  public List<ReindexingIOConfigRule> getIOConfigRules()
  {
    return compactionIOConfigRules;
  }

  @Override
  @JsonProperty("compactionProjectionRules")
  public List<ReindexingProjectionRule> getProjectionRules()
  {
    return compactionProjectionRules;
  }

  @Override
  @JsonProperty("compactionGranularityRules")
  public List<ReindexingGranularityRule> getGranularityRules()
  {
    return compactionGranularityRules;
  }

  @Override
  @JsonProperty("compactionTuningConfigRules")
  public List<ReindexingTuningConfigRule> getTuningConfigRules()
  {
    return compactionTuningConfigRules;
  }

  @Override
  public List<Period> getCondensedAndSortedPeriods(DateTime referenceTime)
  {
    return Stream.of(
                     compactionFilterRules,
                     compactionMetricsRules,
                     compactionDimensionsRules,
                     compactionIOConfigRules,
                     compactionProjectionRules,
                     compactionGranularityRules,
                     compactionTuningConfigRules
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
    return getApplicableRules(compactionFilterRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingMetricsRule> getMetricsRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionMetricsRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingDimensionsRule> getDimensionsRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionDimensionsRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingIOConfigRule> getIOConfigRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionIOConfigRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingProjectionRule> getProjectionRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionProjectionRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingGranularityRule> getGranularityRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionGranularityRules, interval, referenceTime);
  }

  @Override
  public List<ReindexingTuningConfigRule> getTuningConfigRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(compactionTuningConfigRules, interval, referenceTime);
  }

  /**
   * Returns the list of rules that apply to the given interval.
   * <p>
   * This provider implementation only returns rules that fully apply to the given interval.
   * <p>
   * Any non-additive rule types will only return a single rule, even if multiple rules fully apply to the interval. The
   * interval returned is the one with the oldest threshold (i.e., the largest period into the past from "now").
   */
  private <T extends ReindexingRule> List<T> getApplicableRules(List<T> rules, Interval interval, DateTime referenceTime)
  {
    boolean areRulesAdditive = false;
    List<T> applicableRules = new ArrayList<>();
    for (T rule : rules) {
      areRulesAdditive = rule.isAdditive();
      if (rule.appliesTo(interval, referenceTime) == ReindexingRule.AppliesToMode.FULL) {
        applicableRules.add(rule);
      }
    }
    if (!areRulesAdditive && applicableRules.size() > 1) {
      // if rules are not additive, I want the period where (referenceTime - period) is the oldest date of all the rules
      T selectedRule = Collections.min(
          applicableRules,
          Comparator.comparingLong(r -> {
            DateTime threshold = referenceTime.minus(r.getPeriod());
            return threshold.getMillis();
          })
      );
      applicableRules = List.of(selectedRule);
    }
    return applicableRules;
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
    return Objects.equals(compactionFilterRules, that.compactionFilterRules)
           && Objects.equals(compactionMetricsRules, that.compactionMetricsRules)
           && Objects.equals(compactionDimensionsRules, that.compactionDimensionsRules)
           && Objects.equals(compactionIOConfigRules, that.compactionIOConfigRules)
           && Objects.equals(compactionProjectionRules, that.compactionProjectionRules)
           && Objects.equals(compactionGranularityRules, that.compactionGranularityRules)
           && Objects.equals(compactionTuningConfigRules, that.compactionTuningConfigRules);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        compactionFilterRules,
        compactionMetricsRules,
        compactionDimensionsRules,
        compactionIOConfigRules,
        compactionProjectionRules,
        compactionGranularityRules,
        compactionTuningConfigRules
    );
  }

  @Override
  public String toString()
  {
    return "InlineReindexingRuleProvider{"
           + "compactionFilterRules=" + compactionFilterRules
           + ", compactionMetricsRules=" + compactionMetricsRules
           + ", compactionDimensionsRules=" + compactionDimensionsRules
           + ", compactionIOConfigRules=" + compactionIOConfigRules
           + ", compactionProjectionRules=" + compactionProjectionRules
           + ", compactionGranularityRules=" + compactionGranularityRules
           + ", compactionTuningConfigRules=" + compactionTuningConfigRules
           + '}';
  }

  public static class Builder
  {
    private List<ReindexingFilterRule> compactionFilterRules;
    private List<ReindexingMetricsRule> compactionMetricsRules;
    private List<ReindexingDimensionsRule> compactionDimensionsRules;
    private List<ReindexingIOConfigRule> compactionIOConfigRules;
    private List<ReindexingProjectionRule> compactionProjectionRules;
    private List<ReindexingGranularityRule> compactionGranularityRules;
    private List<ReindexingTuningConfigRule> compactionTuningConfigRules;

    public Builder filterRules(List<ReindexingFilterRule> compactionFilterRules)
    {
      this.compactionFilterRules = compactionFilterRules;
      return this;
    }

    public Builder metricsRules(List<ReindexingMetricsRule> compactionMetricsRules)
    {
      this.compactionMetricsRules = compactionMetricsRules;
      return this;
    }

    public Builder dimensionsRules(List<ReindexingDimensionsRule> compactionDimensionsRules)
    {
      this.compactionDimensionsRules = compactionDimensionsRules;
      return this;
    }

    public Builder ioConfigRules(List<ReindexingIOConfigRule> compactionIOConfigRules)
    {
      this.compactionIOConfigRules = compactionIOConfigRules;
      return this;
    }

    public Builder projectionRules(List<ReindexingProjectionRule> compactionProjectionRules)
    {
      this.compactionProjectionRules = compactionProjectionRules;
      return this;
    }

    public Builder granularityRules(List<ReindexingGranularityRule> compactionGranularityRules)
    {
      this.compactionGranularityRules = compactionGranularityRules;
      return this;
    }

    public Builder tuningConfigRules(List<ReindexingTuningConfigRule> compactionTuningConfigRules)
    {
      this.compactionTuningConfigRules = compactionTuningConfigRules;
      return this;
    }

    public InlineReindexingRuleProvider build()
    {
      return new InlineReindexingRuleProvider(
          compactionFilterRules,
          compactionMetricsRules,
          compactionDimensionsRules,
          compactionIOConfigRules,
          compactionProjectionRules,
          compactionGranularityRules,
          compactionTuningConfigRules
      );
    }
  }
}
