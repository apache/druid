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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

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
 *   "reindexingDeletionRules": [
 *     {
 *       "id": "remove-bots-90d",
 *       "olderThan": "P90D",
 *       "deleteWhere": {
 *         "type": "not",
 *         "field": {
 *           "type": "equals",
 *           "column": "is_bot",
 *           "matchValueType": "STRING"
 *           "matchValue": "true"
 *         }
 *       },
 *       "description": "Remove bot traffic from segments older than 90 days"
 *     },
 *     {
 *       "id": "remove-low-priority-180d",
 *       "olderThan": "P180D",
 *       "deleteWhere": {
 *         "type": "not",
 *         "field": {
 *           {
 *             "type": "inType",
 *             "column": "priority",
 *             "matchValueType": "STRING",
 *             "sortedValues": ["low", "spam"]
 *           }
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

  private final List<ReindexingDeletionRule> deletionRules;
  private final List<ReindexingIOConfigRule> ioConfigRules;
  private final List<ReindexingSegmentGranularityRule> segmentGranularityRules;
  private final List<ReindexingTuningConfigRule> tuningConfigRules;
  private final List<ReindexingDataSchemaRule> dataSchemaRules;


  @JsonCreator
  public InlineReindexingRuleProvider(
      @JsonProperty("deletionRules") @Nullable List<ReindexingDeletionRule> deletionRules,
      @JsonProperty("ioConfigRules") @Nullable List<ReindexingIOConfigRule> ioConfigRules,
      @JsonProperty("segmentGranularityRules") @Nullable List<ReindexingSegmentGranularityRule> segmentGranularityRules,
      @JsonProperty("tuningConfigRules") @Nullable List<ReindexingTuningConfigRule> tuningConfigRules,
      @JsonProperty("dataSchemaRules") @Nullable List<ReindexingDataSchemaRule> dataSchemaRules
  )
  {
    this.deletionRules = Configs.valueOrDefault(deletionRules, Collections.emptyList());
    this.ioConfigRules = Configs.valueOrDefault(ioConfigRules, Collections.emptyList());
    this.segmentGranularityRules = Configs.valueOrDefault(segmentGranularityRules, Collections.emptyList());
    this.tuningConfigRules = Configs.valueOrDefault(tuningConfigRules, Collections.emptyList());
    this.dataSchemaRules = Configs.valueOrDefault(dataSchemaRules, Collections.emptyList());
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
  @JsonProperty("deletionRules")
  public List<ReindexingDeletionRule> getDeletionRules()
  {
    return deletionRules;
  }

  @Override
  @Nullable
  public ReindexingDataSchemaRule getDataSchemaRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(dataSchemaRules, interval, referenceTime);
  }

  @Override
  @JsonProperty("dataSchemaRules")
  public List<ReindexingDataSchemaRule> getDataSchemaRules()
  {
    return dataSchemaRules;
  }

  @Override
  @JsonProperty("ioConfigRules")
  public List<ReindexingIOConfigRule> getIOConfigRules()
  {
    return ioConfigRules;
  }

  @Override
  @JsonProperty("segmentGranularityRules")
  public List<ReindexingSegmentGranularityRule> getSegmentGranularityRules()
  {
    return segmentGranularityRules;
  }

  @Override
  @JsonProperty("tuningConfigRules")
  public List<ReindexingTuningConfigRule> getTuningConfigRules()
  {
    return tuningConfigRules;
  }

  @Override
  public List<ReindexingDeletionRule> getDeletionRules(Interval interval, DateTime referenceTime)
  {
    return getApplicableRules(deletionRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingIOConfigRule getIOConfigRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(ioConfigRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingSegmentGranularityRule getSegmentGranularityRule(
      Interval interval,
      DateTime referenceTime
  )
  {
    return getApplicableRule(segmentGranularityRules, interval, referenceTime);
  }

  @Override
  @Nullable
  public ReindexingTuningConfigRule getTuningConfigRule(Interval interval, DateTime referenceTime)
  {
    return getApplicableRule(tuningConfigRules, interval, referenceTime);
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
          DateTime threshold = referenceTime.minus(r.getOlderThan());
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
    return Objects.equals(deletionRules, that.deletionRules)
           && Objects.equals(ioConfigRules, that.ioConfigRules)
           && Objects.equals(segmentGranularityRules, that.segmentGranularityRules)
           && Objects.equals(tuningConfigRules, that.tuningConfigRules)
           && Objects.equals(dataSchemaRules, that.dataSchemaRules);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        deletionRules,
        ioConfigRules,
        segmentGranularityRules,
        tuningConfigRules,
        dataSchemaRules
    );
  }

  @Override
  public String toString()
  {
    return "InlineReindexingRuleProvider{"
           + "deletionRules=" + deletionRules
           + ", ioConfigRules=" + ioConfigRules
           + ", segmentGranularityRules=" + segmentGranularityRules
           + ", tuningConfigRules=" + tuningConfigRules
           + ", dataSchemaRules=" + dataSchemaRules
           + '}';
  }

  public static class Builder
  {
    private List<ReindexingDeletionRule> deletionRules;
    private List<ReindexingIOConfigRule> ioConfigRules;
    private List<ReindexingSegmentGranularityRule> segmentGranularityRules;
    private List<ReindexingTuningConfigRule> tuningConfigRules;
    private List<ReindexingDataSchemaRule> dataSchemaRules;

    public Builder deletionRules(List<ReindexingDeletionRule> deletionRules)
    {
      this.deletionRules = deletionRules;
      return this;
    }

    public Builder dataSchemaRules(List<ReindexingDataSchemaRule> dataSchemaRules)
    {
      this.dataSchemaRules = dataSchemaRules;
      return this;
    }

    public Builder ioConfigRules(List<ReindexingIOConfigRule> ioConfigRules)
    {
      this.ioConfigRules = ioConfigRules;
      return this;
    }

    public Builder segmentGranularityRules(List<ReindexingSegmentGranularityRule> segmentGranularityRules)
    {
      this.segmentGranularityRules = segmentGranularityRules;
      return this;
    }

    public Builder tuningConfigRules(List<ReindexingTuningConfigRule> tuningConfigRules)
    {
      this.tuningConfigRules = tuningConfigRules;
      return this;
    }

    public InlineReindexingRuleProvider build()
    {
      return new InlineReindexingRuleProvider(
          deletionRules,
          ioConfigRules,
          segmentGranularityRules,
          tuningConfigRules,
          dataSchemaRules
      );
    }
  }
}
