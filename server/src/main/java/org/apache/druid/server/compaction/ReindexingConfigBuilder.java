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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds compaction configs by applying reindexing rules.
 * Encapsulates the logic for combining additive rules and applying all rule types.
 */
public class ReindexingConfigBuilder
{
  private static final Logger LOG = new Logger(ReindexingConfigBuilder.class);

  private final ReindexingRuleProvider provider;
  private final Interval interval;
  private final DateTime referenceTime;
  private final List<IntervalGranularityInfo> syntheticTimeline;

  /**
   * Result of applying reindexing rules to a config builder.
   * Contains both the count of rules applied and the actual rules that were applied.
   */
  public static class BuildResult
  {
    private final int ruleCount;
    private final List<ReindexingRule> appliedRules;

    public BuildResult(int ruleCount, List<ReindexingRule> appliedRules)
    {
      this.ruleCount = ruleCount;
      this.appliedRules = appliedRules;
    }

    /**
     * Returns the count of rules that were actually applied to this specific interval.
     * This is NOT the total number of rules in the provider, but rather the count
     * of rules that matched and were applied during config building.
     *
     * @return the number of rules that were applied to the builder
     */
    public int getRuleCount()
    {
      return ruleCount;
    }

    /**
     * @return immutable list of the actual rules that were applied, in application order
     */
    public List<ReindexingRule> getAppliedRules()
    {
      return appliedRules;
    }
  }

  public ReindexingConfigBuilder(
      ReindexingRuleProvider provider,
      Interval interval,
      DateTime referenceTime,
      List<IntervalGranularityInfo> syntheticTimeline
  )
  {
    this.provider = provider;
    this.interval = interval;
    this.referenceTime = referenceTime;
    this.syntheticTimeline = syntheticTimeline;
  }

  /**
   * Applies all applicable rules to the builder.
   *
   * @return number of rules applied
   */
  public int applyTo(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    return applyToWithDetails(builder).getRuleCount();
  }

  /**
   * Applies all applicable rules to the builder and returns detailed information about
   * which rules were applied.
   *
   * @return BuildResult containing the count and list of applied rules
   */
  public BuildResult applyToWithDetails(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    int count = 0;
    List<ReindexingRule> appliedRules = new ArrayList<>();

    // Apply tuning config rule
    ReindexingTuningConfigRule tuningRule = provider.getTuningConfigRule(interval, referenceTime);
    if (tuningRule != null) {
      builder.withTuningConfig(tuningRule.getTuningConfig());
      appliedRules.add(tuningRule);
      count++;
    }

    // Apply IO config rule
    ReindexingIOConfigRule ioConfigRule = provider.getIOConfigRule(interval, referenceTime);
    if (ioConfigRule != null) {
      builder.withIoConfig(ioConfigRule.getIoConfig());
      appliedRules.add(ioConfigRule);
      count++;
    }

    // Apply data schema rules
    ReindexingDataSchemaRule dataSchemaRule = provider.getDataSchemaRule(interval, referenceTime);
    if (dataSchemaRule != null) {
      applyDataSchemaRule(builder, dataSchemaRule);
      appliedRules.add(dataSchemaRule);
      count++;
    }

    // Apply deletion rules (additive)
    List<ReindexingDeletionRule> deletionRules = provider.getDeletionRules(interval, referenceTime);
    if (!deletionRules.isEmpty()) {
      applyDeletionRulesList(builder, deletionRules);
      appliedRules.addAll(deletionRules);
      count += deletionRules.size();
    }

    // Apply segment granularity rule
    // Use granularity from synthetic timeline
    IntervalGranularityInfo granularityInfo = findMatchingInterval(interval);
    if (granularityInfo == null) {
      throw DruidException.defensive(
          "No matching interval found in synthetic timeline for interval[%s]. This should never happen.",
          interval
      );
    }

    builder.withSegmentGranularity(granularityInfo.getGranularity());
    if (granularityInfo.getSourceRule() != null) {
      // Only count and track the rule if it came from an actual rule (not default)
      appliedRules.add(granularityInfo.getSourceRule());
      count++;
    }

    return new BuildResult(count, count == 0 ? List.of() : appliedRules);
  }

  /**
   * Finds the matching interval granularity info from the synthetic timeline.
   * Returns null if no synthetic timeline was provided or no match is found.
   */
  @Nullable
  private IntervalGranularityInfo findMatchingInterval(Interval interval)
  {
    for (IntervalGranularityInfo candidate : syntheticTimeline) {
      if (candidate.getInterval().equals(interval)) {
        return candidate;
      }
    }

    return null;
  }

  private void applyDataSchemaRule(
      InlineSchemaDataSourceCompactionConfig.Builder builder,
      ReindexingDataSchemaRule dataSchemaRule
  )
  {
    if (dataSchemaRule.getDimensionsSpec() != null) {
      builder.withDimensionsSpec(dataSchemaRule.getDimensionsSpec());
    }

    if (dataSchemaRule.getMetricsSpec() != null) {
      builder.withMetricsSpec(dataSchemaRule.getMetricsSpec());
    }

    if (dataSchemaRule.getProjections() != null) {
      builder.withProjections(dataSchemaRule.getProjections());
    }

    if (dataSchemaRule.getQueryGranularity() != null || dataSchemaRule.getRollup() != null) {
      builder.withQueryGranularityAndRollup(
          dataSchemaRule.getQueryGranularity(),
          dataSchemaRule.getRollup()
      );
    }
  }

  /**
   * Applies deletion rules by combining their filters into a single transform filter.
   * <p>
   * Each deletion rule specifies rows that should be deleted. To implement deletion during
   * compaction, we need to keep only rows that do NOT match any deletion rule.
   * <p>
   * Filter construction logic:
   * <ul>
   *   <li>Collect all deletion filters (one per rule)</li>
   *   <li>OR them together: (filter1 OR filter2 OR ...)</li>
   *   <li>Wrap in NOT: NOT(filter1 OR filter2 OR ...)</li>
   * </ul>
   * <p>
   * Result: Rows matching ANY deletion rule are filtered out, all other rows are kept.
   * <p>
   * Example: With rules "delete country=US" and "delete device=mobile":
   * Final filter: NOT((country=US) OR (device=mobile))
   * This keeps all rows except those where country=US OR device=mobile.
   *
   * @param builder the config builder to apply the deletion filter to
   * @param rules the deletion rules to combine
   */
  private void applyDeletionRulesList(
      InlineSchemaDataSourceCompactionConfig.Builder builder,
      List<ReindexingDeletionRule> rules
  )
  {

    // Collect filters and virtual columns in a single pass
    List<DimFilter> removeConditions = new ArrayList<>();
    List<VirtualColumn> allVirtualColumns = new ArrayList<>();

    for (ReindexingDeletionRule rule : rules) {
      removeConditions.add(rule.getDeleteWhere());

      if (rule.getVirtualColumns() != null) {
        allVirtualColumns.addAll(Arrays.asList(rule.getVirtualColumns().getVirtualColumns()));
      }
    }

    // Combine filters: OR all filters together, wrap in NOT
    DimFilter removeFilter = removeConditions.size() == 1
                             ? removeConditions.get(0)
                             : new OrDimFilter(removeConditions);
    DimFilter finalFilter = new NotDimFilter(removeFilter);

    // Create VirtualColumns if any exist
    VirtualColumns virtualColumns = allVirtualColumns.isEmpty()
                                    ? null
                                    : VirtualColumns.create(allVirtualColumns);

    builder.withTransformSpec(new CompactionTransformSpec(finalFilter, virtualColumns));

    LOG.debug("Applied [%d] filter rules for interval %s", rules.size(), interval);
  }
}
