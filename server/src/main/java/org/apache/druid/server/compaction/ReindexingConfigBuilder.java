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

import org.apache.druid.java.util.common.granularity.Granularity;
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
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Builds compaction configs by applying reindexing rules.
 * Encapsulates the logic for combining additive rules and applying all rule types.
 */
public class ReindexingConfigBuilder
{
  private static final Logger LOG = new Logger(ReindexingConfigBuilder.class);

  private final ReindexingRuleProvider provider;
  private final Granularity defaultGranularity;
  private final Interval interval;
  private final DateTime referenceTime;

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
     * @return the number of rules that were applied
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
      Granularity defaultSegmentGranularity, Interval interval,
      DateTime referenceTime
  )
  {
    this.provider = provider;
    this.defaultGranularity = defaultSegmentGranularity;
    this.interval = interval;
    this.referenceTime = referenceTime;
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
    ReindexingSegmentGranularityRule segmentGranularityRule = provider.getSegmentGranularityRule(interval, referenceTime);
    if (segmentGranularityRule == null) {
      if (count > 0) {
        // Insert a default segment granularity to the config only if other rules exist for the interval.
        builder.withSegmentGranularity(defaultGranularity);
      }
    } else {
      builder.withSegmentGranularity(segmentGranularityRule.getSegmentGranularity());
      appliedRules.add(segmentGranularityRule);
      count++;
    }

    return new BuildResult(count, appliedRules);
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
