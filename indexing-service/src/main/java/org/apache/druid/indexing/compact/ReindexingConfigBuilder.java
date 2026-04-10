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

package org.apache.druid.indexing.compact;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.IntervalPartitioningInfo;
import org.apache.druid.server.compaction.ReindexingDataSchemaRule;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingIndexSpecRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds compaction configs for cascading reindexing by applying reindexing rules.
 * This is an implementation detail of {@link CascadingReindexingTemplate} and encapsulates
 * the logic for combining additive rules and applying all rule types.
 * <p>
 * Package-private as this is only used internally by CascadingReindexingTemplate.
 */
class ReindexingConfigBuilder
{
  private static final Logger LOG = new Logger(ReindexingConfigBuilder.class);

  private final ReindexingRuleProvider provider;
  private final Interval interval;
  private final DateTime referenceTime;
  private final List<IntervalPartitioningInfo> syntheticTimeline;
  @Nullable
  private final UserCompactionTaskQueryTuningConfig baseTuningConfig;

  /**
   * Result of applying reindexing rules to a config builder.
   * Contains both the count of rules applied and the actual rules that were applied.
   */
  static class BuildResult
  {
    private final int ruleCount;
    private final List<ReindexingRule> appliedRules;

    BuildResult(int ruleCount, List<ReindexingRule> appliedRules)
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
    int getRuleCount()
    {
      return ruleCount;
    }

    /**
     * @return immutable list of the actual rules that were applied, in application order
     */
    List<ReindexingRule> getAppliedRules()
    {
      return appliedRules;
    }
  }

  ReindexingConfigBuilder(
      ReindexingRuleProvider provider,
      Interval interval,
      DateTime referenceTime,
      List<IntervalPartitioningInfo> syntheticTimeline,
      @Nullable UserCompactionTaskQueryTuningConfig baseTuningConfig
  )
  {
    this.provider = provider;
    this.interval = interval;
    this.referenceTime = referenceTime;
    this.syntheticTimeline = syntheticTimeline;
    this.baseTuningConfig = baseTuningConfig;
  }

  /**
   * Applies all applicable rules to the builder.
   *
   * @return number of rules applied
   */
  int applyTo(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    return applyToWithDetails(builder).getRuleCount();
  }

  /**
   * Applies all applicable rules to the builder and returns detailed information about
   * which rules were applied.
   *
   * @return BuildResult containing the count and list of applied rules
   */
  BuildResult applyToWithDetails(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    int count = 0;
    List<ReindexingRule> appliedRules = new ArrayList<>();

    IntervalPartitioningInfo partitioningInfo = findMatchingInterval(interval);

    if (!partitioningInfo.isRuleSynthetic()) {
      appliedRules.add(partitioningInfo.getSourceRule());
      count++;
    }
    builder.withSegmentGranularity(partitioningInfo.getGranularity());
    PartitionsSpec partitionsSpec = partitioningInfo.getPartitionsSpec();

    IndexSpec indexSpec = null;
    ReindexingIndexSpecRule indexSpecRule = provider.getIndexSpecRule(interval, referenceTime);
    if (indexSpecRule != null) {
      indexSpec = indexSpecRule.getIndexSpec();
      appliedRules.add(indexSpecRule);
      count++;
    }

    // Build tuning config: start from the template's static tuning config (if any),
    // then overlay rule-derived partitionsSpec and indexSpec
    UserCompactionTaskQueryTuningConfig.Builder tuningBuilder =
        baseTuningConfig != null ? baseTuningConfig.toBuilder() : UserCompactionTaskQueryTuningConfig.builder();
    tuningBuilder.partitionsSpec(partitionsSpec);
    // Having no index spec from a rule means we should use the default in the base tuning config (if any)
    if (indexSpec != null) {
      tuningBuilder.indexSpec(indexSpec);
    }
    builder.withTuningConfig(tuningBuilder.build());

    ReindexingDataSchemaRule dataSchemaRule = provider.getDataSchemaRule(interval, referenceTime);
    if (dataSchemaRule != null) {
      applyDataSchemaRule(builder, dataSchemaRule);
      appliedRules.add(dataSchemaRule);
      count++;
    }

    List<ReindexingDeletionRule> deletionRules = provider.getDeletionRules(interval, referenceTime);
    DimFilter deletionFilter = null;
    List<VirtualColumn> deletionVCs = new ArrayList<>();

    if (!deletionRules.isEmpty()) {
      List<DimFilter> removeConditions = new ArrayList<>();

      for (ReindexingDeletionRule rule : deletionRules) {
        removeConditions.add(rule.getDeleteWhere());
        if (rule.getVirtualColumns() != null) {
          deletionVCs.addAll(Arrays.asList(rule.getVirtualColumns().getVirtualColumns()));
        }
      }

      DimFilter removeFilter = removeConditions.size() == 1
                               ? removeConditions.get(0)
                               : new OrDimFilter(removeConditions);
      deletionFilter = new NotDimFilter(removeFilter);

      appliedRules.addAll(deletionRules);
      count += deletionRules.size();
    }

    // Merge partitioning VCs with deletion VCs and set transform spec
    VirtualColumns mergedVCs = mergeVirtualColumns(partitioningInfo.getVirtualColumns(), deletionVCs);

    if (deletionFilter != null || mergedVCs != null) {
      builder.withTransformSpec(new CompactionTransformSpec(deletionFilter, mergedVCs));
      LOG.debug("Applied [%d] filter rules for interval %s", deletionRules.size(), interval);
    }

    return new BuildResult(count, count == 0 ? List.of() : appliedRules);
  }

  /**
   * Finds the matching interval granularity info from the synthetic timeline.
   * <p>
   * Throws a defensive exception if no match is found, but this should never happen
   */
  private IntervalPartitioningInfo findMatchingInterval(Interval interval)
  {
    for (IntervalPartitioningInfo candidate : syntheticTimeline) {
      if (candidate.getInterval().equals(interval)) {
        return candidate;
      }
    }

    throw DruidException.defensive(
        "No matching interval found in synthetic timeline for interval[%s]. This should never happen.",
        interval
    );
  }

  /**
   * Merge partitioning virtual columns with deletion virtual columns, ensuring there are no name collisions.
   * <p>
   * Partitioning VCs and Deletion VCs coexist in the underlying @{link CompactionTransformSpec} so they must be merged safely
   */
  @Nullable
  private static VirtualColumns mergeVirtualColumns(
      @Nullable VirtualColumns partitioningVCs,
      List<VirtualColumn> deletionVCs
  )
  {
    List<VirtualColumn> allVCs = new ArrayList<>(deletionVCs);

    if (partitioningVCs != null && !partitioningVCs.isEmpty()) {
      allVCs.addAll(Arrays.asList(partitioningVCs.getVirtualColumns()));
    }

    if (allVCs.isEmpty()) {
      return null;
    }

    try {
      return VirtualColumns.create(allVCs);
    }
    catch (IAE e) {
      throw InvalidInput.exception(
          e,
          "Partitioning virtual column name collides with a deletion virtual column name. "
          + "Please rename the partitioning virtual column or the deletion virtual column to avoid this collision."
      );
    }
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
}
