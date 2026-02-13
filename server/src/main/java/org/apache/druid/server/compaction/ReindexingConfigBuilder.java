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
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
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
    int count = 0;

    count += applyIfPresent(
        builder::withTuningConfig,
        provider.getTuningConfigRule(interval, referenceTime),
        ReindexingTuningConfigRule::getTuningConfig
    );

    count += applyIfPresent(
        builder::withIoConfig,
        provider.getIOConfigRule(interval, referenceTime),
        ReindexingIOConfigRule::getIoConfig
    );

    count += applyDataSchemaRules(builder);

    count += applyDeletionRules(builder);

    ReindexingSegmentGranularityRule segmentGranularityRule = provider.getSegmentGranularityRule(interval, referenceTime);
    if (segmentGranularityRule == null) {
      if (count > 0) {
        // Insert a default segment granularity to the config only if other rules exist for the interval.
        builder.withSegmentGranularity(defaultGranularity);
      }
    } else {
      count++;
      builder.withSegmentGranularity(segmentGranularityRule.getSegmentGranularity());
    }

    return count;
  }

  // Generic helper for non-additive rules
  private <R, C> int applyIfPresent(
      Consumer<C> setter,
      @Nullable R rule,
      Function<R, C> configExtractor
  )
  {
    if (rule != null) {
      C config = configExtractor.apply(rule);
      setter.accept(config);
      return 1;
    }
    return 0;
  }

  private int applyDataSchemaRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    ReindexingDataSchemaRule dataSchemaRule = provider.getDataSchemaRule(
        interval,
        referenceTime
    );
    if (dataSchemaRule == null) {
      return 0;
    }

    applyIfPresent(
        builder::withDimensionsSpec,
        dataSchemaRule,
        ReindexingDataSchemaRule::getDimensionsSpec
    );

    applyIfPresent(
        builder::withMetricsSpec,
        dataSchemaRule,
        ReindexingDataSchemaRule::getMetricsSpec
    );

    applyIfPresent(
        builder::withProjections,
        dataSchemaRule,
        ReindexingDataSchemaRule::getProjections
    );

    if (dataSchemaRule.getQueryGranularity() != null || dataSchemaRule.getRollup() != null) {
      builder.withQueryGranularityAndRollup(
          dataSchemaRule.getQueryGranularity(),
          dataSchemaRule.getRollup()
      );
    }

    return 1;
  }

  private int applyDeletionRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    List<ReindexingDeletionRule> rules = provider.getDeletionRules(interval, referenceTime);
    if (rules.isEmpty()) {
      return 0;
    }

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
    return rules.size();
  }
}
