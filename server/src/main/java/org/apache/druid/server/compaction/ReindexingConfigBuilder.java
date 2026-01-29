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
  private final Interval interval;
  private final DateTime referenceTime;

  public ReindexingConfigBuilder(
      ReindexingRuleProvider provider,
      Interval interval,
      DateTime referenceTime
  )
  {
    this.provider = provider;
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
        builder::withMetricsSpec,
        provider.getMetricsRule(interval, referenceTime),
        ReindexingMetricsRule::getMetricsSpec
    );

    count += applyIfPresent(
        builder::withDimensionsSpec,
        provider.getDimensionsRule(interval, referenceTime),
        ReindexingDimensionsRule::getDimensionsSpec
    );

    count += applyIfPresent(
        builder::withIoConfig,
        provider.getIOConfigRule(interval, referenceTime),
        ReindexingIOConfigRule::getIoConfig
    );

    count += applyIfPresent(
        builder::withProjections,
        provider.getProjectionRule(interval, referenceTime),
        ReindexingProjectionRule::getProjections
    );

    count += applyGranularityRules(builder);

    count += applyFilterRules(builder);

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
      LOG.debug(
          "Applied rule %s for interval %s",
          ((ReindexingRule) rule).getId(), interval
      );
      return 1;
    }
    return 0;
  }

  private int applyGranularityRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    ReindexingSegmentGranularityRule segmentGranularityRule = provider.getSegmentGranularityRule(
        interval,
        referenceTime
    );
    ReindexingQueryGranularityRule queryGranularityRule = provider.getQueryGranularityRule(
        interval,
        referenceTime
    );

    if (segmentGranularityRule == null && queryGranularityRule == null) {
      return 0;
    }

    // Extract granularities from rules (null if rule doesn't exist)
    Granularity segmentGranularity = segmentGranularityRule != null ? segmentGranularityRule.getSegmentGranularity() : null;

    Granularity queryGranularity = queryGranularityRule != null ? queryGranularityRule.getQueryGranularity() : null;
    Boolean rollup = queryGranularityRule != null ? queryGranularityRule.getRollup() : null;

    // Build and apply the combined granularity config
    UserCompactionTaskGranularityConfig granularityConfig =
        new UserCompactionTaskGranularityConfig(segmentGranularity, queryGranularity, rollup);

    builder.withGranularitySpec(granularityConfig);

    int count = 0;
    if (segmentGranularityRule != null) {
      LOG.debug("Applied segment granularity rule [%s] for interval [%s]", segmentGranularityRule.getId(), interval);
      count++;
    }
    if (queryGranularityRule != null) {
      LOG.debug("Applied query granularity rule [%s] for interval [%s]", queryGranularityRule.getId(), interval);
      count++;
    }

    return count;
  }

  private int applyFilterRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
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
