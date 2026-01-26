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

import org.apache.druid.data.input.impl.AggregateProjectionSpec;
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
import java.util.stream.Collectors;

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
        builder::withGranularitySpec,
        provider.getGranularityRule(interval, referenceTime),
        ReindexingGranularityRule::getGranularityConfig
    );

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

    count += applyProjectionRules(builder);

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

  private int applyProjectionRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    List<ReindexingProjectionRule> rules = provider.getProjectionRules(interval, referenceTime);
    if (rules.isEmpty()) {
      return 0;
    }

    // Combine: flatMap all projections from all rules
    List<AggregateProjectionSpec> combined = rules.stream()
                                                  .flatMap(rule -> rule.getProjections().stream())
                                                  .collect(Collectors.toList());

    builder.withProjections(combined);
    LOG.debug("Applied [%d] projection rules for interval %s", rules.size(), interval);
    return rules.size();
  }

  private int applyFilterRules(InlineSchemaDataSourceCompactionConfig.Builder builder)
  {
    List<ReindexingFilterRule> rules = provider.getFilterRules(interval, referenceTime);
    if (rules.isEmpty()) {
      return 0;
    }

    // Collect filters and virtual columns in a single pass
    List<DimFilter> removeConditions = new ArrayList<>();
    List<VirtualColumn> allVirtualColumns = new ArrayList<>();

    for (ReindexingFilterRule rule : rules) {
      removeConditions.add(rule.getFilter());

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
