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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.ReindexingDimensionsRule;
import org.apache.druid.server.compaction.ReindexingFilterRule;
import org.apache.druid.server.compaction.ReindexingGranularityRule;
import org.apache.druid.server.compaction.ReindexingIOConfigRule;
import org.apache.druid.server.compaction.ReindexingMetricsRule;
import org.apache.druid.server.compaction.ReindexingProjectionRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingTuningConfigRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Template to perform period-based cascading reindexing. Contains a list of
 * {@link ReindexingRule} which divide the segment timeline into reindexable
 * intervals. Each rule specifies a period relative to the current time which is
 * used to determine its applicable interval:
 * <ul>
 * <li>Rule 1: range = [now - p1, +inf)</li>
 * <li>Rule 2: range = [now - p2, now - p1)</li>
 * <li>...</li>
 * <li>Rule n: range = (-inf, now - p(n - 1))</li>
 * </ul>
 *
 * If two adjacent rules explicitly specify a segment granularity, the boundary
 * between them may be adjusted to ensure that there are no unprocessed gaps in the timeline.
 * <p>
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate}
 */
public class CascadingReindexingTemplate implements CompactionJobTemplate, DataSourceCompactionConfig
{
  private static final Logger LOG = new Logger(CascadingReindexingTemplate.class);

  public static final String TYPE = "reindexCascade";

  private final String dataSource;
  private final ReindexingRuleProvider ruleProvider;

  @JsonCreator
  public CascadingReindexingTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("ruleProvider") ReindexingRuleProvider ruleProvider
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource, "'dataSource' cannot be null");
    this.ruleProvider = Objects.requireNonNull(ruleProvider, "'ruleProvider' cannot be null");
  }

  @Override
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  /**
   * Creates a config finalizer that optimizes filter rules for cascading reindexing.
   * When a candidate segment has already been reindexed with a subset of filter rules,
   * this finalizer computes the minimal set of additional filter rules needed.
   * This optimization reduces bitmap operations during reindexing.
   */
  private static ReindexingConfigFinalizer createCascadingFinalizer()
  {
    return (config, candidate, params) -> {
      // Only optimize if candidate has been reindexed before and config has a NotDimFilter
      if (candidate.getCurrentStatus() != null &&
          !candidate.getCurrentStatus().getReason().equals(CompactionStatus.NEVER_COMPACTED_REASON) &&
          config.getTransformSpec() != null &&
          config.getTransformSpec().getFilter() != null &&
          config.getTransformSpec().getFilter() instanceof NotDimFilter) {

        // Compute the minimal set of filter rules needed for this candidate
        NotDimFilter reducedTransformSpecFilter = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
            candidate,
            (NotDimFilter) config.getTransformSpec().getFilter(),
            params.getFingerprintMapper()
        );

        // Safe cast: we know this is InlineSchemaDataSourceCompactionConfig because we just built it
        return ((InlineSchemaDataSourceCompactionConfig) config).toBuilder()
            .withTransformSpec(new CompactionTransformSpec(reducedTransformSpecFilter))
            .build();
      }

      return config; // No optimization needed, return original config
    };
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams jobParams
  )
  {
    // Check if the rule provider is ready before attempting to create jobs
    if (!ruleProvider.isReady()) {
      LOG.info(
          "Rule provider [%s] is not ready, skipping reindexing job creation for dataSource[%s]",
          ruleProvider.getType(),
          dataSource
      );
      return Collections.emptyList();
    }

    final List<CompactionJob> allJobs = new ArrayList<>();
    final DateTime currentTime = jobParams.getScheduleStartTime();

    List<Period> sortedPeriods = ruleProvider.getCondensedAndSortedPeriods(currentTime);
    if (sortedPeriods == null || sortedPeriods.isEmpty()) {
      return Collections.emptyList();
    }

    // Generate intervals from periods and create jobs for each
    List<Interval> intervals = generateIntervalsFromPeriods(sortedPeriods, currentTime, ruleProvider.getGranularityRules());
    for (Interval reindexingInterval : intervals) {
      InlineSchemaDataSourceCompactionConfig.Builder builder = InlineSchemaDataSourceCompactionConfig.builder()
          .forDataSource(dataSource)
          .withSkipOffsetFromLatest(Period.ZERO);

      // Apply all applicable reindexing rules to the builder
      int ruleCount = applyRulesToBuilder(builder, reindexingInterval, currentTime);

      if (ruleCount > 0) {
        LOG.info("Creating reindexing jobs for interval[%s] with %d rules", reindexingInterval, ruleCount);
        allJobs.addAll(
            createJobsForSearchInterval(
                new CompactionConfigBasedJobTemplate(builder.build(), createCascadingFinalizer()),
                reindexingInterval,
                source,
                jobParams
            )
        );
      } else {
        LOG.info("No applicable reindexing rules found for interval[%s]", reindexingInterval);
      }
    }
    return allJobs;
  }

  /**
   * Generates cascading intervals from sorted periods.
   * <p>
   * For periods [P7D, P30D, P90D], generates intervals:
   * <ul>
   *   <li>[now-30d, now-7d)</li>
   *   <li>[now-90d, now-30d)</li>
   *   <li>[DateTimes.MIN, now-90d)</li>
   * </ul>
   * <p>
   * If adjacent rules have segment granularities defined, boundaries are adjusted
   * to align with granularity buckets to prevent gaps in the timeline. Example:
   * <pre>
   * Given P7D rule with HOUR granularity and P30D rule with DAY granularity,
   * and referenceTime = 2025-12-19 14:37:22:
   *
   * Without adjustment:
   *   Calculated boundary: 2025-11-19 14:37:22 (now - 30 days)
   *
   * With adjustment:
   *   Aligned boundary: 2025-11-20 00:00:00 (aligned to start of next day)
   *
   * This ensures the DAY-granularity rule creates complete day-aligned segments
   * and the HOUR-granularity rule starts cleanly at a day boundary.
   * </pre>
   */
  private List<Interval> generateIntervalsFromPeriods(List<Period> sortedPeriods, DateTime referenceTime, List<ReindexingGranularityRule> granularityRules)
  {
    List<Interval> intervals = new ArrayList<>();
    DateTime previousAdjustedBoundary = null;

    for (int i = 0; i < sortedPeriods.size(); i++) {
      // End is either the previous adjusted boundary, or the raw calculation for the first interval
      DateTime end = previousAdjustedBoundary != null
                     ? previousAdjustedBoundary
                     : referenceTime.minus(sortedPeriods.get(i));
      DateTime start;

      if (i + 1 < sortedPeriods.size()) {
        // Bounded interval: between two periods
        // We may need to adjust the start time to avoid gaps if both adjacent rules have segment granularities defined.
        final DateTime calculatedStartTime = referenceTime.minus(sortedPeriods.get(i + 1));
        final int finalI = i;
        ReindexingGranularityRule currentRule = granularityRules.stream()
                                                                .filter(rule ->
                                                                            rule.getPeriod().equals(sortedPeriods.get(finalI)) && rule.getGranularityConfig().getSegmentGranularity() != null)
                                                                .findFirst()
                                                                .orElse(null);
        ReindexingGranularityRule beforeRule = granularityRules.stream()
                                                               .filter(rule ->
                                                                           rule.getPeriod().equals(sortedPeriods.get(finalI + 1)) && rule.getGranularityConfig().getSegmentGranularity() != null)
                                                               .findFirst()
                                                               .orElse(null);

        if (currentRule == null || beforeRule == null) {
          start = calculatedStartTime;
        } else {
          final Granularity granularity = currentRule.getGranularityConfig().getSegmentGranularity();
          final Granularity beforeGranularity = beforeRule.getGranularityConfig().getSegmentGranularity();

          final DateTime beforeRuleEffectiveEnd = beforeGranularity.bucketStart(calculatedStartTime);
          final DateTime possibleStartTime = granularity.bucketStart(beforeRuleEffectiveEnd);
          start = possibleStartTime.isBefore(beforeRuleEffectiveEnd)
                  ? granularity.increment(possibleStartTime)
                  : possibleStartTime;
        }

        // Save the adjusted start boundary for the next (older) interval's end
        previousAdjustedBoundary = start;
      } else {
        // Unbounded interval: from the earliest time. This allows the last rule to cover all remaining segments
        // into the past
        start = DateTimes.MIN;
      }

      intervals.add(new Interval(start, end));
    }
    return intervals;
  }

  /**
   * Applies all applicable reindexing rules to the builder for the given interval.
   *
   * @return number of rules applied
   */
  private int applyRulesToBuilder(
      InlineSchemaDataSourceCompactionConfig.Builder builder,
      Interval reindexingInterval,
      DateTime referenceTime
  )
  {
    int ruleCount = 0;

    // Granularity rules (non-additive, take first)
    List<ReindexingGranularityRule> granularityRules = ruleProvider.getGranularityRules(reindexingInterval, referenceTime);
    if (!granularityRules.isEmpty()) {
      LOG.info("Applying granularity rule %s for interval %s", granularityRules.get(0).getId(), reindexingInterval);
      builder.withGranularitySpec(granularityRules.get(0).getGranularityConfig());
      ruleCount += 1;
    }

    // Tuning config rules (non-additive, take first)
    List<ReindexingTuningConfigRule> tuningConfigRules = ruleProvider.getTuningConfigRules(reindexingInterval, referenceTime);
    if (!tuningConfigRules.isEmpty()) {
      LOG.info("Applying tuning config rule %s for interval %s", tuningConfigRules.get(0).getId(), reindexingInterval);
      builder.withTuningConfig(tuningConfigRules.get(0).getTuningConfig());
      ruleCount += 1;
    }

    // Metrics rules (non-additive, take first)
    List<ReindexingMetricsRule> metricsRules = ruleProvider.getMetricsRules(reindexingInterval, referenceTime);
    if (!metricsRules.isEmpty()) {
      LOG.info("Applying metrics rule %s for interval %s", metricsRules.get(0).getId(), reindexingInterval);
      builder.withMetricsSpec(metricsRules.get(0).getMetricsSpec());
      ruleCount += 1;
    }

    // Dimensions rules (non-additive, take first)
    List<ReindexingDimensionsRule> dimensionsRules = ruleProvider.getDimensionsRules(reindexingInterval, referenceTime);
    if (!dimensionsRules.isEmpty()) {
      LOG.info("Applying dimensions rule %s for interval %s", dimensionsRules.get(0).getId(), reindexingInterval);
      builder.withDimensionsSpec(dimensionsRules.get(0).getDimensionsSpec());
      ruleCount += 1;
    }

    // IO config rules (non-additive, take first)
    List<ReindexingIOConfigRule> ioConfigRules = ruleProvider.getIOConfigRules(reindexingInterval, referenceTime);
    if (!ioConfigRules.isEmpty()) {
      LOG.info("Applying IO config rule %s for interval %s", ioConfigRules.get(0).getId(), reindexingInterval);
      builder.withIoConfig(ioConfigRules.get(0).getIoConfig());
      ruleCount += 1;
    }

    // Projection rules (additive, combine all)
    List<ReindexingProjectionRule> projectionRules = ruleProvider.getProjectionRules(reindexingInterval, referenceTime);
    if (!projectionRules.isEmpty()) {
      LOG.info("Applying [%d] projection rules for interval %s", projectionRules.size(), reindexingInterval);
      builder.withProjections(
          projectionRules.stream()
                         .flatMap(rule -> rule.getProjections().stream())
                         .collect(Collectors.toList())
      );
      ruleCount += projectionRules.size();
    }

    // Filter rules (additive, combine with OR and wrap in NOT)
    List<ReindexingFilterRule> filterRules = ruleProvider.getFilterRules(reindexingInterval, referenceTime);
    if (!filterRules.isEmpty()) {
      LOG.info("Applying up to [%d] filter rules for interval %s", filterRules.size(), reindexingInterval);
      List<DimFilter> removeConditions = filterRules.stream()
                                                    .map(ReindexingFilterRule::getFilter)
                                                    .collect(Collectors.toList());

      DimFilter removeFilter = removeConditions.size() == 1
                               ? removeConditions.get(0)
                               : new OrDimFilter(removeConditions);
      DimFilter finalFilter = new NotDimFilter(removeFilter);
      builder.withTransformSpec(new CompactionTransformSpec(finalFilter));
      ruleCount += filterRules.size();
    }

    return ruleCount;
  }

  private List<CompactionJob> createJobsForSearchInterval(
      CompactionJobTemplate template,
      Interval searchInterval,
      DruidInputSource inputSource,
      CompactionJobParams jobParams
  )
  {
    return template.createCompactionJobs(
        inputSource.withInterval(searchInterval),
        jobParams
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  // Legacy fields from DataSourceCompactionConfig that are not used by this template

  @Nullable
  @Override
  public CompactionEngine getEngine()
  {
    return null;
  }

  @Override
  public int getTaskPriority()
  {
    return 0;
  }

  @Override
  public long getInputSegmentSizeBytes()
  {
    return 0;
  }

  @Nullable
  @Override
  public Integer getMaxRowsPerSegment()
  {
    return 0;
  }

  @Override
  public Period getSkipOffsetFromLatest()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskIOConfig getIoConfig()
  {
    return null;
  }

  @Nullable
  @Override
  public Map<String, Object> getTaskContext()
  {
    return Map.of();
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskGranularityConfig getGranularitySpec()
  {
    return null;
  }

  @Nullable
  @Override
  public List<AggregateProjectionSpec> getProjections()
  {
    return List.of();
  }

  @Nullable
  @Override
  public CompactionTransformSpec getTransformSpec()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return null;
  }

  @Nullable
  @Override
  public AggregatorFactory[] getMetricsSpec()
  {
    return new AggregatorFactory[0];
  }

  @JsonProperty
  private ReindexingRuleProvider getRuleProvider()
  {
    return ruleProvider;
  }
}
