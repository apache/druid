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
import org.apache.druid.server.compaction.CompactionCandidate;
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
import org.apache.druid.timeline.CompactionState;
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
 * Template to perform period-based cascading reindexing. {@link ReindexingRule} are provided by a {@link ReindexingRuleProvider}
 * Each rule specifies a period relative to the current time which is used to determine its applicable interval.
 * A timeline is constructed from a condensed set of these periods and tasks are created for each search interval in
 * the timeline with the applicable rules for said interval.
 * <p>
 * For example if you had the following rules:
 * <ul>
 *   <li>Rule A: period = 1 day</li>
 *   <li>Rule B: period = 7 days</li>
 *   <li>Rule C: period = 30 days</li>
 *   <li>Rule D: period = 7 days</li>
 * </ul>
 *
 * You would end up with the following search intervals (assuming current time is T):
 * <ul>
 *   <li>Interval 1: [T-7days, T-1day)</li>
 *   <li>Interval 2: [T-30days, T-7days)</li>
 *   <li>Interval 3: [-inf, T-30days)</li>
 * </ul>
 * <p>
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate}
 */
public class CascadingReindexingTemplate implements CompactionJobTemplate, DataSourceCompactionConfig
{
  private static final Logger LOG = new Logger(CascadingReindexingTemplate.class);

  public static final String TYPE = "reindexCascade";

  private final String dataSource;
  private final ReindexingRuleProvider ruleProvider;
  @Nullable
  private final Map<String, Object> taskContext;
  @Nullable
  private final CompactionEngine engine;
  private final int taskPriority;
  private final long inputSegmentSizeBytes;

  @JsonCreator
  public CascadingReindexingTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("ruleProvider") ReindexingRuleProvider ruleProvider,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource, "'dataSource' cannot be null");
    this.ruleProvider = Objects.requireNonNull(ruleProvider, "'ruleProvider' cannot be null");
    this.engine = engine;
    this.taskContext = taskContext;
    this.taskPriority = Objects.requireNonNullElse(taskPriority, DEFAULT_COMPACTION_TASK_PRIORITY);
    this.inputSegmentSizeBytes = Objects.requireNonNullElse(inputSegmentSizeBytes, DEFAULT_INPUT_SEGMENT_SIZE_BYTES);
  }

  @Override
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Nullable
  @Override
  public Map<String, Object> getTaskContext()
  {
    return taskContext;
  }

  @JsonProperty
  @Nullable
  @Override
  public CompactionEngine getEngine()
  {
    return engine;
  }

  @JsonProperty
  @Override
  public int getTaskPriority()
  {
    return taskPriority;
  }

  @JsonProperty
  @Override
  public long getInputSegmentSizeBytes()
  {
    return inputSegmentSizeBytes;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  private ReindexingRuleProvider getRuleProvider()
  {
    return ruleProvider;
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
      if (shouldOptimizeFilterRules(candidate, config)) {

        // Compute the minimal set of filter rules needed for this candidate
        NotDimFilter reducedTransformSpecFilter = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

  /**
   * Determines if we should optimize filter rules for this candidate.
   * Returns true only if the candidate has been compacted before and has a NotDimFilter.
   */
  private static boolean shouldOptimizeFilterRules(
      CompactionCandidate candidate,
      DataSourceCompactionConfig config
  )
  {
    if (candidate.getCurrentStatus() == null) {
      return false;
    }

    if (candidate.getCurrentStatus().getReason().equals(CompactionStatus.NEVER_COMPACTED_REASON)) {
      return false;
    }

    if (config.getTransformSpec() == null) {
      return false;
    }

    DimFilter filter = config.getTransformSpec().getFilter();
    return filter instanceof NotDimFilter;
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
    if (sortedPeriods.isEmpty()) {
      return Collections.emptyList();
    }

    List<Interval> intervals = generateSearchIntervals(sortedPeriods, currentTime);
    for (Interval reindexingInterval : intervals) {
      InlineSchemaDataSourceCompactionConfig.Builder builder = InlineSchemaDataSourceCompactionConfig.builder()
          .forDataSource(dataSource)
          .withTaskPriority(taskPriority)
          .withInputSegmentSizeBytes(inputSegmentSizeBytes)
          .withEngine(engine)
          .withTaskContext(taskContext)
          .withSkipOffsetFromLatest(Period.ZERO);

      // Apply all applicable reindexing rules to the builder
      int ruleCount = applyRulesToBuilder(builder, reindexingInterval, currentTime);

      if (ruleCount > 0) {
        LOG.info("Creating reindexing jobs for interval[%s] with [%d] rules selected", reindexingInterval, ruleCount);
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

  private List<Interval> generateSearchIntervals(List<Period> sortedPeriods, DateTime referenceTime)
  {
    List<Interval> intervals = new ArrayList<>(sortedPeriods.size());
    for (int i = 0; i < sortedPeriods.size(); i++) {
      DateTime end = referenceTime.minus(sortedPeriods.get(i));
      DateTime start;
      if (i + 1 < sortedPeriods.size()) {
        start = referenceTime.minus(sortedPeriods.get(i + 1));
      } else {
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
  public CompactionState toCompactionState()
  {
    throw new UnsupportedOperationException("CascadingReindexingTemplate cannot be transformed to a CompactionState object");
  }

  // Legacy fields from DataSourceCompactionConfig that are not used by this template

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
}
