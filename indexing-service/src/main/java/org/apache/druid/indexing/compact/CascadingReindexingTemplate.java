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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.ReindexingConfigBuilder;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
  private final Period skipOffsetFromLatest;
  private final Period skipOffsetFromNow;

  @JsonCreator
  public CascadingReindexingTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("ruleProvider") ReindexingRuleProvider ruleProvider,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("skipOffsetFromNow") @Nullable Period skipOffsetFromNow
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource, "'dataSource' cannot be null");
    this.ruleProvider = Objects.requireNonNull(ruleProvider, "'ruleProvider' cannot be null");
    this.engine = engine;
    this.taskContext = taskContext;
    this.taskPriority = Objects.requireNonNullElse(taskPriority, DEFAULT_COMPACTION_TASK_PRIORITY);
    this.inputSegmentSizeBytes = Objects.requireNonNullElse(inputSegmentSizeBytes, DEFAULT_INPUT_SEGMENT_SIZE_BYTES);

    if (skipOffsetFromNow != null && skipOffsetFromLatest != null) {
      throw new IAE("Cannot set both skipOffsetFromNow and skipOffsetFromLatest");
    }
    if (skipOffsetFromLatest != null) {
      this.skipOffsetFromLatest = skipOffsetFromLatest;
      this.skipOffsetFromNow = null;
    } else if (skipOffsetFromNow != null) {
      this.skipOffsetFromNow = skipOffsetFromNow;
      this.skipOffsetFromLatest = null;
    } else {
      this.skipOffsetFromLatest = null;
      this.skipOffsetFromNow = null;
    }
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

  @Override
  @JsonProperty
  @Nullable
  public Period getSkipOffsetFromLatest()
  {
    return skipOffsetFromLatest;
  }

  @JsonProperty
  @Nullable
  private Period getSkipOffsetFromNow()
  {
    return skipOffsetFromNow;
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
        NotDimFilter reducedTransformSpecFilter = ReindexingDeletionRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
            candidate,
            (NotDimFilter) config.getTransformSpec().getFilter(),
            params.getFingerprintMapper()
        );

        // Filter virtual columns to only include ones referenced by the reduced filter
        VirtualColumns reducedVirtualColumns = ReindexingDeletionRuleOptimizer.filterVirtualColumnsForFilter(
            reducedTransformSpecFilter,
            config.getTransformSpec().getVirtualColumns()
        );

        // Safe cast: we know this is InlineSchemaDataSourceCompactionConfig because we just built it
        return ((InlineSchemaDataSourceCompactionConfig) config)
            .toBuilder()
            .withTransformSpec(new CompactionTransformSpec(reducedTransformSpecFilter, reducedVirtualColumns))
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

    List<Interval> intervals = generateAlignedSearchIntervals(currentTime);
    if (intervals.isEmpty()) {
      return Collections.emptyList();
    }
    SegmentTimeline timeline = jobParams.getTimeline(dataSource);

    if (timeline == null || timeline.isEmpty()) {
      LOG.warn("Segment timeline null or empty for [%s] skipping creating compaction jobs.", dataSource);
      return Collections.emptyList();
    }

    Interval adjustedTimelineInterval = applySkipOffset(
        new Interval(timeline.first().getInterval().getStart(), timeline.last().getInterval().getEnd()),
        jobParams.getScheduleStartTime()
    );
    if (adjustedTimelineInterval == null) {
      LOG.warn("All data for dataSource[%s] is within skip offsets, no reindexing jobs will be created", dataSource);
      return Collections.emptyList();
    }

    for (Interval reindexingInterval : intervals) {

      if (!reindexingInterval.overlaps(adjustedTimelineInterval)) {
        LOG.debug("Search interval[%s] does not overlap with data range[%s], skipping", reindexingInterval, adjustedTimelineInterval);
        continue;
      }

      reindexingInterval = clampIntervalToBounds(reindexingInterval, adjustedTimelineInterval);
      if (reindexingInterval == null) {
        LOG.warn("Clamped reindexing interval is empty after applying bounds, meaning no search interval exists. Skipping.");
        continue;
      }

      InlineSchemaDataSourceCompactionConfig.Builder builder = createBaseBuilder();

      ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(ruleProvider, reindexingInterval, currentTime);
      int ruleCount = configBuilder.applyTo(builder);

      if (ruleCount > 0) {
        LOG.info("Creating reindexing jobs for interval[%s] with [%d] rules selected", reindexingInterval, ruleCount);
        allJobs.addAll(
            createJobsForSearchInterval(
                createJobTemplateForInterval(builder.build()),
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

  protected CompactionJobTemplate createJobTemplateForInterval(
      InlineSchemaDataSourceCompactionConfig config
  )
  {
    return new CompactionConfigBasedJobTemplate(config, createCascadingFinalizer());
  }

  @Nullable
  private Interval clampIntervalToBounds(Interval interval, Interval bounds)
  {
    DateTime start = interval.getStart();
    DateTime end = interval.getEnd();

    if (start.isBefore(bounds.getStart())) {
      LOG.debug(
          "Adjusting start of search interval[%s] to match bounds start[%s]",
          interval,
          bounds.getStart()
      );
      start = bounds.getStart();
    }

    if (end.isAfter(bounds.getEnd())) {
      LOG.debug(
          "Adjusting end of search interval[%s] to match bounds end[%s]",
          interval,
          bounds.getEnd()
      );
      end = bounds.getEnd();
    }

    if (end.isBefore(start)) {
      return null;
    }

    return new Interval(start, end);
  }

  @Nullable
  private Interval applySkipOffset(
      Interval interval,
      DateTime skipFromNowReferenceTime
  )
  {
    DateTime maybeAdjustedEnd = interval.getEnd();
    if (skipOffsetFromNow != null) {
      maybeAdjustedEnd = skipFromNowReferenceTime.minus(skipOffsetFromNow);
    } else if (skipOffsetFromLatest != null) {
      maybeAdjustedEnd = maybeAdjustedEnd.minus(skipOffsetFromLatest);
    }
    if (maybeAdjustedEnd.isBefore(interval.getStart())) {
      return null;
    } else {
      return new Interval(interval.getStart(), maybeAdjustedEnd);
    }
  }

  private InlineSchemaDataSourceCompactionConfig.Builder createBaseBuilder()
  {
    return InlineSchemaDataSourceCompactionConfig.builder()
        .forDataSource(dataSource)
        .withTaskPriority(taskPriority)
        .withInputSegmentSizeBytes(inputSegmentSizeBytes)
        .withEngine(engine)
        .withTaskContext(taskContext)
        .withSkipOffsetFromLatest(Period.ZERO);
  }

  /**
   * Generates granularity-aligned search intervals based on segment granularity rules,
   * then splits them at non-segment-granularity rule thresholds where safe to do so.
   * <p>
   * Algorithm:
   * 1. Generate base timeline from segment granularity rules
   * 2. Collect thresholds from all non-segment-granularity rules
   * 3. For each base interval, find thresholds that fall within it
   * 4. Align those thresholds to the interval's segment granularity
   * 5. Split intervals at aligned thresholds
   *
   * @param referenceTime the reference time for calculating period thresholds
   * @return list of split and aligned intervals, ordered from oldest to newest
   * @throws IAE if no segment granularity rules are found
   */
  List<Interval> generateAlignedSearchIntervals(DateTime referenceTime)
  {
    // Step 1: Generate base timeline from segment granularity rules
    List<IntervalWithGranularity> baseTimeline = generateBaseTimelineWithGranularities(referenceTime);

    // Step 2: Collect thresholds from non-segment-granularity rules
    List<DateTime> nonSegmentGranThresholds = collectNonSegmentGranularityThresholds(referenceTime);

    // Step 3: For each base interval, collect and apply split points
    List<Interval> finalIntervals = new ArrayList<>();

    for (IntervalWithGranularity baseInterval : baseTimeline) {
      List<DateTime> splitPoints = new ArrayList<>();

      // Find thresholds that fall within this base interval
      for (DateTime threshold : nonSegmentGranThresholds) {
        if (threshold.isAfter(baseInterval.interval.getStart()) &&
            threshold.isBefore(baseInterval.interval.getEnd())) {

          // Align threshold to this interval's segment granularity
          DateTime alignedThreshold = baseInterval.granularity.bucketStart(threshold);

          // Only add if it's not at the boundaries (would create zero-length interval)
          if (alignedThreshold.isAfter(baseInterval.interval.getStart()) &&
              alignedThreshold.isBefore(baseInterval.interval.getEnd())) {
            splitPoints.add(alignedThreshold);
          }
        }
      }

      // Sort and deduplicate split points
      splitPoints = splitPoints.stream()
          .distinct()
          .sorted()
          .collect(Collectors.toList());

      // Split this base interval at the split points
      if (splitPoints.isEmpty()) {
        LOG.debug("No splits for interval [%s]", baseInterval.interval);
        finalIntervals.add(baseInterval.interval);
      } else {
        LOG.debug("Splitting interval [%s] at [%d] points", baseInterval.interval, splitPoints.size());
        DateTime start = baseInterval.interval.getStart();
        for (DateTime splitPoint : splitPoints) {
          finalIntervals.add(new Interval(start, splitPoint));
          start = splitPoint;
        }
        // Add the final segment
        finalIntervals.add(new Interval(start, baseInterval.interval.getEnd()));
      }
    }

    return finalIntervals;
  }

  /**
   * Generates the base timeline with segment granularities tracked.
   */
  private List<IntervalWithGranularity> generateBaseTimelineWithGranularities(DateTime referenceTime)
  {
    List<ReindexingSegmentGranularityRule> segmentGranRules = ruleProvider.getSegmentGranularityRules();

    if (segmentGranRules.isEmpty()) {
      throw new IAE(
          "CascadingReindexingTemplate requires at least one segment granularity rule. "
          + "TODO: Support templates with only non-segment-granularity rules"
      );
    }

    // Sort rules by period from longest to shortest (oldest to most recent threshold)
    List<ReindexingSegmentGranularityRule> sortedRules = segmentGranRules.stream()
        .sorted(Comparator.comparingLong(rule -> {
          DateTime threshold = referenceTime.minus(rule.getOlderThan());
          return threshold.getMillis(); // Ascending = oldest first
        }))
        .collect(Collectors.toList());

    // Build base timeline with granularities tracked
    List<IntervalWithGranularity> baseTimeline = new ArrayList<>();
    DateTime previousAlignedEnd = null;

    for (ReindexingSegmentGranularityRule rule : sortedRules) {
      DateTime rawEnd = referenceTime.minus(rule.getOlderThan());
      DateTime alignedEnd = rule.getSegmentGranularity().bucketStart(rawEnd);
      DateTime alignedStart = (previousAlignedEnd != null) ? previousAlignedEnd : DateTimes.MIN;

      LOG.debug(
          "Base interval for rule [%s]: raw end [%s] -> aligned [%s/%s) with granularity [%s]",
          rule.getId(),
          rawEnd,
          alignedStart,
          alignedEnd,
          rule.getSegmentGranularity()
      );

      baseTimeline.add(new IntervalWithGranularity(
          new Interval(alignedStart, alignedEnd),
          rule.getSegmentGranularity()
      ));

      previousAlignedEnd = alignedEnd;
    }

    return baseTimeline;
  }

  /**
   * Collects thresholds from all non-segment-granularity rules.
   */
  private List<DateTime> collectNonSegmentGranularityThresholds(DateTime referenceTime)
  {
    List<DateTime> thresholds = new ArrayList<>();

    // Collect from all rule types
    ruleProvider.getMetricsRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getDimensionsRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getIOConfigRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getProjectionRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getQueryGranularityRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getTuningConfigRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    ruleProvider.getDeletionRules().stream()
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .forEach(thresholds::add);

    return thresholds;
  }

  /**
   * Helper class to track an interval with its associated segment granularity.
   */
  private static class IntervalWithGranularity
  {
    final Interval interval;
    final Granularity granularity;

    IntervalWithGranularity(Interval interval, Granularity granularity)
    {
      this.interval = interval;
      this.granularity = granularity;
    }
  }

  @Deprecated
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
