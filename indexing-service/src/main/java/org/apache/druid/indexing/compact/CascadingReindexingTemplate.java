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
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.server.compaction.ReindexingDataSchemaRule;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingIOConfigRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.compaction.ReindexingTuningConfigRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
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
  private final Granularity defaultSegmentGranularity;

  @JsonCreator
  public CascadingReindexingTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("ruleProvider") ReindexingRuleProvider ruleProvider,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("skipOffsetFromNow") @Nullable Period skipOffsetFromNow,
      @JsonProperty("defaultSegmentGranularity") Granularity defaultSegmentGranularity
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource, "'dataSource' cannot be null");
    this.ruleProvider = Objects.requireNonNull(ruleProvider, "'ruleProvider' cannot be null");
    this.engine = engine;
    this.taskContext = taskContext;
    this.taskPriority = Objects.requireNonNullElse(taskPriority, DEFAULT_COMPACTION_TASK_PRIORITY);
    this.inputSegmentSizeBytes = Objects.requireNonNullElse(inputSegmentSizeBytes, DEFAULT_INPUT_SEGMENT_SIZE_BYTES);
    this.defaultSegmentGranularity = Objects.requireNonNull(
        defaultSegmentGranularity,
        "'defaultSegmentGranularity' cannot be null"
    );

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

  @JsonProperty
  public Granularity getDefaultSegmentGranularity()
  {
    return defaultSegmentGranularity;
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

  /**
   * Generates a timeline view showing the search intervals and their associated compaction
   * configurations. This is useful for operators to understand how rules are applied across
   * different time periods without actually creating compaction jobs.
   *
   * @param referenceTime the reference time to use for computing rule periods (typically DateTime.now())
   * @return a view of the compaction timeline with intervals and their configs
   */
  public CompactionTimelineView getCompactionTimelineView(DateTime referenceTime)
  {
    if (!ruleProvider.isReady()) {
      LOG.info(
          "Rule provider [%s] is not ready, returning empty timeline for dataSource[%s]",
          ruleProvider.getType(),
          dataSource
      );
      return new CompactionTimelineView(dataSource, referenceTime, null, Collections.emptyList(), null);
    }

    List<Interval> searchIntervals;
    try {
      searchIntervals = generateAlignedSearchIntervals(referenceTime);
    }
    catch (GranularityTimelineValidationException e) {
      // Validation failed - extract structured error details and return with validation error
      LOG.warn(e, "Validation failed for compaction timeline of dataSource[%s]", dataSource);
      CompactionTimelineView.ValidationError validationError = new CompactionTimelineView.ValidationError(
          "INVALID_GRANULARITY_TIMELINE",
          e.getMessage(),
          e.getOlderInterval().toString(),
          e.getOlderGranularity().toString(),
          e.getNewerInterval().toString(),
          e.getNewerGranularity().toString()
      );
      return new CompactionTimelineView(dataSource, referenceTime, null, Collections.emptyList(), validationError);
    }
    catch (IAE e) {
      // Other validation errors (e.g., no rules configured)
      LOG.warn(e, "Validation failed for compaction timeline of dataSource[%s]", dataSource);
      CompactionTimelineView.ValidationError validationError = new CompactionTimelineView.ValidationError(
          "VALIDATION_ERROR",
          e.getMessage(),
          null,
          null,
          null,
          null
      );
      return new CompactionTimelineView(dataSource, referenceTime, null, Collections.emptyList(), validationError);
    }

    if (searchIntervals.isEmpty()) {
      LOG.warn("No search intervals generated for dataSource[%s]", dataSource);
      return new CompactionTimelineView(dataSource, referenceTime, null, Collections.emptyList(), null);
    }

    // Calculate effective end time based on skip offset
    DateTime effectiveEndTime = referenceTime;
    CompactionTimelineView.SkipOffsetInfo skipOffsetInfo = null;

    if (skipOffsetFromNow != null) {
      effectiveEndTime = referenceTime.minus(skipOffsetFromNow);
      CompactionTimelineView.AppliedSkipOffset applied = new CompactionTimelineView.AppliedSkipOffset(
          "skipOffsetFromNow",
          skipOffsetFromNow,
          effectiveEndTime
      );
      skipOffsetInfo = new CompactionTimelineView.SkipOffsetInfo(applied, null);
    } else if (skipOffsetFromLatest != null) {
      // skipOffsetFromLatest requires actual timeline data, so we can't apply it in preview mode
      CompactionTimelineView.NotAppliedSkipOffset notApplied = new CompactionTimelineView.NotAppliedSkipOffset(
          "skipOffsetFromLatest",
          skipOffsetFromLatest,
          "Requires actual segment timeline data"
      );
      skipOffsetInfo = new CompactionTimelineView.SkipOffsetInfo(null, notApplied);
    }

    // Build configs for each interval
    List<CompactionTimelineView.IntervalConfig> intervalConfigs = new ArrayList<>();
    for (Interval searchInterval : searchIntervals) {
      // Clamp interval to effective end time
      Interval clampedInterval = searchInterval;
      if (searchInterval.getEnd().isAfter(effectiveEndTime)) {
        if (searchInterval.getStart().isBefore(effectiveEndTime)) {
          clampedInterval = new Interval(searchInterval.getStart(), effectiveEndTime);
        } else {
          // Entire interval is beyond skip offset, skip it
          continue;
        }
      }

      InlineSchemaDataSourceCompactionConfig.Builder builder = createBaseBuilder();
      ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
          ruleProvider,
          defaultSegmentGranularity,
          clampedInterval,
          referenceTime
      );
      int ruleCount = configBuilder.applyTo(builder);

      if (ruleCount > 0) {
        // Collect the ACTUAL rules that were applied (matching ReindexingConfigBuilder logic)
        List<ReindexingRule> appliedRules = new ArrayList<>();

        ReindexingTuningConfigRule tuningRule = ruleProvider.getTuningConfigRule(clampedInterval, referenceTime);
        if (tuningRule != null) {
          appliedRules.add(tuningRule);
        }

        ReindexingIOConfigRule ioConfigRule = ruleProvider.getIOConfigRule(clampedInterval, referenceTime);
        if (ioConfigRule != null) {
          appliedRules.add(ioConfigRule);
        }

        ReindexingDataSchemaRule dataSchemaRule = ruleProvider.getDataSchemaRule(clampedInterval, referenceTime);
        if (dataSchemaRule != null) {
          appliedRules.add(dataSchemaRule);
        }

        // Deletion rules are additive - collect all that apply
        List<ReindexingDeletionRule> deletionRules = ruleProvider.getDeletionRules(clampedInterval, referenceTime);
        appliedRules.addAll(deletionRules);

        ReindexingSegmentGranularityRule segmentGranularityRule = ruleProvider.getSegmentGranularityRule(clampedInterval, referenceTime);
        if (segmentGranularityRule != null) {
          appliedRules.add(segmentGranularityRule);
        }

        intervalConfigs.add(new CompactionTimelineView.IntervalConfig(
            clampedInterval,
            ruleCount,
            builder.build(),
            appliedRules
        ));
      }
    }

    return new CompactionTimelineView(dataSource, referenceTime, skipOffsetInfo, intervalConfigs, null);
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

    SegmentTimeline timeline = jobParams.getTimeline(dataSource);

    if (timeline == null || timeline.isEmpty()) {
      LOG.warn("Segment timeline null or empty for [%s] skipping creating compaction jobs.", dataSource);
      return Collections.emptyList();
    }

    List<Interval> searchIntervals = generateAlignedSearchIntervals(currentTime);
    if (searchIntervals.isEmpty()) {
      LOG.warn("No search intervals generated for dataSource[%s], no reindexing jobs will be created", dataSource);
      return Collections.emptyList();
    }

    // Adjust timeline interval by applying user defined skip offset (if any exists)
    Interval adjustedTimelineInterval = applySkipOffset(
        new Interval(timeline.first().getInterval().getStart(), timeline.last().getInterval().getEnd()),
        jobParams.getScheduleStartTime()
    );
    if (adjustedTimelineInterval == null) {
      LOG.warn("All data for dataSource[%s] is within skip offsets, no reindexing jobs will be created", dataSource);
      return Collections.emptyList();
    }

    for (Interval reindexingInterval : searchIntervals) {

      if (!reindexingInterval.overlaps(adjustedTimelineInterval)) {
        // No underlying data exists to reindex for this interval
        LOG.debug("Search interval[%s] does not overlap with data range[%s], skipping", reindexingInterval, adjustedTimelineInterval);
        continue;
      }

      reindexingInterval = clampIntervalToBounds(reindexingInterval, adjustedTimelineInterval);
      if (reindexingInterval == null) {
        LOG.warn("Clamped reindexing interval is invalid or empty after applying bounds, meaning no search interval exists. Skipping.");
        continue;
      }

      InlineSchemaDataSourceCompactionConfig.Builder builder = createBaseBuilder();

      ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
          ruleProvider,
          defaultSegmentGranularity,
          reindexingInterval,
          currentTime
      );
      int ruleCount = configBuilder.applyTo(builder);

      if (ruleCount > 0) {
        LOG.debug("Creating reindexing jobs for interval[%s] with [%d] rules selected", reindexingInterval, ruleCount);
        allJobs.addAll(
            createJobsForSearchInterval(
                createJobTemplateForInterval(builder.build()),
                reindexingInterval,
                source,
                jobParams
            )
        );
      } else {
        LOG.debug("No applicable reindexing rules found for interval[%s]", reindexingInterval);
      }
    }
    return allJobs;
  }

  @VisibleForTesting
  protected CompactionJobTemplate createJobTemplateForInterval(
      InlineSchemaDataSourceCompactionConfig config
  )
  {
    return new CompactionConfigBasedJobTemplate(config, createCascadingFinalizer());
  }

  /**
   * Clamps an interval to fit within the specified bounds by adjusting start and/or end times.
   * If the interval extends beyond the bounds, it is trimmed to fit. Returns null if the
   * resulting interval would be invalid (end before start).
   *
   * @param interval the interval to clamp
   * @param bounds the bounds to clamp to
   * @return the clamped interval, or null if the result would be invalid or empty
   */
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

    if (end.isBefore(start) || end.isEqual(start)) {
      return null;
    }

    return new Interval(start, end);
  }

  /**
   * Applies the configured skip offset to an interval by adjusting its end time. Uses either
   * skipOffsetFromNow (relative to reference time) or skipOffsetFromLatest (relative to interval end).
   * Returns null if the adjusted end would be before the interval start.
   *
   * @param interval the interval to adjust
   * @param skipFromNowReferenceTime the reference time for skipOffsetFromNow calculation
   * @return the interval with adjusted end time, or null if the result would be invalid
   */
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
   * <ol>
   *   <li>Generate base timeline from segment granularity rules with interval boundaries aligned with segment granularity of the underlying rules</li>
   *   <li>Collect olderThan application thresholds from all non-segment-granularity rules</li>
   *   <li>For each interval in the base timeline:
   *   <ul>
   *     <li>find olderThan thresholds for non-segment granularity rules that fall within it</li>
   *     <li>Align those thresholds to the interval's targeted segment granularity using bucketStart on the threshold date</li>
   *     <li>Split base intervals at the granularity aligned thresholds that were found inside of them</li>
   *   </ul>
   *   <li>Return the timeline of non-overlapping intervals split for most precise possible rule application (due to segment gran alignment, sometimes rules will be applied later than their explicitly defined period)</li>
   * </ol>
   *
   * @param referenceTime the reference time for calculating period thresholds
   * @return list of split and aligned intervals, ordered from oldest to newest
   * @throws IAE if no segment granularity rules are found
   */
  List<Interval> generateAlignedSearchIntervals(DateTime referenceTime)
  {
    List<IntervalWithGranularity> baseTimeline = generateBaseSegmentGranularityAlignedTimeline(referenceTime);

    List<DateTime> nonSegmentGranThresholds = collectNonSegmentGranularityThresholds(referenceTime);

    List<Interval> finalIntervals = new ArrayList<>();

    for (IntervalWithGranularity baseInterval : baseTimeline) {
      List<DateTime> splitPoints = new ArrayList<>();

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
        finalIntervals.add(new Interval(start, baseInterval.interval.getEnd()));
      }
    }

    return finalIntervals;
  }

  /**
   * Generates a base timeline aligned to segment granularities found in segment granularity rules and if necessary,
   * the default granularity for the supervisor.
   * <p>
   * Algorithm:
   * <ol>
   *   <li>If no segment granularity rules exist:
   *     <ol type="a">
   *       <li>Find the most recent threshold from non-segment-granularity rules</li>
   *       <li>Use the default granularity to granularity align an interval from [-inf, most recent threshold)</li>
   *     </ol>
   *   </li>
   *   <li>If segment granularity rules exist:
   *     <ol type="a">
   *       <li>Sort rules by period from longest to shortest (oldest to most recent threshold)</li>
   *       <li>Create intervals for each rule, adjusting the interval end to be aligned to the rule's segment granularity</li>
   *       <li>If non-segment-granularity thresholds exist that are more recent than the most recent segment granularity rule's end:
   *         <ol type="i">
   *           <li>Prepend an interval from [most recent segment granularity rule interval end, most recent non-segment-granularity threshold)</li>
   *         </ol>
   *       </li>
   *     </ol>
   *   </li>
   * </ol>
   */
  private List<IntervalWithGranularity> generateBaseSegmentGranularityAlignedTimeline(DateTime referenceTime)
  {
    List<ReindexingSegmentGranularityRule> segmentGranRules = ruleProvider.getSegmentGranularityRules();
    List<DateTime> nonSegmentGranThresholds = collectNonSegmentGranularityThresholds(referenceTime);

    if (segmentGranRules.isEmpty()) {
      // No segment granularity rules - use default granularity with the smallest period from other rules
      if (nonSegmentGranThresholds.isEmpty()) {
        throw new IAE(
            "CascadingReindexingTemplate requires at least one reindexing rule "
            + "(segment granularity or other type)"
        );
      }

      // Find the smallest period (most recent threshold = largest DateTime value)
      DateTime mostRecentThreshold = Collections.max(nonSegmentGranThresholds);
      DateTime alignedEnd = defaultSegmentGranularity.bucketStart(mostRecentThreshold);

      LOG.debug(
          "No segment granularity rules found for cascading supervisor[%s]. Creating base interval with "
          + "default granularity [%s] and threshold [%s] (aligned: [%s])",
          dataSource,
          defaultSegmentGranularity,
          mostRecentThreshold,
          alignedEnd
      );

      return Collections.singletonList(new IntervalWithGranularity(
          new Interval(DateTimes.MIN, alignedEnd),
          defaultSegmentGranularity
      ));
    }

    // Sort rules by period from longest to shortest (oldest to most recent threshold)
    List<ReindexingSegmentGranularityRule> sortedRules = segmentGranRules.stream()
        .sorted(Comparator.comparingLong(rule -> {
          DateTime threshold = referenceTime.minus(rule.getOlderThan());
          return threshold.getMillis();
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

    // Check if we need to prepend an interval for non-segment-gran rules that are more recent
    // than the most recent segment gran rule
    if (!nonSegmentGranThresholds.isEmpty()) {
      DateTime mostRecentNonSegmentGranThreshold = Collections.max(nonSegmentGranThresholds);
      DateTime mostRecentSegmentGranEnd = baseTimeline.get(baseTimeline.size() - 1).interval.getEnd();

      if (mostRecentNonSegmentGranThreshold.isAfter(mostRecentSegmentGranEnd)) {
        DateTime alignedEnd = defaultSegmentGranularity.bucketStart(mostRecentNonSegmentGranThreshold);

        if (alignedEnd.isBefore(mostRecentSegmentGranEnd) || alignedEnd.isEqual(mostRecentSegmentGranEnd)) {
          LOG.debug(
              "Most recent non-segment-gran threshold [%s] aligns to [%s], which is not after "
              + "most recent segment granularity rule interval end [%s]. No prepended interval needed.",
              mostRecentNonSegmentGranThreshold,
              alignedEnd,
              mostRecentSegmentGranEnd
          );
          return baseTimeline;
        } else {
          LOG.debug(
              "Most recent non-segment-gran threshold [%s] is after most recent segment gran interval end [%s]. "
              + "Prepending interval with default granularity [%s] (aligned end: [%s])",
              mostRecentNonSegmentGranThreshold,
              mostRecentSegmentGranEnd,
              defaultSegmentGranularity,
              alignedEnd
          );

          baseTimeline.add(new IntervalWithGranularity(
              new Interval(mostRecentSegmentGranEnd, alignedEnd),
              defaultSegmentGranularity
          ));
        }
      }
    }

    // Validate the completed timeline before returning
    validateSegmentGranularityTimeline(baseTimeline);

    return baseTimeline;
  }

  /**
   * Validates that the completed segment granularity timeline follows the constraint that
   * granularity must stay the same or become finer as we move from past to present.
   * <p>
   * This ensures that operators cannot misconfigure rules that would cause data to be
   * recompacted from coarse to fine granularity as it ages (e.g., DAY -> HOUR),
   * which is typically undesirable and inefficient.
   *
   * @param timeline the completed base timeline with granularity information
   * @throws IAE if granularity becomes coarser as we move toward present
   */
  private void validateSegmentGranularityTimeline(List<IntervalWithGranularity> timeline)
  {
    if (timeline.size() <= 1) {
      return; // Nothing to validate
    }

    for (int i = 1; i < timeline.size(); i++) {
      IntervalWithGranularity olderInterval = timeline.get(i - 1);
      IntervalWithGranularity newerInterval = timeline.get(i);

      Granularity olderGran = olderInterval.granularity;
      Granularity newerGran = newerInterval.granularity;

      // As we move from past (older intervals) to present (newer intervals),
      // granularity should stay the same or get finer.
      // If the older interval's granularity is finer than the newer interval's granularity,
      // that means we're getting coarser as we move toward present, which is invalid.
      if (olderGran.isFinerThan(newerGran)) {
        throw new GranularityTimelineValidationException(
            dataSource,
            olderInterval.interval,
            olderGran,
            newerInterval.interval,
            newerGran
        );
      }
    }

    LOG.debug(
        "Segment granularity timeline validation passed for dataSource[%s] with [%d] intervals",
        dataSource,
        timeline.size()
    );
  }

  /**
   * Collects thresholds from all non-segment-granularity rules.
   */
  private List<DateTime> collectNonSegmentGranularityThresholds(DateTime referenceTime)
  {
    return ruleProvider.streamAllRules()
        .filter(rule -> !(rule instanceof ReindexingSegmentGranularityRule))
        .map(rule -> referenceTime.minus(rule.getOlderThan()))
        .collect(Collectors.toList());
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
