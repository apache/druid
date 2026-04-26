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
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.IntervalPartitioningInfo;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
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
  private static final ReindexingConfigOptimizer DELETION_RULE_OPTIMIZER = new ReindexingDeletionRuleOptimizer();


  public static final String TYPE = "reindexCascade";

  private final String dataSource;
  private final ReindexingRuleProvider ruleProvider;
  @Nullable
  private final Map<String, Object> taskContext;
  private final int taskPriority;
  private final long inputSegmentSizeBytes;
  private final Period skipOffsetFromLatest;
  private final Period skipOffsetFromNow;
  private final Granularity defaultSegmentGranularity;
  private final PartitionsSpec defaultPartitionsSpec;
  @Nullable
  private final UserCompactionTaskQueryTuningConfig tuningConfig;
  @Nullable
  private final VirtualColumns defaultPartitioningVirtualColumns;
  private final ReindexingPartitioningRule defaultPartitioningRule;

  @JsonCreator
  public CascadingReindexingTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("ruleProvider") ReindexingRuleProvider ruleProvider,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("skipOffsetFromNow") @Nullable Period skipOffsetFromNow,
      @JsonProperty("defaultSegmentGranularity") Granularity defaultSegmentGranularity,
      @JsonProperty("defaultPartitionsSpec") PartitionsSpec defaultPartitionsSpec,
      @JsonProperty("defaultPartitioningVirtualColumns") @Nullable VirtualColumns defaultPartitioningVirtualColumns,
      @JsonProperty("tuningConfig") @Nullable UserCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    InvalidInput.conditionalException(dataSource != null, "'dataSource' cannot be null");
    this.dataSource = dataSource;

    InvalidInput.conditionalException(ruleProvider != null, "'ruleProvider' cannot be null");
    this.ruleProvider = ruleProvider;

    this.taskContext = taskContext;
    this.taskPriority = Objects.requireNonNullElse(taskPriority, DEFAULT_COMPACTION_TASK_PRIORITY);
    this.inputSegmentSizeBytes = Objects.requireNonNullElse(inputSegmentSizeBytes, DEFAULT_INPUT_SEGMENT_SIZE_BYTES);

    InvalidInput.conditionalException(defaultSegmentGranularity != null, "'defaultSegmentGranularity' cannot be null");
    this.defaultSegmentGranularity = defaultSegmentGranularity;

    InvalidInput.conditionalException(defaultPartitionsSpec != null, "'defaultPartitionsSpec' cannot be null");
    this.defaultPartitionsSpec = defaultPartitionsSpec;

    this.defaultPartitioningVirtualColumns = defaultPartitioningVirtualColumns;

    if (tuningConfig != null && tuningConfig.getPartitionsSpec() != null) {
      throw InvalidInput.exception(
          "Cannot set 'partitionsSpec' inside 'tuningConfig' for a cascading reindexing supervisor. "
          + "Partitioning is controlled by 'defaultPartitionsSpec' and partitioning rules. "
          + "Any 'partitionsSpec' in 'tuningConfig' would be ignored."
      );
    }
    this.tuningConfig = tuningConfig;

    if (skipOffsetFromNow != null && skipOffsetFromLatest != null) {
      throw InvalidInput.exception("Cannot set both skipOffsetFromNow and skipOffsetFromLatest");
    }
    this.skipOffsetFromNow = skipOffsetFromNow;
    this.skipOffsetFromLatest = skipOffsetFromLatest;

    this.defaultPartitioningRule = ReindexingPartitioningRule.syntheticRule(
        defaultSegmentGranularity,
        defaultPartitionsSpec,
        defaultPartitioningVirtualColumns
    );
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

  @Override
  public CompactionEngine getEngine()
  {
    return CompactionEngine.MSQ;
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

  @JsonProperty
  public PartitionsSpec getDefaultPartitionsSpec()
  {
    return defaultPartitionsSpec;
  }

  @JsonProperty
  public VirtualColumns getDefaultPartitioningVirtualColumns()
  {
    return defaultPartitioningVirtualColumns;
  }

  @Nullable
  @Override
  @JsonProperty
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  /**
   * Validates this template using a subset of the standard MSQ compaction checks.
   * The standard path in {@link ClientCompactionRunnerInfo#validateCompactionConfig}
   * assumes partitioning is controlled by {@code tuningConfig.partitionsSpec}, but
   * this template forbids that field and uses {@code defaultPartitionsSpec} instead.
   *
   * <p>Checks performed:
   * <ul>
   *   <li>partitionsSpec type and options — validated against {@code defaultPartitionsSpec}.
   *       Range partition dimension type checking passes {@code null} for dimensionSchemas
   *       since those are not known at template level.</li>
   *   <li>maxNumTasks >= 2 in taskContext.</li>
   * </ul>
   *
   * <p>Standard MSQ checks skipped (not applicable at template level):
   * <ul>
   *   <li>rollup vs metricsSpec consistency — {@code granularitySpec} is always null on the
   *       template; rollup is configured per-rule at job generation time.</li>
   *   <li>metricsSpec aggregator combining factory — there is no metricsSpec on the template;
   *       metrics come from per-rule data schema rules resolved at job generation time.</li>
   * </ul>
   *
   * <p>Per-rule overrides (partitionsSpec, metricsSpec, rollup) are validated at task
   * runtime by {@code MSQCompactionRunner.validateCompactionTask()} once the full config
   * is resolved against actual data schemas.
   */
  @Override
  public CompactionConfigValidationResult validate(ClusterCompactionConfig clusterCompactionConfig)
  {
    List<CompactionConfigValidationResult> results = new ArrayList<>();

    results.add(ClientCompactionRunnerInfo.validatePartitionsSpecForMSQ(
        this.getDefaultPartitionsSpec(),
        null,
        this.getDefaultPartitioningVirtualColumns() != null
        ? this.getDefaultPartitioningVirtualColumns()
        : VirtualColumns.EMPTY
    ));

    results.add(ClientCompactionRunnerInfo.validateMaxNumTasksForMSQ(this.getTaskContext()));

    return results.stream()
                  .filter(result -> !result.isValid())
                  .findFirst()
                  .orElse(CompactionConfigValidationResult.success());
  }

  /**
   * Checks if the given interval's end time is after the specified boundary.
   * Used to determine if intervals should be skipped based on skip offset configuration.
   *
   * @param interval the interval to check
   * @param boundary the boundary time to compare against
   * @return true if the interval ends after the boundary
   */
  private static boolean intervalEndsAfter(Interval interval, DateTime boundary)
  {
    return interval.getEnd().isAfter(boundary);
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

    List<IntervalPartitioningInfo> searchIntervals = generateAlignedSearchIntervals(currentTime);
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

    for (IntervalPartitioningInfo intervalInfo : searchIntervals) {
      Interval reindexingInterval = intervalInfo.getInterval();

      if (!reindexingInterval.overlaps(adjustedTimelineInterval)) {
        // No underlying data exists to reindex for this interval
        LOG.debug("Search interval[%s] does not overlap with data range[%s], skipping", reindexingInterval, adjustedTimelineInterval);
        continue;
      }

      // Skip intervals that extend past the skip offset boundary (not just data boundary)
      // This preserves granularity alignment and ensures intervals exist in synthetic timeline
      // Only apply this when a skip offset is actually configured
      if ((skipOffsetFromNow != null || skipOffsetFromLatest != null) &&
          intervalEndsAfter(reindexingInterval, adjustedTimelineInterval.getEnd())) {
        LOG.debug("Search interval[%s] extends past skip offset boundary[%s], skipping to preserve alignment",
                  reindexingInterval, adjustedTimelineInterval.getEnd());
        continue;
      }

      InlineSchemaDataSourceCompactionConfig.Builder builder = createBaseBuilder();

      ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
          ruleProvider,
          reindexingInterval,
          currentTime,
          searchIntervals,
          tuningConfig
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
    return new CompactionConfigBasedJobTemplate(config, DELETION_RULE_OPTIMIZER);
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
    return InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(dataSource)
        .withTaskPriority(taskPriority)
        .withInputSegmentSizeBytes(inputSegmentSizeBytes)
        .withEngine(CompactionEngine.MSQ)
        .withTaskContext(taskContext)
        .withSkipOffsetFromLatest(Period.ZERO); // We handle skip offsets at the timeline level, we know we want to cover the entirety of the interval
  }

  /**
   * Generates granularity-aligned search intervals based on partitioning rules,
   * then splits them at non-partitioning rule thresholds where safe to do so.
   * <p>
   * Algorithm:
   * <ol>
   *   <li>Generate base timeline from partitioning rules with interval boundaries aligned with segment granularity of the underlying rules</li>
   *   <li>Collect olderThan application thresholds from all non-partitioning rules</li>
   *   <li>For each interval in the base timeline:
   *   <ul>
   *     <li>find olderThan thresholds for non-partitioning rules that fall within it</li>
   *     <li>Align those thresholds to the interval's targeted segment granularity using bucketStart on the threshold date</li>
   *     <li>Split base intervals at the granularity aligned thresholds that were found inside of them</li>
   *   </ul>
   *   <li>Return the timeline of non-overlapping intervals split for most precise possible rule application (due to segment gran alignment, sometimes rules will be applied later than their explicitly defined period)</li>
   * </ol>
   *
   * @param referenceTime the reference time for calculating period thresholds
   * @return list of split and aligned intervals with their granularities and source rules, ordered from oldest to newest
   * @throws IAE if no reindexing rules are configured
   * @throws SegmentGranularityTimelineValidationException if granularities become coarser over time
   */
  List<IntervalPartitioningInfo> generateAlignedSearchIntervals(DateTime referenceTime)
  {
    List<IntervalPartitioningInfo> baseTimeline = generateBasePartitioningAlignedTimeline(referenceTime);
    List<DateTime> nonPartitioningThresholds = collectNonPartitioningThresholds(referenceTime);

    List<IntervalPartitioningInfo> finalIntervals = new ArrayList<>();
    for (IntervalPartitioningInfo baseInterval : baseTimeline) {
      List<DateTime> splitPoints = findGranularityAlignedSplitPoints(baseInterval, nonPartitioningThresholds);
      finalIntervals.addAll(splitIntervalAtPoints(baseInterval, splitPoints));
    }

    return finalIntervals;
  }

  /**
   * Finds split points within a base interval by aligning non-partitioning thresholds
   * to the interval's segment granularity. Only includes thresholds that fall strictly inside
   * the interval (not at boundaries, which would create zero-length intervals).
   *
   * @param baseInterval the interval to find split points for
   * @param nonPartitioningThresholds thresholds from non-partitioning rules
   * @return sorted, distinct list of aligned split points that fall inside the interval
   */
  private List<DateTime> findGranularityAlignedSplitPoints(
      IntervalPartitioningInfo baseInterval,
      List<DateTime> nonPartitioningThresholds
  )
  {
    List<DateTime> splitPoints = new ArrayList<>();

    for (DateTime threshold : nonPartitioningThresholds) {
      // Check if threshold falls inside this interval
      if (threshold.isAfter(baseInterval.getInterval().getStart()) &&
          threshold.isBefore(baseInterval.getInterval().getEnd())) {

        // Align threshold to this interval's segment granularity
        DateTime alignedThreshold = baseInterval.getGranularity().bucketStart(threshold);

        // Only add if it's not at the boundaries (would create zero-length interval)
        if (alignedThreshold.isAfter(baseInterval.getInterval().getStart()) &&
            alignedThreshold.isBefore(baseInterval.getInterval().getEnd())) {
          splitPoints.add(alignedThreshold);
        }
      }
    }

    // Remove duplicates and sort
    return splitPoints.stream()
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Splits a base interval at the given split points, preserving the interval's granularity
   * and source rule. If no split points exist, returns the original interval unchanged.
   *
   * @param baseInterval the interval to split
   * @param splitPoints sorted list of points to split at (must be inside the interval)
   * @return list of split intervals, or singleton list with original interval if no splits
   */
  private List<IntervalPartitioningInfo> splitIntervalAtPoints(
      IntervalPartitioningInfo baseInterval,
      List<DateTime> splitPoints
  )
  {
    if (splitPoints.isEmpty()) {
      LOG.debug("No splits for interval [%s]", baseInterval.getInterval());
      return Collections.singletonList(baseInterval);
    }

    LOG.debug("Splitting interval [%s] at [%d] points", baseInterval.getInterval(), splitPoints.size());

    List<IntervalPartitioningInfo> result = new ArrayList<>();
    DateTime start = baseInterval.getInterval().getStart();

    for (DateTime splitPoint : splitPoints) {
      result.add(
          new IntervalPartitioningInfo(
              new Interval(start, splitPoint),
              baseInterval.getSourceRule(),
              baseInterval.isRuleSynthetic()
          )
      );
      start = splitPoint;
    }

    // Add final interval from last split point to end
    result.add(
        new IntervalPartitioningInfo(
            new Interval(start, baseInterval.getInterval().getEnd()),
            baseInterval.getSourceRule(),
            baseInterval.isRuleSynthetic()
        )
    );

    return result;
  }

  /**
   * Generates a base timeline aligned to segment granularities found in partitioning rules and if necessary,
   * the default granularity for the supervisor.
   * <p>
   * Algorithm:
   * <ol>
   *   <li>If no partitioning rules exist:
   *     <ol type="a">
   *       <li>Find the most recent threshold from non-partitioning rules</li>
   *       <li>Use the default granularity to granularity align an interval from [-inf, most recent threshold)</li>
   *     </ol>
   *   </li>
   *   <li>If partitioning rules exist:
   *     <ol type="a">
   *       <li>Sort rules by period from longest to shortest (oldest to most recent threshold)</li>
   *       <li>Create intervals for each rule, adjusting the interval end to be aligned to the rule's segment granularity</li>
   *       <li>If non-partitioning thresholds exist that are more recent than the most recent partitioning rule's end:
   *         <ol type="i">
   *           <li>Prepend an interval from [most recent partitioning rule interval end, most recent non-partitioning threshold)</li>
   *         </ol>
   *       </li>
   *     </ol>
   *   </li>
   * </ol>
   *
   * @param referenceTime the reference time for calculating period thresholds
   * @return base timeline with granularity-aligned intervals, ordered from oldest to newest
   * @throws IAE if no reindexing rules are configured
   * @throws SegmentGranularityTimelineValidationException if granularities become coarser over time
   */
  private List<IntervalPartitioningInfo> generateBasePartitioningAlignedTimeline(DateTime referenceTime)
  {
    List<ReindexingPartitioningRule> partitioningRules = ruleProvider.getPartitioningRules();
    List<DateTime> nonPartitioningThresholds = collectNonPartitioningThresholds(referenceTime);

    List<IntervalPartitioningInfo> baseTimeline;

    if (partitioningRules.isEmpty()) {
      baseTimeline = createDefaultGranularityTimeline(nonPartitioningThresholds);
    } else {
      baseTimeline = createPartitioningTimeline(partitioningRules, referenceTime);
      baseTimeline = maybePrependRecentInterval(baseTimeline, nonPartitioningThresholds);
    }

    validateSegmentGranularityTimeline(baseTimeline);
    return baseTimeline;
  }

  /**
   * Creates a timeline using the default segment granularity and partitionspec when no partitioning rules exist.
   * Uses the most recent threshold from non-partitioning rules to determine the end boundary.
   *
   * @param nonPartitioningThresholds thresholds from non-partitioning rules
   * @return single-interval timeline from MIN to most recent threshold, aligned to default granularity
   * @throws IAE if no non-partitioning rules exist either
   */
  private List<IntervalPartitioningInfo> createDefaultGranularityTimeline(List<DateTime> nonPartitioningThresholds)
  {
    if (nonPartitioningThresholds.isEmpty()) {
      throw InvalidInput.exception(
          "CascadingReindexingTemplate requires at least one reindexing rule (partitioning or other type)"
      );
    }

    // Find the smallest period (most recent threshold = largest DateTime value)
    DateTime mostRecentThreshold = Collections.max(nonPartitioningThresholds);
    DateTime alignedEnd = defaultSegmentGranularity.bucketStart(mostRecentThreshold);

    LOG.debug(
        "No partitioning rules found for cascading supervisor[%s]. Creating base interval with "
        + "default granularity [%s] and threshold [%s] (aligned: [%s])",
        dataSource,
        defaultSegmentGranularity,
        mostRecentThreshold,
        alignedEnd
    );

    return Collections.singletonList(
        new IntervalPartitioningInfo(
            new Interval(DateTimes.MIN, alignedEnd),
            defaultPartitioningRule,
            true
        )
    );
  }

  /**
   * Creates a timeline by processing partitioning rules in chronological order (oldest to newest).
   * Each rule defines an interval with its specific segment granularity, with boundaries aligned to that granularity.
   *
   * @param partitioningRules partitioning rules to process
   * @param referenceTime reference time for computing rule thresholds
   * @return timeline of intervals with their granularities, ordered from oldest to newest
   */
  private List<IntervalPartitioningInfo> createPartitioningTimeline(
      List<ReindexingPartitioningRule> partitioningRules,
      DateTime referenceTime
  )
  {
    // Sort rules by period from longest to shortest (oldest to most recent threshold)
    List<ReindexingPartitioningRule> sortedRules = partitioningRules.stream()
        .sorted(Comparator.comparingLong(rule -> {
          DateTime threshold = referenceTime.minus(rule.getOlderThan());
          return threshold.getMillis();
        }))
        .toList();

    // Build base timeline with granularities tracked
    List<IntervalPartitioningInfo> baseTimeline = new ArrayList<>();
    DateTime previousAlignedEnd = null;

    for (ReindexingPartitioningRule rule : sortedRules) {
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

      baseTimeline.add(new IntervalPartitioningInfo(
          new Interval(alignedStart, alignedEnd),
          rule  // Track the source rule
      ));

      previousAlignedEnd = alignedEnd;
    }

    return baseTimeline;
  }

  /**
   * Checks if non-partitioning rules have more recent thresholds than the most recent
   * partitioning rule, and if so, prepends an interval with the default granularity.
   * This ensures that all rules (not just partitioning rules) are represented in the timeline.
   *
   * @param baseTimeline existing timeline built from partitioning rules
   * @param nonPartitioningThresholds thresholds from non-partitioning rules
   * @return updated timeline with prepended interval if needed, otherwise original timeline
   */
  private List<IntervalPartitioningInfo> maybePrependRecentInterval(
      List<IntervalPartitioningInfo> baseTimeline,
      List<DateTime> nonPartitioningThresholds
  )
  {
    if (nonPartitioningThresholds.isEmpty()) {
      return baseTimeline;
    }

    DateTime mostRecentNonPartitioningThreshold = Collections.max(nonPartitioningThresholds);
    DateTime mostRecentPartitioningEnd = baseTimeline.get(baseTimeline.size() - 1).getInterval().getEnd();

    if (!mostRecentNonPartitioningThreshold.isAfter(mostRecentPartitioningEnd)) {
      return baseTimeline;
    }

    DateTime alignedEnd = defaultSegmentGranularity.bucketStart(mostRecentNonPartitioningThreshold);

    if (alignedEnd.isBefore(mostRecentPartitioningEnd) || alignedEnd.isEqual(mostRecentPartitioningEnd)) {
      LOG.debug(
          "Most recent non-partitioning threshold [%s] aligns to [%s], which is not after "
          + "most recent partitioning rule interval end [%s]. No prepended interval needed.",
          mostRecentNonPartitioningThreshold,
          alignedEnd,
          mostRecentPartitioningEnd
      );
      return baseTimeline;
    }

    LOG.debug(
        "Most recent non-partitioning threshold [%s] is after most recent partitioning interval end [%s]. "
        + "Prepending interval with default granularity [%s] (aligned end: [%s])",
        mostRecentNonPartitioningThreshold,
        mostRecentPartitioningEnd,
        defaultSegmentGranularity,
        alignedEnd
    );

    // Create new list with prepended interval (don't modify original)
    List<IntervalPartitioningInfo> updatedTimeline = new ArrayList<>(baseTimeline);
    updatedTimeline.add(
        new IntervalPartitioningInfo(
            new Interval(mostRecentPartitioningEnd, alignedEnd),
            defaultPartitioningRule,
            true
        )
    );

    return updatedTimeline;
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
   * @throws SegmentGranularityTimelineValidationException if granularity becomes coarser as we move toward present
   */
  private void validateSegmentGranularityTimeline(List<IntervalPartitioningInfo> timeline)
  {
    if (timeline.size() <= 1) {
      return; // Nothing to validate
    }

    for (int i = 1; i < timeline.size(); i++) {
      IntervalPartitioningInfo olderInterval = timeline.get(i - 1);
      IntervalPartitioningInfo newerInterval = timeline.get(i);

      Granularity olderGran = olderInterval.getGranularity();
      Granularity newerGran = newerInterval.getGranularity();

      // As we move from past (older intervals) to present (newer intervals),
      // granularity should stay the same or get finer.
      // If the older interval's granularity is finer than the newer interval's granularity,
      // that means we're getting coarser as we move toward present, which is invalid.
      if (olderGran.isFinerThan(newerGran)) {
        throw new SegmentGranularityTimelineValidationException(
            dataSource,
            olderInterval.getInterval(),
            olderGran,
            newerInterval.getInterval(),
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
   * Collects thresholds from all non-partitioning rules.
   */
  private List<DateTime> collectNonPartitioningThresholds(DateTime referenceTime)
  {
    return ruleProvider.streamAllRules()
        .filter(rule -> !(rule instanceof ReindexingPartitioningRule))
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

  // Legacy fields from DataSourceCompactionConfig that are not used by this template

  @Nullable
  @Override
  public Integer getMaxRowsPerSegment()
  {
    return 0;
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
