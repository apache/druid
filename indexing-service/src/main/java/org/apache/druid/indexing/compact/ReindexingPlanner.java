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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.compaction.IntervalPartitioningInfo;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds a {@link ReindexingPlan} from a {@link CascadingReindexingTemplate} for a given reference
 * time and the live segment timeline. The plan is the single source of truth for both
 * compaction-job creation and the API timeline view, ensuring the two cannot drift.
 *
 * <p>The data range bounds the planned intervals (no-overlap intervals become
 * {@link ReindexingPlan.IntervalDisposition#SKIPPED_NO_DATA}), {@code skipOffsetFromLatest} is
 * anchored on the live data, and intervals that extend past the skip boundary are truncated or
 * marked {@link ReindexingPlan.IntervalDisposition#SKIPPED_BEYOND_BOUNDARY}.
 *
 * <p>Validation failures (misconfigured granularity ordering, no rules) are caught and surfaced
 * on the returned plan as a {@link ReindexingPlan.PlanValidationError} so HTTP callers can render
 * them in the response rather than as 500s.
 */
final class ReindexingPlanner
{
  private static final Logger LOG = new Logger(ReindexingPlanner.class);

  private final CascadingReindexingTemplate template;

  ReindexingPlanner(CascadingReindexingTemplate template)
  {
    this.template = template;
  }

  /**
   * Build a plan for the given reference time. The plan reflects exactly what
   * {@link CascadingReindexingTemplate#createCompactionJobs} would do given the same
   * {@code timeline} and reference time.
   *
   * @param timeline live segment timeline for the datasource; must be non-null and non-empty —
   *                 callers are responsible for short-circuiting when no data exists rather than
   *                 calling this with a fabricated empty timeline.
   */
  ReindexingPlan plan(DateTime referenceTime, SegmentTimeline timeline)
  {
    final String dataSource = template.getDataSource();

    if (!template.getReindexingRuleProvider().isReady()) {
      LOG.info(
          "Rule provider [%s] is not ready for dataSource[%s], returning empty plan",
          template.getReindexingRuleProvider().getType(),
          dataSource
      );
      return new ReindexingPlan(dataSource, referenceTime, null, Collections.emptyList(), null);
    }

    final List<IntervalPartitioningInfo> searchIntervals;
    try {
      searchIntervals = template.generateAlignedSearchIntervals(referenceTime);
    }
    catch (SegmentGranularityTimelineValidationException e) {
      LOG.warn(e, "Granularity timeline validation failed for dataSource[%s]", dataSource);
      return new ReindexingPlan(
          dataSource,
          referenceTime,
          null,
          Collections.emptyList(),
          new ReindexingPlan.PlanValidationError(
              ReindexingPlan.PlanValidationError.Type.INVALID_GRANULARITY_TIMELINE,
              e.getMessage(),
              e.getOlderInterval(),
              e.getOlderGranularity(),
              e.getNewerInterval(),
              e.getNewerGranularity()
          )
      );
    }
    catch (IAE e) {
      LOG.warn(e, "Validation failed while planning timeline for dataSource[%s]", dataSource);
      return new ReindexingPlan(
          dataSource,
          referenceTime,
          null,
          Collections.emptyList(),
          new ReindexingPlan.PlanValidationError(
              ReindexingPlan.PlanValidationError.Type.VALIDATION_ERROR,
              e.getMessage()
          )
      );
    }

    if (searchIntervals.isEmpty()) {
      LOG.debug("No search intervals generated for dataSource[%s]", dataSource);
      return new ReindexingPlan(dataSource, referenceTime, null, Collections.emptyList(), null);
    }

    final ReindexingPlan.SkipOffsetResolution skipOffsetResolution = buildSkipOffsetResolution(timeline, referenceTime);
    final Interval dataRangeWithSkipOffset = computeDataRangeWithSkipOffset(timeline, referenceTime);
    if (dataRangeWithSkipOffset == null) {
      LOG.debug("All data for dataSource[%s] is within skip offsets; no intervals will be planned", dataSource);
      return new ReindexingPlan(dataSource, referenceTime, skipOffsetResolution, Collections.emptyList(), null);
    }

    final List<ReindexingPlan.PlannedInterval> planned = new ArrayList<>(searchIntervals.size());
    for (int i = 0; i < searchIntervals.size(); i++) {
      final IntervalPartitioningInfo originalInfo = searchIntervals.get(i);
      final Interval originalInterval = originalInfo.getInterval();

      if (!originalInterval.overlaps(dataRangeWithSkipOffset)) {
        planned.add(noRuleEntry(originalInterval, originalInterval, ReindexingPlan.IntervalDisposition.SKIPPED_NO_DATA, originalInfo));
        continue;
      }

      // When a skip offset is configured, intervals extending past the adjusted data-range end
      // must be truncated (or skipped entirely if the truncation eats the whole interval).
      final DateTime truncationBoundary =
          (template.getSkipOffsetFromNowOrNull() != null || template.getSkipOffsetFromLatestOrNull() != null)
          ? dataRangeWithSkipOffset.getEnd()
          : null;

      Interval effectiveInterval = originalInterval;
      ReindexingPlan.IntervalDisposition disposition = ReindexingPlan.IntervalDisposition.INCLUDED;

      if (truncationBoundary != null && originalInterval.getEnd().isAfter(truncationBoundary)) {
        final DateTime alignedEnd = originalInfo.getGranularity().bucketStart(truncationBoundary);
        if (!alignedEnd.isAfter(originalInterval.getStart())) {
          planned.add(noRuleEntry(originalInterval, originalInterval, ReindexingPlan.IntervalDisposition.SKIPPED_BEYOND_BOUNDARY, originalInfo));
          continue;
        }
        effectiveInterval = new Interval(originalInterval.getStart(), alignedEnd);
        disposition = ReindexingPlan.IntervalDisposition.TRUNCATED;
        // Replace the entry so the synthetic-timeline lookup in ReindexingConfigBuilder matches the truncated interval.
        searchIntervals.set(
            i,
            new IntervalPartitioningInfo(effectiveInterval, originalInfo.getSourceRule(), originalInfo.isRuleSynthetic())
        );
      }

      final InlineSchemaDataSourceCompactionConfig.Builder builder = template.createBaseConfigBuilder();
      final ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
          template.getReindexingRuleProvider(),
          effectiveInterval,
          referenceTime,
          searchIntervals,
          template.getTuningConfig()
      );
      final ReindexingConfigBuilder.BuildResult buildResult = configBuilder.applyToWithDetails(builder);

      if (buildResult.getRuleCount() > 0) {
        planned.add(new ReindexingPlan.PlannedInterval(
            effectiveInterval,
            originalInterval,
            disposition,
            originalInfo.getSourceRule(),
            originalInfo.isRuleSynthetic(),
            originalInfo.getGranularity(),
            buildResult.getRuleCount(),
            builder.build(),
            buildResult.getAppliedRules()
        ));
      } else {
        // Interval is geometrically valid but no rules apply to it. We retain INCLUDED/TRUNCATED disposition
        // (the geometry didn't skip it) but mark config null so it won't generate a job and the view can
        // choose to hide it.
        planned.add(new ReindexingPlan.PlannedInterval(
            effectiveInterval,
            originalInterval,
            disposition,
            originalInfo.getSourceRule(),
            originalInfo.isRuleSynthetic(),
            originalInfo.getGranularity(),
            0,
            null,
            Collections.emptyList()
        ));
      }
    }

    return new ReindexingPlan(dataSource, referenceTime, skipOffsetResolution, planned, null);
  }

  /**
   * Computes the data-range interval clipped by skip offsets. Returns null when the entire data
   * range falls within skip offsets and nothing remains to plan.
   */
  @Nullable
  private Interval computeDataRangeWithSkipOffset(SegmentTimeline timeline, DateTime referenceTime)
  {
    final Interval dataBounds = new Interval(
        timeline.first().getInterval().getStart(),
        timeline.last().getInterval().getEnd()
    );

    final Period skipFromNow = template.getSkipOffsetFromNowOrNull();
    final Period skipFromLatest = template.getSkipOffsetFromLatestOrNull();

    DateTime end = dataBounds.getEnd();
    if (skipFromNow != null) {
      end = referenceTime.minus(skipFromNow);
    } else if (skipFromLatest != null) {
      end = end.minus(skipFromLatest);
    }
    if (end.isBefore(dataBounds.getStart())) {
      return null;
    }
    return new Interval(dataBounds.getStart(), end);
  }

  /**
   * Builds the skip-offset resolution stamp for the plan. Returns null when no skip offset is configured.
   */
  @Nullable
  private ReindexingPlan.SkipOffsetResolution buildSkipOffsetResolution(SegmentTimeline timeline, DateTime referenceTime)
  {
    final Period skipFromNow = template.getSkipOffsetFromNowOrNull();
    if (skipFromNow != null) {
      return new ReindexingPlan.SkipOffsetResolution(
          ReindexingPlan.SkipOffsetResolution.Type.FROM_NOW,
          skipFromNow,
          referenceTime.minus(skipFromNow)
      );
    }
    final Period skipFromLatest = template.getSkipOffsetFromLatestOrNull();
    if (skipFromLatest != null) {
      return new ReindexingPlan.SkipOffsetResolution(
          ReindexingPlan.SkipOffsetResolution.Type.FROM_LATEST,
          skipFromLatest,
          timeline.last().getInterval().getEnd().minus(skipFromLatest)
      );
    }
    return null;
  }

  private static ReindexingPlan.PlannedInterval noRuleEntry(
      Interval interval,
      Interval originalInterval,
      ReindexingPlan.IntervalDisposition disposition,
      IntervalPartitioningInfo info
  )
  {
    return new ReindexingPlan.PlannedInterval(
        interval,
        originalInterval,
        disposition,
        info.getSourceRule(),
        info.isRuleSynthetic(),
        info.getGranularity(),
        0,
        null,
        Collections.emptyList()
    );
  }
}
