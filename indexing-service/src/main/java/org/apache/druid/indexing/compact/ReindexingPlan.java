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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Internal description of a cascading reindexing plan: the search intervals and, for each,
 * the rules that apply and the resolved compaction config. Produced once by {@link ReindexingPlanner}
 * and walked by both {@link CascadingReindexingTemplate#createCompactionJobs} (which emits jobs)
 * and {@link ReindexingTimelineView#fromPlan} (which formats for the API). Package-private —
 * this is not part of any public contract.
 */
final class ReindexingPlan
{
  private final String dataSource;
  private final DateTime referenceTime;
  @Nullable
  private final SkipOffsetResolution skipOffset;
  private final List<PlannedInterval> intervals;
  @Nullable
  private final PlanValidationError validationError;

  ReindexingPlan(
      String dataSource,
      DateTime referenceTime,
      @Nullable SkipOffsetResolution skipOffset,
      List<PlannedInterval> intervals,
      @Nullable PlanValidationError validationError
  )
  {
    this.dataSource = dataSource;
    this.referenceTime = referenceTime;
    this.skipOffset = skipOffset;
    this.intervals = Collections.unmodifiableList(intervals);
    this.validationError = validationError;
  }

  String getDataSource()
  {
    return dataSource;
  }

  DateTime getReferenceTime()
  {
    return referenceTime;
  }

  @Nullable
  SkipOffsetResolution getSkipOffset()
  {
    return skipOffset;
  }

  List<PlannedInterval> getIntervals()
  {
    return intervals;
  }

  @Nullable
  PlanValidationError getValidationError()
  {
    return validationError;
  }

  /**
   * How a search interval was resolved against data overlap and skip-offset boundaries.
   * Drives whether job creation emits a job for the interval and how the API view renders it.
   */
  enum IntervalDisposition
  {
    /** Interval is fully within the compactable range; emit a job for the original interval. */
    INCLUDED,
    /** Interval extended past a skip-offset or data boundary and was clipped; emit a job for the clipped range. */
    TRUNCATED,
    /** Interval lies entirely past the skip-offset boundary (no data eligible); no job. */
    SKIPPED_BEYOND_BOUNDARY,
    /** Interval has no overlap with the live segment timeline; no job. */
    SKIPPED_NO_DATA
  }

  static final class PlannedInterval
  {
    private final Interval interval;
    private final Interval originalInterval;
    private final IntervalDisposition disposition;
    private final ReindexingPartitioningRule sourceRule;
    private final boolean ruleSynthetic;
    private final Granularity segmentGranularity;
    private final int ruleCount;
    @Nullable
    private final InlineSchemaDataSourceCompactionConfig config;
    private final List<ReindexingRule> appliedRules;

    PlannedInterval(
        Interval interval,
        Interval originalInterval,
        IntervalDisposition disposition,
        ReindexingPartitioningRule sourceRule,
        boolean ruleSynthetic,
        Granularity segmentGranularity,
        int ruleCount,
        @Nullable InlineSchemaDataSourceCompactionConfig config,
        List<ReindexingRule> appliedRules
    )
    {
      this.interval = interval;
      this.originalInterval = originalInterval;
      this.disposition = disposition;
      this.sourceRule = sourceRule;
      this.ruleSynthetic = ruleSynthetic;
      this.segmentGranularity = segmentGranularity;
      this.ruleCount = ruleCount;
      this.config = config;
      this.appliedRules = Collections.unmodifiableList(appliedRules);
    }

    /** Interval after any skip-offset/data-boundary truncation. This is what a compaction job would target. */
    Interval getInterval()
    {
      return interval;
    }

    /** Interval as originally generated from the rule timeline, before any truncation. */
    Interval getOriginalInterval()
    {
      return originalInterval;
    }

    IntervalDisposition getDisposition()
    {
      return disposition;
    }

    ReindexingPartitioningRule getSourceRule()
    {
      return sourceRule;
    }

    boolean isRuleSynthetic()
    {
      return ruleSynthetic;
    }

    Granularity getSegmentGranularity()
    {
      return segmentGranularity;
    }

    int getRuleCount()
    {
      return ruleCount;
    }

    @Nullable
    InlineSchemaDataSourceCompactionConfig getConfig()
    {
      return config;
    }

    List<ReindexingRule> getAppliedRules()
    {
      return appliedRules;
    }

    /** True if a compaction job should be emitted for this interval (rules resolved and not skipped). */
    boolean isJobEligible()
    {
      return config != null
             && (disposition == IntervalDisposition.INCLUDED || disposition == IntervalDisposition.TRUNCATED);
    }
  }

  /**
   * The configured skip offset resolved against the live segment timeline. Present only when
   * a skip offset is configured on the template. Always fully resolved — there is no
   * "configured but not applied" state in the plan, because the planner requires a live timeline.
   */
  static final class SkipOffsetResolution
  {
    enum Type
    {
      FROM_NOW,
      FROM_LATEST
    }

    private final Type type;
    private final Period period;
    private final DateTime effectiveEndTime;

    SkipOffsetResolution(Type type, Period period, DateTime effectiveEndTime)
    {
      this.type = type;
      this.period = period;
      this.effectiveEndTime = effectiveEndTime;
    }

    Type getType()
    {
      return type;
    }

    Period getPeriod()
    {
      return period;
    }

    DateTime getEffectiveEndTime()
    {
      return effectiveEndTime;
    }
  }

  /**
   * Captured when planning fails before any intervals can be produced (e.g., misconfigured
   * granularity ordering, no rules). The plan still carries the error so callers can surface
   * it as part of the response rather than as an exception.
   */
  static final class PlanValidationError
  {
    enum Type
    {
      INVALID_GRANULARITY_TIMELINE,
      VALIDATION_ERROR
    }

    private final Type type;
    private final String message;
    @Nullable
    private final Interval olderInterval;
    @Nullable
    private final Granularity olderGranularity;
    @Nullable
    private final Interval newerInterval;
    @Nullable
    private final Granularity newerGranularity;

    PlanValidationError(Type type, String message)
    {
      this(type, message, null, null, null, null);
    }

    PlanValidationError(
        Type type,
        String message,
        @Nullable Interval olderInterval,
        @Nullable Granularity olderGranularity,
        @Nullable Interval newerInterval,
        @Nullable Granularity newerGranularity
    )
    {
      this.type = type;
      this.message = message;
      this.olderInterval = olderInterval;
      this.olderGranularity = olderGranularity;
      this.newerInterval = newerInterval;
      this.newerGranularity = newerGranularity;
    }

    Type getType()
    {
      return type;
    }

    String getMessage()
    {
      return message;
    }

    @Nullable
    Interval getOlderInterval()
    {
      return olderInterval;
    }

    @Nullable
    Granularity getOlderGranularity()
    {
      return olderGranularity;
    }

    @Nullable
    Interval getNewerInterval()
    {
      return newerInterval;
    }

    @Nullable
    Granularity getNewerGranularity()
    {
      return newerGranularity;
    }
  }
}
