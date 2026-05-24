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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.server.compaction.ReindexingDataSchemaRule;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingIndexSpecRule;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Public API DTO describing the timeline of search intervals and their associated reindexing
 * configurations for a cascading reindexing supervisor. Surfaced through the
 * {@code /supervisor/{id}/reindexingTimeline} endpoint so operators (and the web console)
 * can visualize how rules are applied across time.
 *
 * <p>Always produced via {@link #fromPlan(ReindexingPlan)} so the API view is a pure projection
 * of the same plan that drives job creation — the two cannot drift.
 */
public class ReindexingTimelineView
{
  private final String dataSource;
  private final DateTime referenceTime;
  @Nullable
  private final SkipOffsetInfo skipOffset;
  private final List<IntervalConfig> intervals;
  @Nullable
  private final ValidationError validationError;

  @JsonCreator
  public ReindexingTimelineView(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("referenceTime") DateTime referenceTime,
      @JsonProperty("skipOffset") @Nullable SkipOffsetInfo skipOffset,
      @JsonProperty("intervals") List<IntervalConfig> intervals,
      @JsonProperty("validationError") @Nullable ValidationError validationError
  )
  {
    this.dataSource = dataSource;
    this.referenceTime = referenceTime;
    this.skipOffset = skipOffset;
    this.intervals = Collections.unmodifiableList(intervals);
    this.validationError = validationError;
  }

  /**
   * Projects a {@link ReindexingPlan} into the API view. Intervals that have rules applied
   * are emitted with their resolved config; intervals fully eclipsed by a skip-offset boundary
   * are emitted with zero rules so the UI can render the skipped span. Intervals with no data
   * overlap (a job-path concern) are not surfaced — operators see only ranges that would
   * actually be compacted or that are explicitly skipped.
   */
  static ReindexingTimelineView fromPlan(ReindexingPlan plan)
  {
    final List<IntervalConfig> intervalConfigs = new ArrayList<>();
    for (ReindexingPlan.PlannedInterval planned : plan.getIntervals()) {
      switch (planned.getDisposition()) {
        case INCLUDED:
        case TRUNCATED:
          if (planned.getRuleCount() > 0) {
            intervalConfigs.add(new IntervalConfig(
                planned.getInterval(),
                planned.getRuleCount(),
                planned.getConfig(),
                planned.getAppliedRules()
            ));
          }
          break;
        case SKIPPED_BEYOND_BOUNDARY:
          intervalConfigs.add(new IntervalConfig(
              planned.getInterval(),
              0,
              null,
              Collections.emptyList()
          ));
          break;
        case SKIPPED_NO_DATA:
          // Operator-visible timeline only shows ranges that would be compacted or are
          // explicitly skipped by configuration. Ranges with no underlying data are omitted.
          break;
      }
    }

    return new ReindexingTimelineView(
        plan.getDataSource(),
        plan.getReferenceTime(),
        SkipOffsetInfo.fromPlan(plan.getSkipOffset()),
        intervalConfigs,
        ValidationError.fromPlan(plan.getValidationError())
    );
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getReferenceTime()
  {
    return referenceTime;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public SkipOffsetInfo getSkipOffset()
  {
    return skipOffset;
  }

  @JsonProperty
  public List<IntervalConfig> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public ValidationError getValidationError()
  {
    return validationError;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReindexingTimelineView that = (ReindexingTimelineView) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(referenceTime, that.referenceTime)
           && Objects.equals(skipOffset, that.skipOffset)
           && Objects.equals(intervals, that.intervals)
           && Objects.equals(validationError, that.validationError);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, referenceTime, skipOffset, intervals, validationError);
  }

  /**
   * Describes a granularity-ordering or configuration error encountered while building the timeline.
   * Surfaced in the response body rather than as a 5xx so clients can render meaningful messages.
   */
  public static class ValidationError
  {
    private final String errorType;
    private final String message;
    @Nullable
    private final String olderInterval;
    @Nullable
    private final String olderGranularity;
    @Nullable
    private final String newerInterval;
    @Nullable
    private final String newerGranularity;

    @JsonCreator
    public ValidationError(
        @JsonProperty("errorType") String errorType,
        @JsonProperty("message") String message,
        @JsonProperty("olderInterval") @Nullable String olderInterval,
        @JsonProperty("olderGranularity") @Nullable String olderGranularity,
        @JsonProperty("newerInterval") @Nullable String newerInterval,
        @JsonProperty("newerGranularity") @Nullable String newerGranularity
    )
    {
      this.errorType = errorType;
      this.message = message;
      this.olderInterval = olderInterval;
      this.olderGranularity = olderGranularity;
      this.newerInterval = newerInterval;
      this.newerGranularity = newerGranularity;
    }

    @Nullable
    static ValidationError fromPlan(@Nullable ReindexingPlan.PlanValidationError error)
    {
      if (error == null) {
        return null;
      }
      return new ValidationError(
          error.getType().name(),
          error.getMessage(),
          error.getOlderInterval() == null ? null : error.getOlderInterval().toString(),
          error.getOlderGranularity() == null ? null : error.getOlderGranularity().toString(),
          error.getNewerInterval() == null ? null : error.getNewerInterval().toString(),
          error.getNewerGranularity() == null ? null : error.getNewerGranularity().toString()
      );
    }

    @JsonProperty
    public String getErrorType()
    {
      return errorType;
    }

    @JsonProperty
    public String getMessage()
    {
      return message;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getOlderInterval()
    {
      return olderInterval;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getOlderGranularity()
    {
      return olderGranularity;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getNewerInterval()
    {
      return newerInterval;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public String getNewerGranularity()
    {
      return newerGranularity;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ValidationError that = (ValidationError) o;
      return Objects.equals(errorType, that.errorType)
             && Objects.equals(message, that.message)
             && Objects.equals(olderInterval, that.olderInterval)
             && Objects.equals(olderGranularity, that.olderGranularity)
             && Objects.equals(newerInterval, that.newerInterval)
             && Objects.equals(newerGranularity, that.newerGranularity);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(errorType, message, olderInterval, olderGranularity, newerInterval, newerGranularity);
    }
  }

  /**
   * Describes the configured skip offset that was applied to this view. Only present in the
   * response when the configured skip offset was actually resolved against the live segment
   * timeline — operators don't see a "configured but not applied" state because the scheduler
   * short-circuits to an empty timeline when no segments exist.
   */
  public static class SkipOffsetInfo
  {
    private final String type;
    private final Period period;
    private final DateTime effectiveEndTime;

    @JsonCreator
    public SkipOffsetInfo(
        @JsonProperty("type") String type,
        @JsonProperty("period") Period period,
        @JsonProperty("effectiveEndTime") DateTime effectiveEndTime
    )
    {
      this.type = type;
      this.period = period;
      this.effectiveEndTime = effectiveEndTime;
    }

    @Nullable
    static SkipOffsetInfo fromPlan(@Nullable ReindexingPlan.SkipOffsetResolution resolution)
    {
      if (resolution == null) {
        return null;
      }
      final String typeName = resolution.getType() == ReindexingPlan.SkipOffsetResolution.Type.FROM_NOW
                              ? "skipOffsetFromNow"
                              : "skipOffsetFromLatest";
      return new SkipOffsetInfo(typeName, resolution.getPeriod(), resolution.getEffectiveEndTime());
    }

    @JsonProperty
    public String getType()
    {
      return type;
    }

    @JsonProperty
    public Period getPeriod()
    {
      return period;
    }

    @JsonProperty
    public DateTime getEffectiveEndTime()
    {
      return effectiveEndTime;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SkipOffsetInfo that = (SkipOffsetInfo) o;
      return Objects.equals(type, that.type)
             && Objects.equals(period, that.period)
             && Objects.equals(effectiveEndTime, that.effectiveEndTime);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(type, period, effectiveEndTime);
    }
  }

  /**
   * A single search interval and its resolved reindexing configuration. {@code ruleCount==0}
   * with a null {@code config} indicates a span that was eclipsed by the skip-offset boundary
   * (no compaction would occur in that span).
   */
  public static class IntervalConfig
  {
    private final Interval interval;
    private final int ruleCount;
    @Nullable
    private final DataSourceCompactionConfig config;
    private final List<ReindexingRule> appliedRules;

    @JsonCreator
    public IntervalConfig(
        @JsonProperty("interval") Interval interval,
        @JsonProperty("ruleCount") int ruleCount,
        @JsonProperty("config") @Nullable DataSourceCompactionConfig config,
        @JsonProperty("appliedRules") List<ReindexingRule> appliedRules
    )
    {
      this.interval = interval;
      this.ruleCount = ruleCount;
      this.config = config;
      this.appliedRules = Collections.unmodifiableList(appliedRules);
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }

    @JsonProperty
    public int getRuleCount()
    {
      return ruleCount;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public DataSourceCompactionConfig getConfig()
    {
      return config;
    }

    @JsonProperty
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ReindexingDataSchemaRule.class, name = "dataSchema"),
        @JsonSubTypes.Type(value = ReindexingDeletionRule.class, name = "deletion"),
        @JsonSubTypes.Type(value = ReindexingPartitioningRule.class, name = "partitioning"),
        @JsonSubTypes.Type(value = ReindexingIndexSpecRule.class, name = "indexSpec"),
    })
    public List<ReindexingRule> getAppliedRules()
    {
      return appliedRules;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IntervalConfig that = (IntervalConfig) o;
      return ruleCount == that.ruleCount
             && Objects.equals(interval, that.interval)
             && Objects.equals(config, that.config)
             && Objects.equals(appliedRules, that.appliedRules);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(interval, ruleCount, config, appliedRules);
    }
  }
}
