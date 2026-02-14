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
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents the timeline of search intervals and their associated reindexing configurations
 * for a cascading reindexing supervisor. This view helps operators understand how different
 * rules are applied across time intervals.
 */
public class ReindexingTimelineView
{
  private final String dataSource;
  private final DateTime referenceTime;
  private final SkipOffsetInfo skipOffset;
  private final List<IntervalConfig> intervals;
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
    this.intervals = intervals;
    this.validationError = validationError;
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
    return Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(referenceTime, that.referenceTime) &&
           Objects.equals(skipOffset, that.skipOffset) &&
           Objects.equals(intervals, that.intervals) &&
           Objects.equals(validationError, that.validationError);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, referenceTime, skipOffset, intervals, validationError);
  }

  /**
   * Information about a validation error that occurred while building the timeline.
   */
  public static class ValidationError
  {
    private final String errorType;
    private final String message;
    private final String olderInterval;
    private final String olderGranularity;
    private final String newerInterval;
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
      return Objects.equals(errorType, that.errorType) &&
             Objects.equals(message, that.message) &&
             Objects.equals(olderInterval, that.olderInterval) &&
             Objects.equals(olderGranularity, that.olderGranularity) &&
             Objects.equals(newerInterval, that.newerInterval) &&
             Objects.equals(newerGranularity, that.newerGranularity);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(errorType, message, olderInterval, olderGranularity, newerInterval, newerGranularity);
    }
  }

  /**
   * Information about skip offsets and whether they were applied.
   */
  public static class SkipOffsetInfo
  {
    private final AppliedSkipOffset applied;
    private final NotAppliedSkipOffset notApplied;

    @JsonCreator
    public SkipOffsetInfo(
        @JsonProperty("applied") @Nullable AppliedSkipOffset applied,
        @JsonProperty("notApplied") @Nullable NotAppliedSkipOffset notApplied
    )
    {
      this.applied = applied;
      this.notApplied = notApplied;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public AppliedSkipOffset getApplied()
    {
      return applied;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public NotAppliedSkipOffset getNotApplied()
    {
      return notApplied;
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
      return Objects.equals(applied, that.applied) &&
             Objects.equals(notApplied, that.notApplied);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(applied, notApplied);
    }
  }

  /**
   * Information about a skip offset that was applied.
   */
  public static class AppliedSkipOffset
  {
    private final String type;
    private final Period period;
    private final DateTime effectiveEndTime;

    @JsonCreator
    public AppliedSkipOffset(
        @JsonProperty("type") String type,
        @JsonProperty("period") Period period,
        @JsonProperty("effectiveEndTime") DateTime effectiveEndTime
    )
    {
      this.type = type;
      this.period = period;
      this.effectiveEndTime = effectiveEndTime;
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
      AppliedSkipOffset that = (AppliedSkipOffset) o;
      return Objects.equals(type, that.type) &&
             Objects.equals(period, that.period) &&
             Objects.equals(effectiveEndTime, that.effectiveEndTime);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(type, period, effectiveEndTime);
    }
  }

  /**
   * Information about a skip offset that was not applied.
   */
  public static class NotAppliedSkipOffset
  {
    private final String type;
    private final Period period;
    private final String reason;

    @JsonCreator
    public NotAppliedSkipOffset(
        @JsonProperty("type") String type,
        @JsonProperty("period") Period period,
        @JsonProperty("reason") String reason
    )
    {
      this.type = type;
      this.period = period;
      this.reason = reason;
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
    public String getReason()
    {
      return reason;
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
      NotAppliedSkipOffset that = (NotAppliedSkipOffset) o;
      return Objects.equals(type, that.type) &&
             Objects.equals(period, that.period) &&
             Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(type, period, reason);
    }
  }

  /**
   * Represents a search interval and its associated reindexing configuration.
   */
  public static class IntervalConfig
  {
    private final Interval interval;
    private final int ruleCount;
    private final DataSourceCompactionConfig config;
    private final List<ReindexingRule> appliedRules;

    @JsonCreator
    public IntervalConfig(
        @JsonProperty("interval") Interval interval,
        @JsonProperty("ruleCount") int ruleCount,
        @JsonProperty("config") DataSourceCompactionConfig config,
        @JsonProperty("appliedRules") List<ReindexingRule> appliedRules
    )
    {
      this.interval = interval;
      this.ruleCount = ruleCount;
      this.config = config;
      this.appliedRules = appliedRules;
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
    public DataSourceCompactionConfig getConfig()
    {
      return config;
    }

    @JsonProperty
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
      return ruleCount == that.ruleCount &&
             Objects.equals(interval, that.interval) &&
             Objects.equals(config, that.config) &&
             Objects.equals(appliedRules, that.appliedRules);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(interval, config, ruleCount, appliedRules);
    }
  }
}
