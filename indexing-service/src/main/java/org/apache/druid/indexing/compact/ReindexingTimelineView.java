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
import org.apache.druid.server.compaction.ReindexingIOConfigRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.compaction.ReindexingTuningConfigRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Collections;
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
    this.intervals = Collections.unmodifiableList(intervals);
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
   * Information about the skip offset configuration and whether it was applied.
   */
  public static class SkipOffsetInfo
  {
    private final String type;
    private final Period period;
    private final boolean isApplied;
    private final DateTime effectiveEndTime;
    private final String reason;

    @JsonCreator
    public SkipOffsetInfo(
        @JsonProperty("type") String type,
        @JsonProperty("period") Period period,
        @JsonProperty("isApplied") boolean isApplied,
        @JsonProperty("effectiveEndTime") @Nullable DateTime effectiveEndTime,
        @JsonProperty("reason") @Nullable String reason
    )
    {
      this.type = type;
      this.period = period;
      this.isApplied = isApplied;
      this.effectiveEndTime = effectiveEndTime;
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

    @JsonProperty("isApplied")
    public boolean isApplied()
    {
      return isApplied;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    public DateTime getEffectiveEndTime()
    {
      return effectiveEndTime;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
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
      SkipOffsetInfo that = (SkipOffsetInfo) o;
      return isApplied == that.isApplied &&
             Objects.equals(type, that.type) &&
             Objects.equals(period, that.period) &&
             Objects.equals(effectiveEndTime, that.effectiveEndTime) &&
             Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(type, period, isApplied, effectiveEndTime, reason);
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
    public DataSourceCompactionConfig getConfig()
    {
      return config;
    }

    @JsonProperty
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ReindexingDataSchemaRule.class, name = "dataSchema"),
        @JsonSubTypes.Type(value = ReindexingDeletionRule.class, name = "deletion"),
        @JsonSubTypes.Type(value = ReindexingSegmentGranularityRule.class, name = "segmentGranularity"),
        @JsonSubTypes.Type(value = ReindexingTuningConfigRule.class, name = "tuningConfig"),
        @JsonSubTypes.Type(value = ReindexingIOConfigRule.class, name = "ioConfig")
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
