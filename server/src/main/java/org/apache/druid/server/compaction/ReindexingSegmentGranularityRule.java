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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a segment granularity for reindexing tasks to configure.
 * <p>
 * This rule controls how time-series data is bucketed into segments during reindexing. For example, changing from
 * 15-minute segments to hourly segments reduces segment count. There is a strict allow list of supported granularities
 * to prevent misconfiguration.
 * <p>
 * This is a non-additive rule. Multiple segment granularity rules cannot be applied to the same segment.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *     "id": "daily-30d",
 *     "olderThan": "P30D",
 *     "segmentGranularity": "DAY"
 *     "description": "Compact to daily segments for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingSegmentGranularityRule extends AbstractReindexingRule
{
  private static final List<Granularity> SUPPORTED_SEGMENT_GRANULARITIES = List.of(
      Granularities.MINUTE,
      Granularities.FIFTEEN_MINUTE,
      Granularities.HOUR,
      Granularities.DAY,
      Granularities.MONTH,
      Granularities.QUARTER,
      Granularities.YEAR
  );

  private final Granularity segmentGranularity;

  @JsonCreator
  public ReindexingSegmentGranularityRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("segmentGranularity") @Nonnull Granularity segmentGranularity
  )
  {
    super(id, description, olderThan);
    InvalidInput.conditionalException(
        SUPPORTED_SEGMENT_GRANULARITIES.contains(segmentGranularity),
        "Unsupported segment granularity [%s]. Supported values are: MINUTE, FIFTEEN_MINUTE, HOUR, DAY, MONTH, QUARTER, YEAR",
        segmentGranularity
    );
    this.segmentGranularity = segmentGranularity;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
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
    ReindexingSegmentGranularityRule that = (ReindexingSegmentGranularityRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(segmentGranularity, that.segmentGranularity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        segmentGranularity
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingSegmentGranularityRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", segmentGranularity=" + segmentGranularity
           + '}';
  }

}
