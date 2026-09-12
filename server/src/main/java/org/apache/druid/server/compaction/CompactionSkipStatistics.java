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
import com.google.common.base.Preconditions;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;

/**
 * Stats for the intervals skipped for a single {@link CompactionSkipReason} in a
 * compaction run. The {@link CompactionSkipReason.Category} is reported alongside
 * the reason so that a consumer can decide how to treat these intervals without
 * having to keep its own mapping of reasons to categories.
 */
public class CompactionSkipStatistics
{
  @JsonProperty
  private final CompactionSkipReason reason;

  /**
   * Read-only so that Jackson does not overwrite the value derived from
   * {@link #reason} with whatever the payload happened to contain.
   */
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private final CompactionSkipReason.Category category;
  @JsonProperty
  private final long bytes;
  @JsonProperty
  private final long segmentCount;
  @JsonProperty
  private final long intervalCount;

  /**
   * The {@code category} is always derived from the {@code reason} rather than
   * read from the payload, so that the two can never contradict each other. It is
   * still serialized so that consumers do not need their own mapping of reasons
   * to categories.
   */
  @JsonCreator
  public CompactionSkipStatistics(
      @JsonProperty("reason") @NotNull CompactionSkipReason reason,
      @JsonProperty("bytes") long bytes,
      @JsonProperty("segmentCount") long segmentCount,
      @JsonProperty("intervalCount") long intervalCount
  )
  {
    this.reason = Preconditions.checkNotNull(reason, "reason cannot be null");
    this.category = reason.getCategory();
    this.bytes = bytes;
    this.segmentCount = segmentCount;
    this.intervalCount = intervalCount;
  }

  public static CompactionSkipStatistics of(CompactionSkipReason reason, CompactionStatistics stats)
  {
    return new CompactionSkipStatistics(
        reason,
        stats.getTotalBytes(),
        stats.getNumSegments(),
        stats.getNumIntervals()
    );
  }

  public CompactionSkipReason getReason()
  {
    return reason;
  }

  public CompactionSkipReason.Category getCategory()
  {
    return category;
  }

  public long getBytes()
  {
    return bytes;
  }

  public long getSegmentCount()
  {
    return segmentCount;
  }

  public long getIntervalCount()
  {
    return intervalCount;
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
    CompactionSkipStatistics that = (CompactionSkipStatistics) o;
    return bytes == that.bytes
           && segmentCount == that.segmentCount
           && intervalCount == that.intervalCount
           && reason == that.reason;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(reason, bytes, segmentCount, intervalCount);
  }

  @Override
  public String toString()
  {
    return "CompactionSkipStatistics{" +
           "reason=" + reason +
           ", category=" + category +
           ", bytes=" + bytes +
           ", segmentCount=" + segmentCount +
           ", intervalCount=" + intervalCount +
           '}';
  }
}
