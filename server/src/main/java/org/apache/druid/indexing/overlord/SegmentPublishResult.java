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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result of a segment publish operation.
 */
public class SegmentPublishResult
{
  private final Set<DataSegment> segments;
  private final boolean success;
  private final boolean retryable;
  private final String errorMsg;
  private final List<PendingSegmentRecord> upgradedPendingSegments;

  public static SegmentPublishResult ok(Set<DataSegment> segments)
  {
    return new SegmentPublishResult(segments, true, false, null);
  }

  public static SegmentPublishResult ok(Set<DataSegment> segments, List<PendingSegmentRecord> upgradedPendingSegments)
  {
    return new SegmentPublishResult(segments, true, false, null, upgradedPendingSegments);
  }

  public static SegmentPublishResult fail(String errorMsg, Object... args)
  {
    return new SegmentPublishResult(Set.of(), false, false, StringUtils.format(errorMsg, args), null);
  }

  public static SegmentPublishResult retryableFailure(String errorMsg, Object... args)
  {
    return new SegmentPublishResult(Set.of(), false, true, StringUtils.format(errorMsg, args), null);
  }

  @JsonCreator
  private SegmentPublishResult(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("success") boolean success,
      @JsonProperty("retryable") boolean retryable,
      @JsonProperty("errorMsg") @Nullable String errorMsg
  )
  {
    this(segments, success, retryable, errorMsg, null);
  }

  private SegmentPublishResult(
      Set<DataSegment> segments,
      boolean success,
      boolean retryable,
      @Nullable String errorMsg,
      List<PendingSegmentRecord> upgradedPendingSegments
  )
  {
    this.segments = Preconditions.checkNotNull(segments, "segments");
    this.success = success;
    this.errorMsg = errorMsg;
    this.retryable = retryable;
    this.upgradedPendingSegments = upgradedPendingSegments;

    if (!success) {
      Preconditions.checkArgument(segments.isEmpty(), "segments must be empty for unsuccessful publishes");
      Preconditions.checkArgument(
          CollectionUtils.isNullOrEmpty(upgradedPendingSegments),
          "upgraded pending segments must be null or empty for unsuccessful publishes"
      );
    }
  }

  /**
   * Set of segments published successfully.
   *
   * @return Empty set if the publish operation failed or if all the segments had
   * already been published by a different transaction.
   */
  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public boolean isSuccess()
  {
    return success;
  }

  @JsonProperty
  @Nullable
  public String getErrorMsg()
  {
    return errorMsg;
  }

  @JsonProperty
  public boolean isRetryable()
  {
    return retryable;
  }

  @Nullable
  public List<PendingSegmentRecord> getUpgradedPendingSegments()
  {
    return upgradedPendingSegments;
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
    SegmentPublishResult that = (SegmentPublishResult) o;
    return success == that.success &&
           retryable == that.retryable &&
           Objects.equals(segments, that.segments) &&
           Objects.equals(errorMsg, that.errorMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments, success, errorMsg, retryable);
  }

  @Override
  public String toString()
  {
    return "SegmentPublishResult{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", success=" + success +
           ", retryable=" + retryable +
           ", errorMsg='" + errorMsg + '\'' +
           '}';
  }
}
