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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result of an operation that attempts to publish segments. Indicates the set of segments actually published
 * and whether the transaction was a success.
 * <p>
 * If "success" is false then the segments set will be empty.
 * <p>
 * It's possible for the segments set to be empty even if "success" is true, since the segments set only
 * includes segments actually published as part of the transaction. The requested segments could have been
 * published by a different transaction (e.g. in the case of replica sets) and this one would still succeed.
 */
public class SegmentPublishResult
{
  private final Set<DataSegment> segments;
  private final boolean success;
  @Nullable
  private final String errorMsg;
  @Nullable
  private final List<DataSegment> upgradedAppendSegments;
  @Nullable
  private final List<PendingSegmentRecord> upgradedPendingSegments;

  public static SegmentPublishResult ok(Set<DataSegment> segments)
  {
    return new SegmentPublishResult(segments, true, null);
  }

  public static SegmentPublishResult ok(
      Set<DataSegment> segments,
      List<DataSegment> upgradedAppendSegments,
      List<PendingSegmentRecord> upgradedPendingSegments
  )
  {
    return new SegmentPublishResult(segments, true, null, upgradedAppendSegments, upgradedPendingSegments);
  }

  public static SegmentPublishResult fail(String errorMsg)
  {
    return new SegmentPublishResult(ImmutableSet.of(), false, errorMsg);
  }

  @JsonCreator
  private SegmentPublishResult(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("success") boolean success,
      @JsonProperty("errorMsg") @Nullable String errorMsg
  )
  {
    this(segments, success, errorMsg, null, null);
  }

  private SegmentPublishResult(
      Set<DataSegment> segments,
      boolean success,
      @Nullable String errorMsg,
      @Nullable List<DataSegment> upgradedAppendSegments,
      @Nullable List<PendingSegmentRecord> upgradedPendingSegments
  )
  {
    this.segments = Preconditions.checkNotNull(segments, "segments");
    this.success = success;
    this.errorMsg = errorMsg;
    this.upgradedAppendSegments = upgradedAppendSegments;
    this.upgradedPendingSegments = upgradedPendingSegments;

    if (!success) {
      Preconditions.checkArgument(segments.isEmpty(), "Segments must be empty for unsuccessful publish.");
      Preconditions.checkArgument(
          CollectionUtils.isNullOrEmpty(upgradedPendingSegments),
          "Upgraded pending segments must be null or empty for unsuccessful publish."
      );
      Preconditions.checkArgument(
          CollectionUtils.isNullOrEmpty(upgradedAppendSegments),
          "Upgraded append segments must be null or empty for unsuccessful publish."
      );
    }
  }

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

  @Nullable
  @JsonProperty
  public String getErrorMsg()
  {
    return errorMsg;
  }

  @Nullable
  public List<PendingSegmentRecord> getUpgradedPendingSegments()
  {
    return upgradedPendingSegments;
  }

  @Nullable
  public List<DataSegment> getUpgradedAppendSegments()
  {
    return upgradedAppendSegments;
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
           Objects.equals(segments, that.segments) &&
           Objects.equals(errorMsg, that.errorMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments, success, errorMsg);
  }

  @Override
  public String toString()
  {
    return "SegmentPublishResult{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", success=" + success +
           ", errorMsg='" + errorMsg + '\'' +
           '}';
  }
}
