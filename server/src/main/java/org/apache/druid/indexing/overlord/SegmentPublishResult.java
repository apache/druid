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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Result of an operation that attempts to publish segments. Indicates the set of segments actually published
 * and whether or not the transaction was a success.
 *
 * If "success" is false then the segments set will be empty.
 *
 * It's possible for the segments set to be empty even if "success" is true, since the segments set only
 * includes segments actually published as part of the transaction. The requested segments could have been
 * published by a different transaction (e.g. in the case of replica sets) and this one would still succeed.
 */
public class SegmentPublishResult
{
  private final Set<DataSegment> segments;
  private final List<PendingSegmentUpgradeRecord> pendingSegmentUpgrades;
  private final boolean success;
  @Nullable
  private final String errorMsg;

  public static SegmentPublishResult ok(Set<DataSegment> segments, List<PendingSegmentUpgradeRecord> pendingSegmentUpgrades)
  {
    return new SegmentPublishResult(segments, pendingSegmentUpgrades, true, null);
  }

  public static SegmentPublishResult ok(Set<DataSegment> segments)
  {
    return new SegmentPublishResult(segments, ImmutableList.of(), true, null);
  }

  public static SegmentPublishResult fail(String errorMsg)
  {
    return new SegmentPublishResult(ImmutableSet.of(), ImmutableList.of(), false, errorMsg);
  }

  @JsonCreator
  private SegmentPublishResult(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("pendingSegmentUpgrades") @Nullable List<PendingSegmentUpgradeRecord> pendingSegmentUpgrades,
      @JsonProperty("success") boolean success,
      @JsonProperty("errorMsg") @Nullable String errorMsg
  )
  {
    this.segments = Preconditions.checkNotNull(segments, "segments");
    this.pendingSegmentUpgrades = pendingSegmentUpgrades == null
                                  ? ImmutableList.of()
                                  : ImmutableList.copyOf(pendingSegmentUpgrades);
    this.success = success;
    this.errorMsg = errorMsg;

    if (!success) {
      Preconditions.checkArgument(segments.isEmpty(), "segments must be empty for unsuccessful publishes");
    }
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public List<PendingSegmentUpgradeRecord> getPendingSegmentUpgrades()
  {
    return pendingSegmentUpgrades;
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
           Objects.equals(pendingSegmentUpgrades, that.pendingSegmentUpgrades) &&
           Objects.equals(errorMsg, that.errorMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments, pendingSegmentUpgrades, success, errorMsg);
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
