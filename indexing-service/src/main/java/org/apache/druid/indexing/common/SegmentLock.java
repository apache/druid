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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.overlord.LockRequest;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.SpecificSegmentLockRequest;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.Interval;

import java.util.Objects;

/**
 * Lock for set of segments. Should be unique for (dataSource, interval, version, partitionId).
 */
public class SegmentLock implements TaskLock
{
  static final String TYPE = "segment";

  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final int partitionId;
  private final int priority;
  private final boolean revoked;

  @JsonCreator
  public SegmentLock(
      @JsonProperty("type") TaskLockType lockType,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("priority") int priority,
      @JsonProperty("revoked") boolean revoked
  )
  {
    this.lockType = Preconditions.checkNotNull(lockType, "lockType");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.partitionId = partitionId;
    this.priority = priority;
    this.revoked = revoked;
  }

  public SegmentLock(
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      String version,
      int partitionId,
      int priority
  )
  {
    this(lockType, groupId, dataSource, interval, version, partitionId, priority, false);
  }

  @Override
  public TaskLock revokedCopy()
  {
    return new SegmentLock(lockType, groupId, dataSource, interval, version, partitionId, priority, true);
  }

  @Override
  public TaskLock withPriority(int newPriority)
  {
    return new SegmentLock(lockType, groupId, dataSource, interval, version, partitionId, newPriority, revoked);
  }

  @Override
  public LockGranularity getGranularity()
  {
    return LockGranularity.SEGMENT;
  }

  @JsonProperty
  @Override
  public TaskLockType getType()
  {
    return lockType;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public int getPartitionId()
  {
    return partitionId;
  }

  @JsonProperty
  @Override
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  @Override
  public Integer getPriority()
  {
    return priority;
  }

  @Override
  public int getNonNullPriority()
  {
    return priority;
  }

  @JsonProperty
  @Override
  public boolean isRevoked()
  {
    return revoked;
  }

  @Override
  public boolean conflict(LockRequest request)
  {
    if (request instanceof LockRequestForNewSegment) {
      // request for new segments doens't conflict with any locks because it allocates a new partitionId
      return false;
    }

    if (!dataSource.equals(request.getDataSource())) {
      return false;
    }

    if (request instanceof TimeChunkLockRequest) {
      // For different interval, all overlapping intervals cause conflict.
      return interval.overlaps(request.getInterval());
    } else if (request instanceof SpecificSegmentLockRequest) {
      if (interval.equals(request.getInterval())) {
        final SpecificSegmentLockRequest specificSegmentLockRequest = (SpecificSegmentLockRequest) request;
        // Lock conflicts only if the interval is same and the partitionIds intersect.
        return specificSegmentLockRequest.getPartitionId() == partitionId;
      } else {
        // For different interval, all overlapping intervals cause conflict.
        return interval.overlaps(request.getInterval());
      }
    } else {
      throw new ISE("Unknown request type[%s]", request.getClass().getName());
    }
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
    SegmentLock that = (SegmentLock) o;
    return partitionId == that.partitionId &&
           priority == that.priority &&
           revoked == that.revoked &&
           lockType == that.lockType &&
           Objects.equals(groupId, that.groupId) &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(version, that.version);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(lockType, groupId, dataSource, interval, partitionId, version, priority, revoked);
  }

  @Override
  public String toString()
  {
    return "SegmentLock{" +
           "lockType=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionId=" + partitionId +
           ", priority=" + priority +
           ", revoked=" + revoked +
           '}';
  }
}
