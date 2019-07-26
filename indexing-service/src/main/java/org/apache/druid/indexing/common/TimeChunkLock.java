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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.overlord.LockRequest;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

public class TimeChunkLock implements TaskLock
{
  static final String TYPE = "timeChunk";

  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  @Nullable
  private final Integer priority;
  private final boolean revoked;

  @JsonCreator
  public TimeChunkLock(
      @JsonProperty("type") @Nullable TaskLockType lockType,            // nullable for backward compatibility
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("priority") @Nullable Integer priority,
      @JsonProperty("revoked") boolean revoked
  )
  {
    this.lockType = lockType == null ? TaskLockType.EXCLUSIVE : lockType;
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.priority = priority;
    this.revoked = revoked;
  }

  @VisibleForTesting
  public TimeChunkLock(
      TaskLockType type,
      String groupId,
      String dataSource,
      Interval interval,
      String version,
      int priority
  )
  {
    this(type, groupId, dataSource, interval, version, priority, false);
  }

  @Override
  public TaskLock revokedCopy()
  {
    return new TimeChunkLock(
        lockType,
        groupId,
        dataSource,
        interval,
        version,
        priority,
        true
    );
  }

  @Override
  public TaskLock withPriority(int priority)
  {
    return new TimeChunkLock(
        this.lockType,
        this.groupId,
        this.dataSource,
        this.interval,
        this.version,
        priority,
        this.revoked
    );
  }

  @Override
  public LockGranularity getGranularity()
  {
    return LockGranularity.TIME_CHUNK;
  }

  @Override
  @JsonProperty
  public TaskLockType getType()
  {
    return lockType;
  }

  @Override
  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @Override
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @Override
  @JsonProperty
  @Nullable
  public Integer getPriority()
  {
    return priority;
  }

  @Override
  public int getNonNullPriority()
  {
    return Preconditions.checkNotNull(priority, "priority");
  }

  @Override
  @JsonProperty
  public boolean isRevoked()
  {
    return revoked;
  }

  @Override
  public boolean conflict(LockRequest request)
  {
    return dataSource.equals(request.getDataSource())
           && interval.overlaps(request.getInterval());
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
    TimeChunkLock that = (TimeChunkLock) o;
    return revoked == that.revoked &&
           lockType == that.lockType &&
           Objects.equals(groupId, that.groupId) &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(version, that.version) &&
           Objects.equals(priority, that.priority);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(lockType, groupId, dataSource, interval, version, priority, revoked);
  }

  @Override
  public String toString()
  {
    return "TimeChunkLock{" +
           "type=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           ", priority=" + priority +
           ", revoked=" + revoked +
           '}';
  }
}
