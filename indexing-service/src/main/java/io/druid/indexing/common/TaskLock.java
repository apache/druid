/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Represents a lock held by some task. Immutable.
 */
public class TaskLock
{
  private final TaskLockType type;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final int priority;
  private final TaskLockStatus lockStatus;

  @JsonCreator
  public TaskLock(
      @JsonProperty("type") @Nullable TaskLockType type,            // nullable for backward compatibility
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("priority") int priority,
      @JsonProperty("lockStatus") @Nullable TaskLockStatus lockStatus // nullable for backward compatibility
  )
  {
    this.type = type == null ? TaskLockType.EXCLUSIVE : type;
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.priority = priority;
    this.lockStatus = lockStatus == null ? TaskLockStatus.NON_PREEMPTIBLE : lockStatus;

    Preconditions.checkArgument(
        !this.type.equals(TaskLockType.SHARED) || this.lockStatus != TaskLockStatus.NON_PREEMPTIBLE,
        "lock[%s] cannot be upgraded to non-preemptible",
        this.type
    );
  }

  public TaskLock(
      TaskLockType type,
      String groupId,
      String dataSource,
      Interval interval,
      String version,
      int priority
  )
  {
    this(type, groupId, dataSource, interval, version, priority, TaskLockStatus.PREEMPTIBLE);
  }

  public TaskLock upgrade()
  {
    return new TaskLock(
        type,
        groupId,
        dataSource,
        interval,
        version,
        priority,
        lockStatus.transitTo(TaskLockStatus.NON_PREEMPTIBLE)
    );
  }

  public TaskLock downgrade()
  {
    return new TaskLock(
        type,
        groupId,
        dataSource,
        interval,
        version,
        priority,
        lockStatus.transitTo(TaskLockStatus.PREEMPTIBLE)
    );
  }

  public TaskLock revoke()
  {
    return new TaskLock(
        type,
        groupId,
        dataSource,
        interval,
        version,
        priority,
        lockStatus.transitTo(TaskLockStatus.REVOKED)
    );
  }

  @JsonProperty
  public TaskLockType getType()
  {
    return type;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public int getPriority()
  {
    return priority;
  }

  @JsonProperty
  public TaskLockStatus getLockStatus()
  {
    return lockStatus;
  }

  public boolean isUpgraded()
  {
    return lockStatus == TaskLockStatus.NON_PREEMPTIBLE;
  }

  public boolean isRevoked()
  {
    return lockStatus == TaskLockStatus.REVOKED;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof TaskLock)) {
      return false;
    } else {
      final TaskLock that = (TaskLock) o;
      return this.type.equals(that.type) &&
             this.groupId.equals(that.groupId) &&
             this.dataSource.equals(that.dataSource) &&
             this.interval.equals(that.interval) &&
             this.version.equals(that.version) &&
             this.priority == that.priority &&
             this.lockStatus == that.lockStatus;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(type, groupId, dataSource, interval, version, priority, lockStatus);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("type", type)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .add("priority", priority)
                  .add("lockStatus", lockStatus)
                  .toString();
  }
}
