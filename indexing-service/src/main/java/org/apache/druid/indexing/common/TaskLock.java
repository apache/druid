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
  private final Integer priority;
  private final boolean revoked;

  public static TaskLock withPriority(TaskLock lock, int priority)
  {
    return new TaskLock(
        lock.type,
        lock.getGroupId(),
        lock.getDataSource(),
        lock.getInterval(),
        lock.getVersion(),
        priority,
        lock.isRevoked()
    );
  }

  @JsonCreator
  public TaskLock(
      @JsonProperty("type") @Nullable TaskLockType type,            // nullable for backward compatibility
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("priority") @Nullable Integer priority,
      @JsonProperty("revoked") boolean revoked
  )
  {
    this.type = type == null ? TaskLockType.EXCLUSIVE : type;
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.priority = priority;
    this.revoked = revoked;
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
    this(type, groupId, dataSource, interval, version, priority, false);
  }

  public TaskLock revokedCopy()
  {
    return new TaskLock(
        type,
        groupId,
        dataSource,
        interval,
        version,
        priority,
        true
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
  @Nullable
  public Integer getPriority()
  {
    return priority;
  }

  public int getNonNullPriority()
  {
    return Preconditions.checkNotNull(priority, "priority");
  }

  @JsonProperty
  public boolean isRevoked()
  {
    return revoked;
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
             Objects.equal(this.priority, that.priority) &&
             this.revoked == that.revoked;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(type, groupId, dataSource, interval, version, priority, revoked);
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
                  .add("revoked", revoked)
                  .toString();
  }
}
