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

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class TimeChunkLockRequest implements LockRequest
{
  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  @Nullable
  private final String preferredVersion;
  private final int priority;
  private final boolean revoked;

  public TimeChunkLockRequest(
      TaskLockType lockType,
      Task task,
      Interval interval,
      @Nullable String preferredVersion
  )
  {
    this(lockType, task.getGroupId(), task.getDataSource(), interval, preferredVersion, task.getPriority(), false);
  }

  public TimeChunkLockRequest(LockRequestForNewSegment lockRequestForNewSegment)
  {
    this(
        lockRequestForNewSegment.getType(),
        lockRequestForNewSegment.getGroupId(),
        lockRequestForNewSegment.getDataSource(),
        lockRequestForNewSegment.getInterval(),
        lockRequestForNewSegment.getVersion(),
        lockRequestForNewSegment.getPriority(),
        lockRequestForNewSegment.isRevoked()
    );
  }

  public TimeChunkLockRequest(
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      @Nullable String preferredVersion,
      int priority,
      boolean revoked
  )
  {
    this.lockType = lockType;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.preferredVersion = preferredVersion;
    this.priority = priority;
    this.revoked = revoked;
  }

  @Override
  public LockGranularity getGranularity()
  {
    return LockGranularity.TIME_CHUNK;
  }

  @Override
  public TaskLockType getType()
  {
    return lockType;
  }

  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public String getVersion()
  {
    return preferredVersion == null ? DateTimes.nowUtc().toString() : preferredVersion;
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  @Override
  public boolean isRevoked()
  {
    return revoked;
  }

  @Override
  public TaskLock toLock()
  {
    return new TimeChunkLock(
        lockType,
        groupId,
        dataSource,
        interval,
        getVersion(),
        priority,
        revoked
    );
  }

  @Override
  public String toString()
  {
    return "TimeChunkLockRequest{" +
           "lockType=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", preferredVersion='" + preferredVersion + '\'' +
           ", priority=" + priority +
           ", revoked=" + revoked +
           '}';
  }
}
