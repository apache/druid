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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.SpecificSegmentLockRequest;
import org.joda.time.Interval;

/**
 * TaskAction to acquire a {@link org.apache.druid.indexing.common.SegmentLock}.
 * This action is a blocking operation and the caller could wait until it gets {@link LockResult}
 * (up to timeoutMs if it's > 0).
 *
 * This action is currently used by only stream ingestion tasks.
 */
public class SegmentLockAcquireAction implements TaskAction<LockResult>
{
  private final TaskLockType lockType;
  private final Interval interval;
  private final String version;
  private final int partitionId;
  private final long timeoutMs;

  @JsonCreator
  public SegmentLockAcquireAction(
      @JsonProperty("lockType") TaskLockType lockType,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("timeoutMs") long timeoutMs
  )
  {
    this.lockType = Preconditions.checkNotNull(lockType, "lockType");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.partitionId = partitionId;
    this.timeoutMs = timeoutMs;
  }

  @JsonProperty
  public TaskLockType getLockType()
  {
    return lockType;
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
  public int getPartitionId()
  {
    return partitionId;
  }

  @JsonProperty
  public long getTimeoutMs()
  {
    return timeoutMs;
  }

  @Override
  public TypeReference<LockResult> getReturnTypeReference()
  {
    return new TypeReference<LockResult>()
    {
    };
  }

  @Override
  public LockResult perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      if (timeoutMs == 0) {
        return toolbox.getTaskLockbox().lock(
            task,
            new SpecificSegmentLockRequest(lockType, task, interval, version, partitionId)
        );
      } else {
        return toolbox.getTaskLockbox().lock(
            task,
            new SpecificSegmentLockRequest(lockType, task, interval, version, partitionId), timeoutMs
        );
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentLockAcquireAction{" +
           "lockType=" + lockType +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionId=" + partitionId +
           ", timeoutMs=" + timeoutMs +
           '}';
  }
}
