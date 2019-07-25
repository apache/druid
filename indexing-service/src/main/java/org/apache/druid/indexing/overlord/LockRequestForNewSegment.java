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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class LockRequestForNewSegment implements LockRequest
{
  private final LockGranularity lockGranularity;
  private final TaskLockType lockType;
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final ShardSpecFactory shardSpecFactory;
  private final int priority;
  private final String sequenceName;
  @Nullable
  private final String previsousSegmentId;
  private final boolean skipSegmentLineageCheck;

  private String version;

  public LockRequestForNewSegment(
      LockGranularity lockGranularity,
      TaskLockType lockType,
      String groupId,
      String dataSource,
      Interval interval,
      ShardSpecFactory shardSpecFactory,
      int priority,
      String sequenceName,
      @Nullable String previsousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    this.lockGranularity = lockGranularity;
    this.lockType = lockType;
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.shardSpecFactory = shardSpecFactory;
    this.priority = priority;
    this.sequenceName = sequenceName;
    this.previsousSegmentId = previsousSegmentId;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
  }

  @VisibleForTesting
  public LockRequestForNewSegment(
      LockGranularity lockGranularity,
      TaskLockType lockType,
      Task task,
      Interval interval,
      ShardSpecFactory shardSpecFactory,
      String sequenceName,
      @Nullable String previsousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    this(
        lockGranularity,
        lockType,
        task.getGroupId(),
        task.getDataSource(),
        interval,
        shardSpecFactory,
        task.getPriority(),
        sequenceName,
        previsousSegmentId,
        skipSegmentLineageCheck
    );
  }

  @Override
  public LockGranularity getGranularity()
  {
    return lockGranularity;
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
  public int getPriority()
  {
    return priority;
  }

  public ShardSpecFactory getShardSpecFactory()
  {
    return shardSpecFactory;
  }

  @Override
  public String getVersion()
  {
    if (version == null) {
      version = DateTimes.nowUtc().toString();
    }
    return version;
  }

  @Override
  public boolean isRevoked()
  {
    return false;
  }

  @Override
  public TaskLock toLock()
  {
    throw new UnsupportedOperationException(
        "This lockRequest must be converted to SpecificSegmentLockRequest or TimeChunkLockRequest first"
        + " to convert to TaskLock"
    );
  }

  public String getSequenceName()
  {
    return sequenceName;
  }

  @Nullable
  public String getPrevisousSegmentId()
  {
    return previsousSegmentId;
  }

  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  @Override
  public String toString()
  {
    return "LockRequestForNewSegment{" +
           "lockGranularity=" + lockGranularity +
           ", lockType=" + lockType +
           ", groupId='" + groupId + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", shardSpecFactory=" + shardSpecFactory +
           ", priority=" + priority +
           ", sequenceName='" + sequenceName + '\'' +
           ", previsousSegmentId='" + previsousSegmentId + '\'' +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           '}';
  }
}
