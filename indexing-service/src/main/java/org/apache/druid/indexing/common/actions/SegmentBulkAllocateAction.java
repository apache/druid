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
import org.apache.druid.indexing.overlord.LockRequest;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SegmentBulkAllocateAction implements TaskAction<Map<Interval, List<SegmentIdWithShardSpec>>>
{
  // interval -> # of segments to allocate
  private final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec;
  private final String baseSequenceName;

  @JsonCreator
  public SegmentBulkAllocateAction(
      @JsonProperty("allocateSpec") Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec,
      @JsonProperty("baseSequenceName") String baseSequenceName
  )
  {
    this.allocateSpec = allocateSpec;
    this.baseSequenceName = baseSequenceName;
  }

  @JsonProperty
  public Map<Interval, Pair<ShardSpecFactory, Integer>> getAllocateSpec()
  {
    return allocateSpec;
  }

  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @Override
  public TypeReference<Map<Interval, List<SegmentIdWithShardSpec>>> getReturnTypeReference()
  {
    return new TypeReference<Map<Interval, List<SegmentIdWithShardSpec>>>()
    {
    };
  }

  @Override
  public Map<Interval, List<SegmentIdWithShardSpec>> perform(Task task, TaskActionToolbox toolbox)
  {
    final Map<Interval, List<SegmentIdWithShardSpec>> segmentIds = new HashMap<>(allocateSpec.size());

    for (Entry<Interval, Pair<ShardSpecFactory, Integer>> entry : allocateSpec.entrySet()) {
      final Interval interval = entry.getKey();
      final ShardSpecFactory shardSpecFactory = entry.getValue().lhs;
      final int numSegmentsToAllocate = Preconditions.checkNotNull(
          entry.getValue().rhs,
          "numSegmentsToAllocate for interval[%s]",
          interval
      );

      for (int i = 0; i < numSegmentsToAllocate; i++) {
        final String sequenceName = StringUtils.format("%s_%d", baseSequenceName, i);
        final LockRequest lockRequest = new LockRequestForNewSegment(
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            interval,
            shardSpecFactory,
            task.getPriority(),
            sequenceName,
            null,
            true
        );

        final LockResult lockResult = toolbox.getTaskLockbox().tryLock(task, lockRequest);

        if (lockResult.isRevoked()) {
          // The lock was preempted by other tasks
          throw new ISE("WTH? lock[%s] for new segment request is revoked?", lockResult.getTaskLock());
        }

        if (lockResult.isOk()) {
          final SegmentIdWithShardSpec identifier = lockResult.getNewSegmentId();
          if (identifier != null) {
            segmentIds.computeIfAbsent(interval, k -> new ArrayList<>()).add(identifier);
          } else {
            throw new ISE("Cannot allocate new pending segmentIds with request[%s]", lockRequest);
          }
        } else {
          throw new ISE("Could not acquire lock with request[%s]", lockRequest);
        }
      }
    }

    return segmentIds;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentBulkAllocateAction{" +
           "allocateSpec=" + allocateSpec +
           ", baseSequenceName='" + baseSequenceName + '\'' +
           '}';
  }
}
