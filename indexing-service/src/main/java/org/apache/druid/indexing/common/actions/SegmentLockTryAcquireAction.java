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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TaskAction to try to acquire a {@link org.apache.druid.indexing.common.SegmentLock}.
 * This action returns immediately failed {@link LockResult} if it fails to get locks for the given partitionIds.
 */
public class SegmentLockTryAcquireAction implements TaskAction<List<LockResult>>
{
  private final TaskLockType type;
  private final Interval interval;
  private final String version;
  private final Set<Integer> partitionIds;

  @JsonCreator
  public SegmentLockTryAcquireAction(
      @JsonProperty("lockType") TaskLockType type,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("partitionIds") Set<Integer> partitionIds
  )
  {
    Preconditions.checkState(partitionIds != null && !partitionIds.isEmpty(), "partitionIds is empty");
    this.type = Preconditions.checkNotNull(type, "type");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.version = Preconditions.checkNotNull(version, "version");
    this.partitionIds = partitionIds;
  }

  @JsonProperty("lockType")
  public TaskLockType getType()
  {
    return type;
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
  public Set<Integer> getPartitionIds()
  {
    return partitionIds;
  }

  @Override
  public TypeReference<List<LockResult>> getReturnTypeReference()
  {
    return new TypeReference<List<LockResult>>()
    {
    };
  }

  @Override
  public List<LockResult> perform(Task task, TaskActionToolbox toolbox)
  {
    return partitionIds.stream()
                       .map(partitionId -> toolbox.getTaskLockbox().tryLock(
                           task,
                           new SpecificSegmentLockRequest(type, task, interval, version, partitionId)
                       ))
                       .collect(Collectors.toList());
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentLockTryAcquireAction{" +
           "type=" + type +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionIds=" + partitionIds +
           '}';
  }
}
