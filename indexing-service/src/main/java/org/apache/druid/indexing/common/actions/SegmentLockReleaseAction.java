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
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.Interval;

/**
 * TaskAction to release a {@link org.apache.druid.indexing.common.SegmentLock}.
 * Used by batch tasks when they fail to acquire all necessary locks.
 */
public class SegmentLockReleaseAction implements TaskAction<Void>
{
  private final Interval interval;
  private final int partitionId;

  @JsonCreator
  public SegmentLockReleaseAction(@JsonProperty Interval interval, @JsonProperty int partitionId)
  {
    this.interval = interval;
    this.partitionId = partitionId;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public int getPartitionId()
  {
    return partitionId;
  }

  @Override
  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>()
    {
    };
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    toolbox.getTaskLockbox().unlock(task, interval, partitionId);
    return null;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentLockReleaseAction{" +
           "interval=" + interval +
           ", partitionId=" + partitionId +
           '}';
  }
}
