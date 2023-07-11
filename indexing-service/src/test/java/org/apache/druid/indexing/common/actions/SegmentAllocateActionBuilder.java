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

import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.DateTime;

public class SegmentAllocateActionBuilder
{
  private String dataSource;
  private DateTime timestamp;
  private Granularity queryGranularity;
  private Granularity preferredSegmentGranularity;
  private String sequenceName;
  private String previousSegmentId;
  private boolean skipSegmentLineageCheck;
  private PartialShardSpec partialShardSpec;
  private LockGranularity lockGranularity;
  private TaskLockType taskLockType;
  private Task task;

  public SegmentAllocateActionBuilder forDatasource(String dataSource)
  {
    this.dataSource = dataSource;
    return this;
  }

  public SegmentAllocateActionBuilder forTimestamp(DateTime timestamp)
  {
    this.timestamp = timestamp;
    return this;
  }

  public SegmentAllocateActionBuilder forTimestamp(String instant)
  {
    this.timestamp = DateTimes.of(instant);
    return this;
  }

  public SegmentAllocateActionBuilder withQueryGranularity(Granularity queryGranularity)
  {
    this.queryGranularity = queryGranularity;
    return this;
  }

  public SegmentAllocateActionBuilder withSegmentGranularity(Granularity segmentGranularity)
  {
    this.preferredSegmentGranularity = segmentGranularity;
    return this;
  }

  public SegmentAllocateActionBuilder withSequenceName(String sequenceName)
  {
    this.sequenceName = sequenceName;
    return this;
  }

  public SegmentAllocateActionBuilder withPreviousSegmentId(String previousSegmentId)
  {
    this.previousSegmentId = previousSegmentId;
    return this;
  }

  public SegmentAllocateActionBuilder withSkipLineageCheck(boolean skipLineageCheck)
  {
    this.skipSegmentLineageCheck = skipLineageCheck;
    return this;
  }

  public SegmentAllocateActionBuilder withPartialShardSpec(PartialShardSpec partialShardSpec)
  {
    this.partialShardSpec = partialShardSpec;
    return this;
  }

  public SegmentAllocateActionBuilder withLockGranularity(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
    return this;
  }

  public SegmentAllocateActionBuilder withTaskLockType(TaskLockType taskLockType)
  {
    this.taskLockType = taskLockType;
    return this;
  }

  public SegmentAllocateActionBuilder forTask(Task task)
  {
    this.dataSource = task.getDataSource();
    this.sequenceName = task.getId();
    this.task = task;
    return this;
  }

  public SegmentAllocateRequest build()
  {
    return new SegmentAllocateRequest(task, buildAction(), 1);
  }

  public SegmentAllocateAction buildAction()
  {
    return new SegmentAllocateAction(
        dataSource,
        timestamp,
        queryGranularity,
        preferredSegmentGranularity,
        sequenceName,
        previousSegmentId,
        skipSegmentLineageCheck,
        partialShardSpec,
        lockGranularity,
        taskLockType
    );
  }
}
