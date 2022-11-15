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
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.concurrent.Future;

/**
 * Allocates a pending segment for a given timestamp. The preferredSegmentGranularity is used if there are no prior
 * segments for the given timestamp, or if the prior segments for the given timestamp are already at the
 * preferredSegmentGranularity. Otherwise, the prior segments will take precedence.
 * <p/>
 * This action implicitly acquires some task locks when it allocates segments. You do not have to acquire them
 * beforehand, although you *do* have to release them yourself. (Note that task locks are automatically released when
 * the task is finished.)
 * <p/>
 * If this action cannot acquire an appropriate task lock, or if it cannot expand an existing segment set, it returns
 * null.
 */
public class SegmentAllocateAction implements TaskAction<SegmentIdWithShardSpec>
{
  public static final String TYPE = "segmentAllocate";

  private static final Logger log = new Logger(SegmentAllocateAction.class);

  // Prevent spinning forever in situations where the segment list just won't stop changing.
  private static final int MAX_ATTEMPTS = 90;

  private final String dataSource;
  private final DateTime timestamp;
  private final Granularity queryGranularity;
  private final Granularity preferredSegmentGranularity;
  private final String sequenceName;
  private final String previousSegmentId;
  private final boolean skipSegmentLineageCheck;
  private final PartialShardSpec partialShardSpec;
  private final LockGranularity lockGranularity;
  private final TaskLockType taskLockType;

  @JsonCreator
  public SegmentAllocateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("preferredSegmentGranularity") Granularity preferredSegmentGranularity,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("previousSegmentId") String previousSegmentId,
      @JsonProperty("skipSegmentLineageCheck") boolean skipSegmentLineageCheck,
      // nullable for backward compatibility
      @JsonProperty("shardSpecFactory") @Nullable PartialShardSpec partialShardSpec,
      @JsonProperty("lockGranularity") @Nullable LockGranularity lockGranularity,
      @JsonProperty("taskLockType") @Nullable TaskLockType taskLockType
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.queryGranularity = Preconditions.checkNotNull(queryGranularity, "queryGranularity");
    this.preferredSegmentGranularity = Preconditions.checkNotNull(
        preferredSegmentGranularity,
        "preferredSegmentGranularity"
    );
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
    this.previousSegmentId = previousSegmentId;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
    this.partialShardSpec = partialShardSpec == null ? NumberedPartialShardSpec.instance() : partialShardSpec;
    this.lockGranularity = lockGranularity == null ? LockGranularity.TIME_CHUNK : lockGranularity;
    this.taskLockType = taskLockType == null ? TaskLockType.EXCLUSIVE : taskLockType;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Granularity getPreferredSegmentGranularity()
  {
    return preferredSegmentGranularity;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  @JsonProperty
  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  @JsonProperty("shardSpecFactory")
  public PartialShardSpec getPartialShardSpec()
  {
    return partialShardSpec;
  }

  @JsonProperty
  public LockGranularity getLockGranularity()
  {
    return lockGranularity;
  }

  @JsonProperty
  public TaskLockType getTaskLockType()
  {
    return taskLockType;
  }

  @Override
  public TypeReference<SegmentIdWithShardSpec> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdWithShardSpec>()
    {
    };
  }

  public Future<SegmentIdWithShardSpec> performAsync(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getSegmentAllocationQueue().add(
        new SegmentAllocateRequest(task, this, MAX_ATTEMPTS)
    );
  }

  @Override
  public SegmentIdWithShardSpec perform(Task task, TaskActionToolbox toolbox)
  {
    if (!task.getDataSource().equals(dataSource)) {
      throw new IAE("Task dataSource must match action dataSource, [%s] != [%s].", task.getDataSource(), dataSource);
    }

    try {
      return performAsync(task, toolbox).get();
    }
    catch (Exception e) {
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
    return "SegmentAllocateAction{" +
           "dataSource='" + dataSource + '\'' +
           ", timestamp=" + timestamp +
           ", queryGranularity=" + queryGranularity +
           ", preferredSegmentGranularity=" + preferredSegmentGranularity +
           ", sequenceName='" + sequenceName + '\'' +
           ", previousSegmentId='" + previousSegmentId + '\'' +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           ", partialShardSpec=" + partialShardSpec +
           ", lockGranularity=" + lockGranularity +
           '}';
  }
}
