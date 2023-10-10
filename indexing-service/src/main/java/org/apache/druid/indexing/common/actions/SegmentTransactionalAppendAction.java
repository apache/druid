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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Append segments to metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 */
public class SegmentTransactionalAppendAction implements TaskAction<SegmentPublishResult>
{
  private final Set<DataSegment> segments;
  @Nullable
  private final DataSourceMetadata startMetadata;
  @Nullable
  private final DataSourceMetadata endMetadata;

  public static SegmentTransactionalAppendAction forSegments(Set<DataSegment> segments)
  {
    return new SegmentTransactionalAppendAction(segments, null, null);
  }

  public static SegmentTransactionalAppendAction forSegmentsAndMetadata(
      Set<DataSegment> segments,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    return new SegmentTransactionalAppendAction(segments, startMetadata, endMetadata);
  }

  @JsonCreator
  private SegmentTransactionalAppendAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("startMetadata") @Nullable DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") @Nullable DataSourceMetadata endMetadata
  )
  {
    this.segments = segments;
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;

    if ((startMetadata == null && endMetadata != null)
        || (startMetadata != null && endMetadata == null)) {
      throw InvalidInput.exception("startMetadata and endMetadata must either be both null or both non-null.");
    }
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  @Nullable
  public DataSourceMetadata getStartMetadata()
  {
    return startMetadata;
  }

  @JsonProperty
  @Nullable
  public DataSourceMetadata getEndMetadata()
  {
    return endMetadata;
  }

  @Override
  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<SegmentPublishResult>()
    {
    };
  }

  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox)
  {
    // Verify that all the locks are of expected type
    final List<TaskLock> locks = toolbox.getTaskLockbox().findLocksForTask(task);
    for (TaskLock lock : locks) {
      if (lock.getType() != TaskLockType.APPEND) {
        throw InvalidInput.exception(
            "Cannot use action[%s] for task[%s] as it is holding a lock of type[%s] instead of [APPEND].",
            "SegmentTransactionalAppendAction", task.getId(), lock.getType()
        );
      }
    }

    TaskLocks.checkLockCoversSegments(task, toolbox.getTaskLockbox(), segments);

    final String datasource = task.getDataSource();
    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
        = TaskLocks.findReplaceLocksCoveringSegments(datasource, toolbox.getTaskLockbox(), segments);

    final CriticalAction.Action<SegmentPublishResult> publishAction;
    if (startMetadata == null) {
      publishAction = () -> toolbox.getIndexerMetadataStorageCoordinator().commitAppendSegments(
          segments,
          segmentToReplaceLock
      );
    } else {
      publishAction = () -> toolbox.getIndexerMetadataStorageCoordinator().commitAppendSegmentsAndMetadata(
          segments,
          segmentToReplaceLock,
          startMetadata,
          endMetadata
      );
    }

    final SegmentPublishResult retVal;
    try {
      retVal = toolbox.getTaskLockbox().doInCriticalSection(
          task,
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()),
          CriticalAction.<SegmentPublishResult>builder()
              .onValidLocks(publishAction)
              .onInvalidLocks(
                  () -> SegmentPublishResult.fail(
                      "Invalid task locks. Maybe they are revoked by a higher priority task."
                      + " Please check the overlord log for details."
                  )
              )
              .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    IndexTaskUtils.emitSegmentPublishMetrics(retVal, task, toolbox);
    return retVal;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalAppendAction{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           '}';
  }
}
