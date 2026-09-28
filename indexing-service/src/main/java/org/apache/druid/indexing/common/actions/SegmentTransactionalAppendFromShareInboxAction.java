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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.PendingSegmentAllocatingTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.ShareInboxCompletionRequest;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SegmentTransactionalAppendFromShareInboxAction implements TaskAction<SegmentPublishResult>
{
  public static final String TYPE = "segmentTransactionalAppendFromShareInbox";

  private final Set<DataSegment> segments;
  private final ShareInboxCompletionRequest completionRequest;
  @Nullable
  private final SegmentSchemaMapping segmentSchemaMapping;

  @JsonCreator
  public SegmentTransactionalAppendFromShareInboxAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("completionRequest") ShareInboxCompletionRequest completionRequest,
      @JsonProperty("segmentSchemaMapping") @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    this.segments = Set.copyOf(Objects.requireNonNull(segments, "segments"));
    this.completionRequest = Objects.requireNonNull(completionRequest, "completionRequest");
    this.segmentSchemaMapping = segmentSchemaMapping;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public ShareInboxCompletionRequest getCompletionRequest()
  {
    return completionRequest;
  }

  @Nullable
  @JsonProperty
  public SegmentSchemaMapping getSegmentSchemaMapping()
  {
    return segmentSchemaMapping;
  }

  @Override
  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox)
  {
    if (!(task instanceof PendingSegmentAllocatingTask)) {
      throw DruidException.defensive(
          "Task[%s] of type[%s] cannot append share inbox segments because it does not implement PendingSegmentAllocatingTask.",
          task.getId(),
          task.getType()
      );
    }
    ClaimShareInboxManifestsAction.validateTask(
        task,
        completionRequest.getDataSource(),
        completionRequest.getTaskId()
    );

    final List<TaskLock> locks = toolbox.getTaskLockbox().findLocksForTask(task);
    for (TaskLock lock : locks) {
      if (lock.getType() != TaskLockType.APPEND) {
        throw InvalidInput.exception(
            "Cannot use action[%s] for task[%s] as it is holding a lock of type[%s] instead of [APPEND].",
            TYPE,
            task.getId(),
            lock.getType()
        );
      }
    }

    final PendingSegmentAllocatingTask allocatingTask = (PendingSegmentAllocatingTask) task;
    final SegmentPublishResult result;
    if (segments.isEmpty()) {
      result = commit(toolbox, allocatingTask.getTaskAllocatorId(), Map.of());
    } else {
      TaskLocks.checkLockCoversSegments(task, toolbox.getTaskLockbox(), segments);
      final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock = TaskLocks.findReplaceLocksCoveringSegments(
          task.getDataSource(),
          toolbox.getTaskLockbox(),
          segments
      );
      try {
        result = toolbox.getTaskLockbox().doInCriticalSection(
            task,
            segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()),
            CriticalAction.<SegmentPublishResult>builder()
                .onValidLocks(() -> commit(toolbox, allocatingTask.getTaskAllocatorId(), segmentToReplaceLock))
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
    }

    IndexTaskUtils.emitSegmentPublishMetrics(result, task, toolbox);
    return result;
  }

  private SegmentPublishResult commit(
      TaskActionToolbox toolbox,
      String taskAllocatorId,
      Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
  )
  {
    return toolbox.getIndexerMetadataStorageCoordinator().commitAppendSegmentsAndShareInbox(
        segments,
        segmentToReplaceLock,
        taskAllocatorId,
        segmentSchemaMapping,
        completionRequest
    );
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalAppendFromShareInboxAction{" +
           "segments=" + segments +
           ", completionRequest=" + completionRequest +
           '}';
  }
}
