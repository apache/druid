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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task action to commit realtime segments and metadata when using APPEND task locks.
 * <p>
 * This action performs the following operations in a single transaction:
 * <ul>
 * <li>Commit the append segments</li>
 * <li>Upgrade the append segments to all visible REPLACE versions</li>
 * <li>Commit start and end {@link DataSourceMetadata}.</li>
 * </ul>
 * This action differs from {@link SegmentTransactionalInsertAction} as it is used
 * only with APPEND locks and also upgrades segments as needed.
 */
public class CommitRealtimeSegmentsAndMetadataAction implements TaskAction<SegmentPublishResult>
{
  /**
   * Set of segments to be inserted into metadata storage
   */
  private final Set<DataSegment> segments;

  private final DataSourceMetadata startMetadata;
  private final DataSourceMetadata endMetadata;

  public static CommitRealtimeSegmentsAndMetadataAction create(
      Set<DataSegment> segments,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    return new CommitRealtimeSegmentsAndMetadataAction(segments, startMetadata, endMetadata);
  }

  @JsonCreator
  private CommitRealtimeSegmentsAndMetadataAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("startMetadata") DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") DataSourceMetadata endMetadata
  )
  {
    Preconditions.checkArgument(
        segments != null && !segments.isEmpty(),
        "Segments to commit should not be empty"
    );
    this.segments = segments;
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DataSourceMetadata getStartMetadata()
  {
    return startMetadata;
  }

  @JsonProperty
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
            "CommitRealtimeSegmentsAndMetadata", task.getId(), lock.getType()
        );
      }
    }

    TaskLocks.checkLockCoversSegments(task, toolbox.getTaskLockbox(), segments);
    final String datasource = task.getDataSource();
    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
        = TaskLocks.findReplaceLocksCoveringSegments(datasource, toolbox.getTaskLockbox(), segments);

    final SegmentPublishResult publishResult;
    try {
      publishResult = toolbox.getTaskLockbox().doInCriticalSection(
          task,
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()),
          CriticalAction.<SegmentPublishResult>builder()
              .onValidLocks(
                  () -> toolbox.getIndexerMetadataStorageCoordinator().commitAppendSegmentsAndMetadata(
                      segments,
                      segmentToReplaceLock,
                      startMetadata,
                      endMetadata
                  )
              )
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

    IndexTaskUtils.emitSegmentPublishMetrics(publishResult, task, toolbox);
    return publishResult;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "CommitRealtimeSegmentsAndMetadataAction{" +
           ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", startMetadata=" + startMetadata +
           ", endMetadata=" + endMetadata +
           '}';
  }
}
