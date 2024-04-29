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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Replace segments in metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 *
 * <pre>
 *  Pseudo code (for a single interval)
 *- For a replace lock held over an interval:
 *     transaction {
 *       commit input segments contained within interval
 *       upgrade ids in the upgradeSegments table corresponding to this task to the replace lock's version and commit them
 *       fetch payload, task_allocator_id for pending segments
 *       upgrade each such pending segment to the replace lock's version with the corresponding root segment
 *     }
 * For every pending segment with version == replace lock version:
 *    Fetch payload, group_id or the pending segment and relay them to the supervisor
 *    The supervisor relays the payloads to all the tasks with the corresponding group_id to serve realtime queries
 * </pre>
 */
public class SegmentTransactionalReplaceAction implements TaskAction<SegmentPublishResult>
{
  private static final Logger log = new Logger(SegmentTransactionalReplaceAction.class);

  /**
   * Set of segments to be inserted into metadata storage
   */
  private final Set<DataSegment> segments;

  @Nullable
  private final SegmentSchemaMapping segmentSchemaMapping;

  public static SegmentTransactionalReplaceAction create(
      Set<DataSegment> segmentsToPublish,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return new SegmentTransactionalReplaceAction(segmentsToPublish, segmentSchemaMapping);
  }

  @JsonCreator
  private SegmentTransactionalReplaceAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("segmentSchemaMapping") @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
    this.segmentSchemaMapping = segmentSchemaMapping;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  @Nullable
  public SegmentSchemaMapping getSegmentSchemaMapping()
  {
    return segmentSchemaMapping;
  }

  @Override
  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<SegmentPublishResult>()
    {
    };
  }

  /**
   * Performs some sanity checks and publishes the given segments.
   */
  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox)
  {
    TaskLocks.checkLockCoversSegments(task, toolbox.getTaskLockbox(), segments);

    // Find the active replace locks held only by this task
    final Set<ReplaceTaskLock> replaceLocksForTask
        = toolbox.getTaskLockbox().findReplaceLocksForTask(task);

    final SegmentPublishResult publishResult;
    try {
      publishResult = toolbox.getTaskLockbox().doInCriticalSection(
          task,
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()),
          CriticalAction.<SegmentPublishResult>builder()
              .onValidLocks(
                  () -> toolbox.getIndexerMetadataStorageCoordinator()
                               .commitReplaceSegments(segments, replaceLocksForTask, segmentSchemaMapping)
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

    // Upgrade any overlapping pending segments
    // Do not perform upgrade in the same transaction as replace commit so that
    // failure to upgrade pending segments does not affect success of the commit
    if (publishResult.isSuccess() && toolbox.getSupervisorManager() != null) {
      try {
        registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, publishResult.getUpgradedPendingSegments());
      }
      catch (Exception e) {
        log.error(e, "Error while upgrading pending segments for task[%s]", task.getId());
      }
    }

    return publishResult;
  }

  /**
   * Registers upgraded pending segments on the active supervisor, if any
   */
  private void registerUpgradedPendingSegmentsOnSupervisor(
      Task task,
      TaskActionToolbox toolbox,
      List<PendingSegmentRecord> upgradedPendingSegments
  )
  {
    final SupervisorManager supervisorManager = toolbox.getSupervisorManager();
    final Optional<String> activeSupervisorIdWithAppendLock =
        supervisorManager.getActiveSupervisorIdForDatasourceWithAppendLock(task.getDataSource());

    if (!activeSupervisorIdWithAppendLock.isPresent()) {
      return;
    }

    upgradedPendingSegments.forEach(
        upgradedPendingSegment -> supervisorManager.registerUpgradedPendingSegmentOnSupervisor(
            activeSupervisorIdWithAppendLock.get(),
            upgradedPendingSegment
        )
    );
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalReplaceAction{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           '}';
  }
}
