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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.SegmentUpgradeMetrics;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
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
   * Upper bound on the number of per-segment entries included in the INFO summary of upgraded pending segments. A broad
   * REPLACE (e.g. compaction over a wide interval of sparse, finely-partitioned append data) can upgrade thousands of
   * pending segments, so the full mapping is reserved for DEBUG.
   */
  private static final int NOTIFIED_LOG_SAMPLE_SIZE = 10;

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
    return new TypeReference<>() {};
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
  @VisibleForTesting
  void registerUpgradedPendingSegmentsOnSupervisor(
      Task task,
      TaskActionToolbox toolbox,
      List<PendingSegmentRecord> upgradedPendingSegments
  )
  {
    if (upgradedPendingSegments == null || upgradedPendingSegments.isEmpty()) {
      return;
    }

    // Emit one persisted event per upgraded segment (rather than a single aggregate) regardless of whether a supervisor
    // exists to receive them, so the total can be compared against the count actually announced by tasks and so each
    // event carries the segment's interval and version.
    for (PendingSegmentRecord upgradedPendingSegment : upgradedPendingSegments) {
      final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
      IndexTaskUtils.setTaskDimensions(metricBuilder, task);
      IndexTaskUtils.setPendingSegmentDimensions(metricBuilder, upgradedPendingSegment);
      toolbox.getEmitter().emit(metricBuilder.setMetric(SegmentUpgradeMetrics.PERSISTED, 1));
    }

    final SupervisorManager supervisorManager = toolbox.getSupervisorManager();
    final List<String> activeSupervisorIdWithAppendLock =
        supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(task.getDataSource());

    if (activeSupervisorIdWithAppendLock.isEmpty()) {
      log.info(
          "Could not find any active concurrent streaming supervisor for datasource[%s]."
          + " Ignoring registry of [%d] pending segment(s) upgraded by task[%s]. These will become queryable only"
          + " after handoff.",
          task.getDataSource(),
          upgradedPendingSegments.size(),
          task.getId()
      );
      return;
    }

    // Try to register the upgraded pending segments with all eligible supervisors
    // Only the supervisor which owns a pending segment will actually register it
    for (String supervisorId : activeSupervisorIdWithAppendLock) {
      registerUpgradedPendingSegmentOnSupervisor(task, toolbox, upgradedPendingSegments, supervisorId);
    }
  }

  /**
   * Registers each upgraded pending segment on the given supervisor and prints
   * a summary of the batch.
   */
  private void registerUpgradedPendingSegmentOnSupervisor(
      Task task,
      TaskActionToolbox toolbox,
      List<PendingSegmentRecord> upgradedPendingSegments,
      String supervisorId
  )
  {
    final Map<String, Integer> notifiedTasksBySegment = new LinkedHashMap<>();
    for (PendingSegmentRecord upgradedPendingSegment : upgradedPendingSegments) {
      final OptionalInt notified = toolbox.getSupervisorManager()
          .registerUpgradedPendingSegmentOnSupervisor(supervisorId, upgradedPendingSegment);
      if (notified.isEmpty()) {
        continue;
      }
      final int notifiedCount = notified.getAsInt();
      notifiedTasksBySegment.put(upgradedPendingSegment.getId().toString(), notifiedCount);
    }

    final boolean truncateSummary = notifiedTasksBySegment.size() > NOTIFIED_LOG_SAMPLE_SIZE && !log.isDebugEnabled();
    log.info(
        "Registered [%d] upgraded pending segment(s) created by task[%s] on supervisor[%s]."
        + " Tasks notified per segment %s: %s",
        notifiedTasksBySegment.size(),
        task.getId(),
        supervisorId,
        truncateSummary ? " (first " + NOTIFIED_LOG_SAMPLE_SIZE + ", enable debug for all)" : "",
        truncateSummary
        ? notifiedTasksBySegment.entrySet().stream().limit(NOTIFIED_LOG_SAMPLE_SIZE)
        : notifiedTasksBySegment
    );
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalReplaceAction{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           '}';
  }
}
