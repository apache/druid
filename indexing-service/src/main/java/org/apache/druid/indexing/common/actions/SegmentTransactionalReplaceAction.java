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
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Action used by a REPLACE task to transactionally commit segments to the
 * metadata store for one or more intervals of a datasource, thereby overshadowing
 * the pre-existing segments in those intervals.
 * Each segment being committed must have a version less than or equal to the
 * {@link org.apache.druid.indexing.common.TaskLockType#REPLACE} lock held by
 * the task over the corresponding interval.
 * <p>
 * This action performs the following operations within a single transaction:
 * <ul>
 * <li>Commit the given REPLACE segments.</li>
 * <li>Upgrade APPEND segments committed to an interval after it had been locked
 * by this REPLACE task.</li>
 * <li>Upgrade pending segments that overlap with any of the REPLACE segments.</li>
 * </ul>
 * After the transaction succeeds, the upgraded pending segments are registered
 * on the real-time ingestion supervisor for this datasource (if any), which then
 * relays the upgrade information to the corresponding real-time tasks.
 */
public class SegmentTransactionalReplaceAction implements TaskAction<SegmentPublishResult>
{
  private static final Logger log = new Logger(SegmentTransactionalReplaceAction.class);

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
    emitSegmentUpgradeMetrics(publishResult, task, toolbox);

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

  private void emitSegmentUpgradeMetrics(SegmentPublishResult publishResult, Task task, TaskActionToolbox toolbox)
  {
    // Log and emit metric for number of upgraded segments
    final List<DataSegment> upgradedSegments = publishResult.getUpgradedSegments();
    if (!CollectionUtils.isNullOrEmpty(upgradedSegments)) {
      final Map<String, Integer> versionToNumUpgradedSegments = new HashMap<>();
      upgradedSegments.forEach(
          segment -> versionToNumUpgradedSegments.merge(segment.getId().getVersion(), 1, Integer::sum)
      );
      versionToNumUpgradedSegments.forEach(
          (version, numUpgradedSegments) -> log.info(
              "Task[%s] of datasource[%s] upgraded [%d] segments to replace version[%s].",
              task.getId(), task.getDataSource(), numUpgradedSegments, version
          )
      );

      final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
      IndexTaskUtils.setTaskDimensions(metricBuilder, task);
      toolbox.getEmitter().emit(
          metricBuilder.setMetric("segment/upgraded/count", upgradedSegments.size())
      );
    }

    // Log and emit metric for number of upgraded pending segments
    final List<PendingSegmentRecord> upgradedPendingSegments = publishResult.getUpgradedPendingSegments();
    if (!CollectionUtils.isNullOrEmpty(upgradedPendingSegments)) {
      final Map<String, Integer> versionToNumUpgradedSegments = new HashMap<>();
      upgradedPendingSegments.forEach(
          segment -> versionToNumUpgradedSegments.merge(segment.getId().getVersion(), 1, Integer::sum)
      );
      versionToNumUpgradedSegments.forEach(
          (version, numUpgradedSegments) -> log.info(
              "Task[%s] of datasource[%s] upgraded [%d] pending segments to replace version[%s].",
              task.getId(), task.getDataSource(), numUpgradedSegments, version
          )
      );

      final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
      IndexTaskUtils.setTaskDimensions(metricBuilder, task);
      toolbox.getEmitter().emit(
          metricBuilder.setMetric("segment/upgradedRealtime/count", upgradedPendingSegments.size())
      );
    }
  }

  /**
   * Registers upgraded pending segments on the active supervisor, if any.
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

    if (!activeSupervisorIdWithAppendLock.isPresent() || upgradedPendingSegments == null) {
      return;
    }

    final String supervisorId = activeSupervisorIdWithAppendLock.get();
    log.info(
        "Registering [%d] upgraded pending segments on supervisor[%s].",
        upgradedPendingSegments.size(), supervisorId
    );
    upgradedPendingSegments.forEach(
        segment -> supervisorManager.registerUpgradedPendingSegmentOnSupervisor(supervisorId, segment)
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
