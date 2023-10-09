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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Replace segments in metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 */
public class SegmentTransactionalReplaceAction implements TaskAction<SegmentPublishResult>
{
  private static final Logger log = new Logger(SegmentTransactionalReplaceAction.class);

  /**
   * Set of segments to be inserted into metadata storage
   */
  private final Set<DataSegment> segments;

  public static SegmentTransactionalReplaceAction create(
      Set<DataSegment> segmentsToPublish
  )
  {
    return new SegmentTransactionalReplaceAction(segmentsToPublish);
  }

  @JsonCreator
  private SegmentTransactionalReplaceAction(
      @JsonProperty("segments") Set<DataSegment> segments
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
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
                               .commitReplaceSegments(segments, replaceLocksForTask)
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

    if (publishResult.isSuccess()) {
      // If upgrade of pending segments fails, the segments will still get upgraded
      // when the corresponding APPEND task commits the segments.
      // Thus, the upgrade of pending segments should not be done in the same
      // transaction as the commit of replace segments and failure to upgrade
      // pending segments should not affect success of replace commit.
      try {
        List<SegmentIdWithShardSpec> upgradedPendingSegments =
            toolbox.getIndexerMetadataStorageCoordinator().upgradePendingSegments(segments);
        log.info(
            "Upgraded [%d] pending segments for REPLACE task[%s]: [%s]",
            upgradedPendingSegments.size(), task.getId(), upgradedPendingSegments
        );

        // These upgraded pending segments should be forwarded to the SupervisorManager
      }
      catch (Exception e) {
        log.error(e, "Error while upgrading pending segments for task[%s]", task.getId());
      }
    }

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
    return "SegmentTransactionalReplaceAction{" +
           "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           '}';
  }
}
