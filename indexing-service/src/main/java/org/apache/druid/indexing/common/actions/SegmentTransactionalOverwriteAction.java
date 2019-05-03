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
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: merging with SegmentTransactionalInsertAction?
public class SegmentTransactionalOverwriteAction implements TaskAction<SegmentPublishResult>
{
  private final Set<DataSegment> oldSegments;
  private final Set<DataSegment> newSegments;

  @JsonCreator
  public SegmentTransactionalOverwriteAction(
      @JsonProperty("oldSegments") Set<DataSegment> oldSegments,
      @JsonProperty("newSegments") Set<DataSegment> newSegments
  )
  {
    this.oldSegments = oldSegments;
    this.newSegments = newSegments;
  }

  @JsonProperty
  public Set<DataSegment> getOldSegments()
  {
    return oldSegments;
  }

  @JsonProperty
  public Set<DataSegment> getNewSegments()
  {
    return newSegments;
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
    final Set<DataSegment> allSegments = new HashSet<>(oldSegments);
    allSegments.addAll(newSegments);
    TaskActionPreconditions.checkLockCoversSegments(task, toolbox.getTaskLockbox(), allSegments);

    final Map<Interval, Set<Integer>> intervalToPartitionIds = new HashMap<>();
    for (DataSegment segment : allSegments) {
      intervalToPartitionIds.computeIfAbsent(segment.getInterval(), k -> new HashSet<>())
                            .add(segment.getShardSpec().getPartitionNum());
    }

    final List<TaskLock> locks = toolbox.getTaskLockbox().findLocksForTask(task);
    // Let's do some sanity check that newSegments can overwrite oldSegments.
    if (locks.get(0).getGranularity() == LockGranularity.SEGMENT) {
      checkWithSegmentLock();
    }

    final SegmentPublishResult retVal;
    try {
      retVal = toolbox.getTaskLockbox().doInCriticalSection(
          task,
          new ArrayList<>(intervalToPartitionIds.keySet()),
          CriticalAction.<SegmentPublishResult>builder()
              .onValidLocks(
                  () -> toolbox.getIndexerMetadataStorageCoordinator()
                               .announceHistoricalSegments(newSegments, null, null)
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

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);

    if (retVal.isSuccess()) {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/success", 1));
    } else {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/failure", 1));
    }

    // getSegments() should return an empty set if announceHistoricalSegments() failed
    for (DataSegment segment : retVal.getSegments()) {
      metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("segment/added/bytes", segment.getSize()));
    }

    return retVal;
  }

  private void checkWithSegmentLock()
  {
    final Map<Interval, List<DataSegment>> oldSegmentsMap = groupSegmentsByIntervalAndSort(oldSegments);
    final Map<Interval, List<DataSegment>> newSegmentsMap = groupSegmentsByIntervalAndSort(newSegments);

    oldSegmentsMap.values().forEach(AbstractBatchIndexTask::verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull);
    newSegmentsMap.values().forEach(AbstractBatchIndexTask::verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull);

    oldSegmentsMap.forEach((interval, oldSegmentsPerInterval) -> {
      final List<DataSegment> newSegmentsPerInterval = Preconditions.checkNotNull(
          newSegmentsMap.get(interval),
          "segments of interval[%s]",
          interval
      );
      // These lists are already sorted in groupSegmentsByIntervalAndSort().
      final int oldStartRootPartitionId = oldSegmentsPerInterval.get(0).getStartRootPartitionId();
      final int oldEndRootPartitionId = oldSegmentsPerInterval.get(oldSegmentsPerInterval.size() - 1)
                                                              .getEndRootPartitionId();
      final int newStartRootPartitionId = newSegmentsPerInterval.get(0).getStartRootPartitionId();
      final int newEndRootPartitionId = newSegmentsPerInterval.get(newSegmentsPerInterval.size() - 1)
                                                              .getEndRootPartitionId();

      if (oldStartRootPartitionId != newStartRootPartitionId || oldEndRootPartitionId != newEndRootPartitionId) {
        throw new ISE(
            "Root partition range[%d, %d] of new segments doesn't match to root partition range[%d, %d] of old segments",
            newStartRootPartitionId,
            newEndRootPartitionId,
            oldStartRootPartitionId,
            oldEndRootPartitionId
        );
      }

      newSegmentsPerInterval
          .forEach(eachNewSegment -> oldSegmentsPerInterval
              .forEach(eachOldSegment -> {
                if (eachNewSegment.getMinorVersion() <= eachOldSegment.getMinorVersion()) {
                  throw new ISE(
                      "New segment[%s] have a smaller minor version than old segment[%s]",
                      eachNewSegment,
                      eachOldSegment
                  );
                }
              }));
    });
  }

  private static Map<Interval, List<DataSegment>> groupSegmentsByIntervalAndSort(Set<DataSegment> segments)
  {
    final Map<Interval, List<DataSegment>> segmentsMap = new HashMap<>();
    segments.forEach(segment -> segmentsMap.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>())
                                           .add(segment));
    segmentsMap.values().forEach(segmentsPerInterval -> segmentsPerInterval.sort((s1, s2) -> {
      if (s1.getStartRootPartitionId() != s2.getStartRootPartitionId()) {
        return Integer.compare(s1.getStartRootPartitionId(), s2.getStartRootPartitionId());
      } else {
        return Integer.compare(s1.getEndRootPartitionId(), s2.getEndRootPartitionId());
      }
    }));
    return segmentsMap;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalOverwriteAction{" +
           "oldSegments=" + oldSegments +
           ", newSegments=" + newSegments +
           '}';
  }
}
