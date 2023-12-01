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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractBatchSubtask;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This action exists in addition to retrieveUsedSegmentsAction because that action suffers
 * from a race condition described by the following sequence of events:
 *
 * -Segments S1, S2, S3 exist
 * -Compact acquires a replace lock
 * -A concurrent appending job publishes a segment S4 which needs to be upgraded to the replace lock's version
 * -Compact task processes S1-S4 to create new segments
 * -Compact task publishes new segments and carries S4 forward to the new version
 *
 * This can lead to the data in S4 being duplicated
 *
 * This TaskAction returns a collection of segments which have data within the specified interval and are marked as
 * used, and have been created before a REPLACE lock, if any, was acquired.
 * This ensures that a consistent set of segments is returned each time this action is called
 */
public class RetrieveSegmentsToReplaceAction implements TaskAction<Collection<DataSegment>>
{
  private static final Logger log = new Logger(RetrieveSegmentsToReplaceAction.class);

  private final String dataSource;

  private final List<Interval> intervals;

  @JsonCreator
  public RetrieveSegmentsToReplaceAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.dataSource = dataSource;
    this.intervals = intervals;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public TypeReference<Collection<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Collection<DataSegment>>() {};
  }

  @Override
  public Collection<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    // The DruidInputSource can be used to read from one datasource and write to another.
    // In such a case, the race condition described in the class-level docs cannot occur,
    // and the action can simply fetch all visible segments for the datasource and interval
    if (!task.getDataSource().equals(dataSource)) {
      return retrieveAllVisibleSegments(toolbox);
    }

    final String supervisorId;
    if (task instanceof AbstractBatchSubtask) {
      supervisorId = ((AbstractBatchSubtask) task).getSupervisorTaskId();
    } else {
      supervisorId = task.getId();
    }

    final Set<ReplaceTaskLock> replaceLocksForTask = toolbox
        .getTaskLockbox()
        .getAllReplaceLocksForDatasource(task.getDataSource())
        .stream()
        .filter(lock -> supervisorId.equals(lock.getSupervisorTaskId()))
        .collect(Collectors.toSet());

    // If there are no replace locks for the task, simply fetch all visible segments for the interval
    if (replaceLocksForTask.isEmpty()) {
      return retrieveAllVisibleSegments(toolbox);
    }

    Map<Interval, Map<String, Set<DataSegment>>> intervalToCreatedToSegments = new HashMap<>();
    for (Pair<DataSegment, String> segmentAndCreatedDate :
        toolbox.getIndexerMetadataStorageCoordinator().retrieveUsedSegmentsAndCreatedDates(dataSource, intervals)) {
      final DataSegment segment = segmentAndCreatedDate.lhs;
      final String created = segmentAndCreatedDate.rhs;
      intervalToCreatedToSegments.computeIfAbsent(segment.getInterval(), s -> new HashMap<>())
                                 .computeIfAbsent(created, c -> new HashSet<>())
                                 .add(segment);
    }

    Set<DataSegment> allSegmentsToBeReplaced = new HashSet<>();
    for (final Map.Entry<Interval, Map<String, Set<DataSegment>>> entry : intervalToCreatedToSegments.entrySet()) {
      final Interval segmentInterval = entry.getKey();
      String lockVersion = null;
      for (ReplaceTaskLock replaceLock : replaceLocksForTask) {
        if (replaceLock.getInterval().contains(segmentInterval)) {
          lockVersion = replaceLock.getVersion();
        }
      }
      final Map<String, Set<DataSegment>> createdToSegmentsMap = entry.getValue();
      for (Map.Entry<String, Set<DataSegment>> createdAndSegments : createdToSegmentsMap.entrySet()) {
        if (lockVersion == null || lockVersion.compareTo(createdAndSegments.getKey()) > 0) {
          allSegmentsToBeReplaced.addAll(createdAndSegments.getValue());
        } else {
          for (DataSegment segment : createdAndSegments.getValue()) {
            log.info("Ignoring segment[%s] as it has created_date[%s] greater than the REPLACE lock version[%s]",
                     segment.getId(), createdAndSegments.getKey(), lockVersion);
          }
        }
      }
    }

    return SegmentTimeline.forSegments(allSegmentsToBeReplaced)
                          .findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
  }

  private Collection<DataSegment> retrieveAllVisibleSegments(TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .retrieveUsedSegmentsForIntervals(dataSource, intervals, Segments.ONLY_VISIBLE);
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RetrieveSegmentsToReplaceAction that = (RetrieveSegmentsToReplaceAction) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(intervals, that.intervals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals);
  }
  @Override
  public String toString()
  {
    return "RetrieveSegmentsToReplaceAction{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           '}';
  }
}
