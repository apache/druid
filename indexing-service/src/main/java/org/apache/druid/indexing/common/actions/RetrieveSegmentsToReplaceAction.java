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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractBatchSubtask;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This TaskAction returns a collection of segments which have data within the specified interval and are marked as
 * used, and have been created before a REPLACE lock, if any, was acquired.
 *
 * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
 * the collection only once.
 *
 * @implNote This action doesn't produce a {@link Set} because it's implemented via {@link
 * org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator#retrieveUsedSegmentsForIntervals} which returns
 * a collection. Producing a {@link Set} would require an unnecessary copy of segments collection.
 */
public class RetrieveSegmentsToReplaceAction implements TaskAction<Collection<DataSegment>>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final Interval interval;

  @JsonCreator
  public RetrieveSegmentsToReplaceAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public TypeReference<Collection<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Collection<DataSegment>>() {};
  }

  @Override
  public Collection<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    if (!task.getDataSource().equals(dataSource)) {
      return toolbox.getIndexerMetadataStorageCoordinator()
                    .retrieveUsedSegmentsForInterval(dataSource, interval, Segments.ONLY_VISIBLE);
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

    Collection<Pair<DataSegment, String>> segmentsAndCreatedDates =
        toolbox.getIndexerMetadataStorageCoordinator().retrieveUsedSegmentsAndCreatedDates(dataSource, interval);

    Set<DataSegment> allSegmentsToBeReplaced = new HashSet<>();
    segmentsAndCreatedDates.forEach(segmentAndCreatedDate -> allSegmentsToBeReplaced.add(segmentAndCreatedDate.lhs));
    for (Pair<DataSegment, String> segmentAndCreatedDate : segmentsAndCreatedDates) {
      final DataSegment segment = segmentAndCreatedDate.lhs;
      final String createdDate = segmentAndCreatedDate.rhs;
      for (ReplaceTaskLock replaceLock : replaceLocksForTask) {
        if (replaceLock.getInterval().contains(segment.getInterval())
            && replaceLock.getVersion().compareTo(createdDate) < 0) {
          // If a REPLACE lock covers a segment but has a version less than the segment's created date, remove it
          allSegmentsToBeReplaced.remove(segment);
        }
      }
    }

    return SegmentTimeline.forSegments(allSegmentsToBeReplaced)
                          .findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
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

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    return interval.equals(that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, interval);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           '}';
  }
}
