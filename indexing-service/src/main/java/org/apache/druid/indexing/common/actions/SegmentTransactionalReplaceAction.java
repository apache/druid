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
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.CriticalAction;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.TaskLockInfo;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Replace segments in metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 */
public class SegmentTransactionalReplaceAction implements TaskAction<SegmentPublishResult>
{
  /**
   * Set of segments that was fully overshadowed by new segments, {@link SegmentTransactionalReplaceAction#segments}
   */
  @Nullable
  private final Set<DataSegment> segmentsToBeOverwritten;
  /**
   * Set of segments to be inserted into metadata storage
   */
  private final Set<DataSegment> segments;
  /**
   * Set of segments to be dropped (mark unused) when new segments, {@link SegmentTransactionalReplaceAction#segments},
   * are inserted into metadata storage.
   */
  @Nullable
  private final Set<DataSegment> segmentsToBeDropped;

  @Nullable
  private final DataSourceMetadata startMetadata;
  @Nullable
  private final DataSourceMetadata endMetadata;
  @Nullable
  private final String dataSource;

  public static SegmentTransactionalReplaceAction overwriteAction(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      @Nullable Set<DataSegment> segmentsToBeDropped,
      Set<DataSegment> segmentsToPublish
  )
  {
    return new SegmentTransactionalReplaceAction(segmentsToBeOverwritten, segmentsToBeDropped, segmentsToPublish, null, null, null);
  }

  @JsonCreator
  private SegmentTransactionalReplaceAction(
      @JsonProperty("segmentsToBeOverwritten") @Nullable Set<DataSegment> segmentsToBeOverwritten,
      @JsonProperty("segmentsToBeDropped") @Nullable Set<DataSegment> segmentsToBeDropped,
      @JsonProperty("segments") @Nullable Set<DataSegment> segments,
      @JsonProperty("startMetadata") @Nullable DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") @Nullable DataSourceMetadata endMetadata,
      @JsonProperty("dataSource") @Nullable String dataSource
  )
  {
    this.segmentsToBeOverwritten = segmentsToBeOverwritten;
    this.segmentsToBeDropped = segmentsToBeDropped;
    this.segments = segments == null ? ImmutableSet.of() : ImmutableSet.copyOf(segments);
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;
    this.dataSource = dataSource;
  }

  @JsonProperty
  @Nullable
  public Set<DataSegment> getSegmentsToBeOverwritten()
  {
    return segmentsToBeOverwritten;
  }

  @JsonProperty
  @Nullable
  public Set<DataSegment> getSegmentsToBeDropped()
  {
    return segmentsToBeDropped;
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

  @JsonProperty
  @Nullable
  public String getDataSource()
  {
    return dataSource;
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
    final SegmentPublishResult retVal;

    if (segments.isEmpty()) {
      // A stream ingestion task didn't ingest any rows and created no segments (e.g., all records were unparseable),
      // but still needs to update metadata with the progress that the task made.
      try {
        retVal = toolbox.getIndexerMetadataStorageCoordinator().commitMetadataOnly(
            dataSource,
            startMetadata,
            endMetadata
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      return retVal;
    }

    final Set<DataSegment> allSegments = new HashSet<>(segments);

    TaskLocks.checkLockCoversSegments(task, toolbox.getTaskLockbox(), allSegments);

    String datasource = task.getDataSource();
    Map<Interval, TaskLock> replaceLocks = new HashMap<>();
    for (TaskLock lock : TaskLocks.findReplaceLocksForSegments(datasource, toolbox.getTaskLockbox(), segments)) {
      replaceLocks.put(lock.getInterval(), lock);
    }
    Set<TaskLockInfo> taskLockInfos = new HashSet<>();
    for (TaskLock taskLock : replaceLocks.values()) {
      taskLockInfos.add(getTaskLockInfo(taskLock));
    }

    try {
      retVal = toolbox.getTaskLockbox().doInCriticalSection(
          task,
          allSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList()),
          CriticalAction.<SegmentPublishResult>builder()
              .onValidLocks(
                  () -> toolbox.getIndexerMetadataStorageCoordinator().announceHistoricalSegments(
                      segments,
                      segmentsToBeDropped,
                      startMetadata,
                      endMetadata,
                      null,
                      taskLockInfos,
                      false
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
      metricBuilder.setDimension(
          DruidMetrics.PARTITIONING_TYPE,
          segment.getShardSpec() == null ? null : segment.getShardSpec().getType()
      );
      toolbox.getEmitter().emit(metricBuilder.build("segment/added/bytes", segment.getSize()));
    }

    return retVal;
  }

  private TaskLockInfo getTaskLockInfo(TaskLock taskLock)
  {
    return new TaskLockInfo(taskLock.getInterval(), taskLock.getVersion());
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentTransactionalInsertAction{" +
           "segmentsToBeOverwritten=" + SegmentUtils.commaSeparatedIdentifiers(segmentsToBeOverwritten) +
           ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", startMetadata=" + startMetadata +
           ", endMetadata=" + endMetadata +
           ", dataSource='" + dataSource + '\'' +
           ", segmentsToBeDropped=" + SegmentUtils.commaSeparatedIdentifiers(segmentsToBeDropped) +
           '}';
  }
}
