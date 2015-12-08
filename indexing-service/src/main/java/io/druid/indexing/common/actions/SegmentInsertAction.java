/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.indexing.common.task.Task;
import io.druid.query.DruidMetrics;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Set;

/**
 * Insert segments into metadata storage. The segment versions must all be less than or equal to a lock held by
 * your task for the segment intervals.
 * <p/>
 * Word of warning: Very large "segments" sets can cause oversized audit log entries, which is bad because it means
 * that the task cannot actually complete. Callers should avoid this by avoiding inserting too many segments in the
 * same action.
 */
public class SegmentInsertAction implements TaskAction<Set<DataSegment>>
{
  private final Set<DataSegment> segments;
  private final Object oldCommitMetadata;
  private final Object newCommitMetadata;

  public SegmentInsertAction(
      Set<DataSegment> segments
  )
  {
    this(segments, null, null);
  }

  @JsonCreator
  public SegmentInsertAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("oldCommitMetadata") Object oldCommitMetadata,
      @JsonProperty("newCommitMetadata") Object newCommitMetadata
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
    this.oldCommitMetadata = oldCommitMetadata;
    this.newCommitMetadata = newCommitMetadata;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public Object getOldCommitMetadata()
  {
    return oldCommitMetadata;
  }

  @JsonProperty
  public Object getNewCommitMetadata()
  {
    return newCommitMetadata;
  }

  public TypeReference<Set<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Set<DataSegment>>()
    {
    };
  }

  @Override
  public Set<DataSegment> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    // TODO: It's possible that we lose our locks after calling this. This should be OK if we're using commitMetadata.
    // TODO: Although, of course, that's not always used...
    toolbox.verifyTaskLocks(task, segments);

    // TODO: I'm pretty sure the attempt at transactionality is foiled by:
    // TODO:  - a zombie task can clobber a good segment on deep storage
    // TODO:  - announceHistoricalSegments will silently do nothing if one already exists with the same id
    final Set<DataSegment> retVal = toolbox.getIndexerMetadataStorageCoordinator().announceHistoricalSegments(
        segments,
        oldCommitMetadata,
        newCommitMetadata
    );

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setDimension(DruidMetrics.DATASOURCE, task.getDataSource())
        .setDimension(DruidMetrics.TASK_TYPE, task.getType());

    for (DataSegment segment : segments) {
      metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("segment/added/bytes", segment.getSize()));
    }

    return retVal;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentInsertAction{" +
           "segments=" + segments +
           '}';
  }
}
