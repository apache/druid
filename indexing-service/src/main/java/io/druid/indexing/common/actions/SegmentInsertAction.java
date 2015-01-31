/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.indexing.common.task.Task;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Set;

public class SegmentInsertAction implements TaskAction<Set<DataSegment>>
{
  @JsonIgnore
  private final Set<DataSegment> segments;

  @JsonIgnore
  private final boolean allowOlderVersions;

  public SegmentInsertAction(Set<DataSegment> segments)
  {
    this(segments, false);
  }

  @JsonCreator
  public SegmentInsertAction(
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("allowOlderVersions") boolean allowOlderVersions
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
    this.allowOlderVersions = allowOlderVersions;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public boolean isAllowOlderVersions()
  {
    return allowOlderVersions;
  }

  public SegmentInsertAction withAllowOlderVersions(boolean _allowOlderVersions)
  {
    return new SegmentInsertAction(segments, _allowOlderVersions);
  }

  public TypeReference<Set<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Set<DataSegment>>() {};
  }

  @Override
  public Set<DataSegment> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    toolbox.verifyTaskLocksAndSinglePartitionSettitude(task, segments, true);

    final Set<DataSegment> retVal = toolbox.getIndexerMetadataStorageCoordinator().announceHistoricalSegments(segments);

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setUser2(task.getDataSource())
        .setUser4(task.getType());

    for (DataSegment segment : segments) {
      metricBuilder.setUser5(segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("indexer/segment/bytes", segment.getSize()));
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
