package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.emitter.service.ServiceMetricEvent;

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
    if(!toolbox.taskLockCoversSegments(task, segments, allowOlderVersions)) {
      throw new ISE("Segments not covered by locks for task[%s]: %s", task.getId(), segments);
    }

    final Set<DataSegment> retVal = toolbox.getMergerDBCoordinator().announceHistoricalSegments(segments);

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
