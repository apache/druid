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

public class SegmentNukeAction implements TaskAction<Void>
{
  @JsonIgnore
  private final Set<DataSegment> segments;

  @JsonCreator
  public SegmentNukeAction(
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

  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>() {};
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    if(!toolbox.taskLockCoversSegments(task, segments, true)) {
      throw new ISE("Segments not covered by locks for task: %s", task.getId());
    }

    toolbox.getMergerDBCoordinator().deleteSegments(segments);

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setUser2(task.getDataSource())
        .setUser4(task.getType());

    for (DataSegment segment : segments) {
      metricBuilder.setUser5(segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("indexer/segmentNuked/bytes", segment.getSize()));
    }

    return null;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentNukeAction{" +
           "segments=" + segments +
           '}';
  }
}
