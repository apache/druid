package com.metamx.druid.merger.common.actions;

import com.google.common.base.Throwables;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Interval;

import java.util.List;

public class SegmentListUsedAction implements TaskAction<List<DataSegment>>
{
  private final Task task;
  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public SegmentListUsedAction(
      @JsonProperty("task") Task task,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    this.task = task;
    this.dataSource = dataSource;
    this.interval = interval;
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
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

  public TypeReference<List<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<List<DataSegment>>() {};
  }

  @Override
  public List<DataSegment> perform(TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getMergerDBCoordinator().getUsedSegmentsForInterval(dataSource, interval);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
