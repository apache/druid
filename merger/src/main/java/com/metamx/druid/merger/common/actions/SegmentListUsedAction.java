package com.metamx.druid.merger.common.actions;

import com.google.common.base.Throwables;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.Interval;

import java.util.List;

public class SegmentListUsedAction implements TaskAction<List<DataSegment>>
{
  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public SegmentListUsedAction(
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

  public TypeReference<List<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<List<DataSegment>>() {};
  }

  @Override
  public List<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getMergerDBCoordinator().getUsedSegmentsForInterval(dataSource, interval);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
