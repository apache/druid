package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public class KillTask extends AbstractTask
{
  private static final Logger log = new Logger(KillTask.class);

  @JsonCreator
  public KillTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        String.format(
            "kill_%s_%s_%s_%s",
            dataSource,
            interval.getStart(),
            interval.getEnd(),
            new DateTime().toString()
        ),
        dataSource,
        interval
    );
  }

  @Override
  public Type getType()
  {
    return Task.Type.KILL;
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox, TaskCallback callback) throws Exception
  {
    // Kill segments
    toolbox.getSegmentKiller()
               .kill(context.getUnusedSegments());

    return TaskStatus.success(getId()).withSegmentsNuked(context.getUnusedSegments());
  }
}
