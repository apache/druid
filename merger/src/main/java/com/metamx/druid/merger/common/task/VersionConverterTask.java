package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.SpawnTasksAction;
import com.metamx.druid.merger.common.actions.TaskActionClient;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class VersionConverterTask extends AbstractTask
{
  private static final String TYPE = "version_converter";

  public VersionConverterTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        joinId(TYPE, dataSource, interval.getStart(), interval.getEnd(), new DateTime()),
        dataSource,
        interval
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    throw new ISE("Should never run, %s just exists to create subtasks", this.getClass().getSimpleName());
  }

  @Override
  public TaskStatus preflight(TaskToolbox toolbox) throws Exception
  {
    final TaskActionClient taskClient = toolbox.getTaskActionClient();

    List<DataSegment> segments = taskClient.submit(makeListUsedAction());

    taskClient.submit(
        new SpawnTasksAction(
            this,
            Lists.transform(
                segments,
                new Function<DataSegment, Task>()
                {
                  @Override
                  public Task apply(@Nullable DataSegment input)
                  {
                    return new VersionConverterSubTask(getGroupId(), input);
                  }
                }
            )
        )
    );

    return TaskStatus.success(getId());
  }
}
