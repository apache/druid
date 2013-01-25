package com.metamx.druid.merger.common.task;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

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
    Set<DataSegment> segmentsToKill = ImmutableSet.copyOf(
        toolbox.getSegmentKiller()
               .kill(getDataSource(), getInterval())
    );

    return TaskStatus.success(getId(), segmentsToKill);
  }
}
