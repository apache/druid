package com.metamx.druid.merger.common.task;

import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
public class V8toV9UpgradeTask extends AbstractTask
{
  public V8toV9UpgradeTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        String.format("v8tov9_%s_%s_%s", dataSource, interval.toString().replace("/", "_"), new DateTime()),
        dataSource,
        interval
    );
  }

  @Override
  public Type getType()
  {
    throw new UnsupportedOperationException("Do we really need to return a Type?");
  }

  @Override
  public TaskStatus run(
      TaskContext context, TaskToolbox toolbox, TaskCallback callback
  ) throws Exception
  {
    throw new UnsupportedOperationException();
  }
}
