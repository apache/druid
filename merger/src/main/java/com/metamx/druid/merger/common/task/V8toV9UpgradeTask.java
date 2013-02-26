package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
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
  public String getType()
  {
    return "8to9";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    throw new UnsupportedOperationException();
  }
}
