package com.metamx.druid.merger.common.actions;

import com.google.common.base.Throwables;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.Interval;

import java.util.List;

public class LockReleaseAction implements TaskAction<Void>
{
  private final Interval interval;

  @JsonCreator
  public LockReleaseAction(
      @JsonProperty("interval") Interval interval
  )
  {
    this.interval = interval;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>() {};
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    toolbox.getTaskLockbox().unlock(task, interval);
    return null;
  }

  @Override
  public String toString()
  {
    return "LockReleaseAction{" +
           "interval=" + interval +
           '}';
  }
}
