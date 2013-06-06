package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import com.metamx.druid.indexing.common.TaskLock;
import com.metamx.druid.indexing.common.task.Task;
import org.joda.time.Interval;

public class LockAcquireAction implements TaskAction<TaskLock>
{
  @JsonIgnore
  private final Interval interval;

  @JsonCreator
  public LockAcquireAction(
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

  public TypeReference<TaskLock> getReturnTypeReference()
  {
    return new TypeReference<TaskLock>()
    {
    };
  }

  @Override
  public TaskLock perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getTaskLockbox().lock(task, interval);
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "LockAcquireAction{" +
           "interval=" + interval +
           '}';
  }
}
