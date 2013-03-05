package com.metamx.druid.merger.common.actions;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.Interval;

public class LockAcquireAction implements TaskAction<Optional<TaskLock>>
{
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

  public TypeReference<Optional<TaskLock>> getReturnTypeReference()
  {
    return new TypeReference<Optional<TaskLock>>() {};
  }

  @Override
  public Optional<TaskLock> perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getTaskLockbox().tryLock(task, interval);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
