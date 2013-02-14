package com.metamx.druid.merger.common.actions;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Interval;

public class LockAcquireAction implements TaskAction<Optional<TaskLock>>
{
  private final Task task;
  private final Interval interval;

  @JsonCreator
  public LockAcquireAction(
      @JsonProperty("task") Task task,
      @JsonProperty("interval") Interval interval
  )
  {
    this.task = task;
    this.interval = interval;
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
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
  public Optional<TaskLock> perform(TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getTaskLockbox().tryLock(task, interval);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
