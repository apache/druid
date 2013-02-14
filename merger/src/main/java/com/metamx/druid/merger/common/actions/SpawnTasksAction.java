package com.metamx.druid.merger.common.actions;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.type.TypeReference;

import java.util.List;

public class SpawnTasksAction implements TaskAction<Void>
{
  private final Task task;
  private final List<Task> newTasks;

  @JsonCreator
  public SpawnTasksAction(
      @JsonProperty("task") Task task,
      @JsonProperty("newTasks") List<Task> newTasks
  )
  {
    this.task = task;
    this.newTasks = ImmutableList.copyOf(newTasks);
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
  }

  @JsonProperty
  public List<Task> getNewTasks()
  {
    return newTasks;
  }

  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>() {};
  }

  @Override
  public Void perform(TaskActionToolbox toolbox)
  {
    try {
      for(final Task newTask : newTasks) {
        toolbox.getTaskQueue().add(newTask);
      }

      return null;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
