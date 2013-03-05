package com.metamx.druid.merger.common.actions;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class SpawnTasksAction implements TaskAction<Void>
{
  private final List<Task> newTasks;

  @JsonCreator
  public SpawnTasksAction(
      @JsonProperty("newTasks") List<Task> newTasks
  )
  {
    this.newTasks = ImmutableList.copyOf(newTasks);
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
  public Void perform(Task task, TaskActionToolbox toolbox)
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
