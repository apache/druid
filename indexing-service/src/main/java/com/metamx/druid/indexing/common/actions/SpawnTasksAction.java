package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.indexing.common.task.Task;

import java.util.List;

public class SpawnTasksAction implements TaskAction<Void>
{
  @JsonIgnore
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
    for(final Task newTask : newTasks) {
      toolbox.getTaskQueue().add(newTask);
    }

    return null;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SpawnTasksAction{" +
           "newTasks=" + newTasks +
           '}';
  }
}
