/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import io.druid.indexing.common.task.Task;

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
