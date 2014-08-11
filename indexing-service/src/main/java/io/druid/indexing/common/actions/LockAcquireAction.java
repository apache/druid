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
import com.google.common.base.Throwables;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
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
    return false;
  }

  @Override
  public String toString()
  {
    return "LockAcquireAction{" +
           "interval=" + interval +
           '}';
  }
}
