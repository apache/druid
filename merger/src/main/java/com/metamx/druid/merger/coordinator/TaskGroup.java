/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator;

import java.util.Set;
import java.util.TreeSet;

import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.druid.merger.common.task.Task;

/**
 * Represents a transaction as well as the lock it holds. Not immutable: the task set can change.
 */
public class TaskGroup
{
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final Set<Task> taskSet = new TreeSet<Task>(
      new Ordering<Task>()
      {
        @Override
        public int compare(Task task, Task task1)
        {
          return task.getId().compareTo(task1.getId());
        }
      }.nullsFirst()
  );

  public TaskGroup(String groupId, String dataSource, Interval interval, String version)
  {
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public String getVersion()
  {
    return version;
  }

  public Set<Task> getTaskSet()
  {
    return taskSet;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .add(
                      "taskSet",
                      Lists.newArrayList(
                          Iterables.transform(
                              taskSet, new Function<Task, Object>()
                          {
                            @Override
                            public Object apply(Task task)
                            {
                              return task.getId();
                            }
                          }
                          )
                      )
                  )
                  .toString();
  }
}
