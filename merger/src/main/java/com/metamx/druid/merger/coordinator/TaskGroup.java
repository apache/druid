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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.commit.CommitStyle;
import com.metamx.druid.merger.coordinator.commit.ImmediateCommitStyle;
import org.joda.time.Interval;

import java.util.Map;

/**
 * Represents a transaction as well as the lock it holds. Not immutable: the task set can change.
 */
public class TaskGroup
{
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final Map<String, Task> taskMap = Maps.newHashMap();

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

  public CommitStyle getCommitStyle()
  {
    // TODO -- should be configurable
    return new ImmediateCommitStyle();
  }

  /**
   * Returns number of tasks in this group.
   */
  public int size() {
    return taskMap.size();
  }

  /**
   * Adds a task to this group.
   * @param task task to add
   * @return true iff this group did not already contain the task
   */
  public boolean add(final Task task) {
    Preconditions.checkArgument(
        task.getGroupId().equals(groupId),
        "Task group id[%s] != TaskGroup group id[%s]",
        task.getGroupId(),
        groupId
    );

    if(taskMap.containsKey(task.getId())) {
      return false;
    } else {
      taskMap.put(task.getId(), task);
      return true;
    }
  }

  /**
   * Returns true if this group contains a particular task.
   */
  public boolean contains(final String taskId) {
    return taskMap.containsKey(taskId);
  }

  /**
   * Removes a task from this group.
   * @param taskId task ID to remove
   * @return the removed task, or null if the task was not in this group
   */
  public Task remove(final String taskId)
  {
    return taskMap.remove(taskId);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .add("tasks", taskMap.keySet())
                  .toString();
  }
}
