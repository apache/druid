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

package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import org.joda.time.Interval;

/**
 * Represents a task that can run on a worker. Immutable.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultMergeTask.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "append", value = AppendTask.class),
    @JsonSubTypes.Type(name = "merge", value = DefaultMergeTask.class),
    @JsonSubTypes.Type(name = "delete", value = DeleteTask.class),
    @JsonSubTypes.Type(name = "kill", value = KillTask.class),
    @JsonSubTypes.Type(name = "index", value = IndexTask.class),
    @JsonSubTypes.Type(name = "index_partitions", value = IndexDeterminePartitionsTask.class),
    @JsonSubTypes.Type(name = "index_generator", value = IndexGeneratorTask.class)
})
public interface Task
{
  /**
   * Returns ID of this task. Must be unique across all tasks ever created.
   */
  public String getId();

  /**
   * Returns group ID of this task. Tasks with the same group ID can share locks. If tasks do not need to share locks,
   * a common convention is to set group ID equal to task ID.
   */
  public String getGroupId();

  /**
   * Returns a descriptive label for this task type. Used for metrics emission and logging.
   */
  public String getType();

  /**
   * Returns the datasource this task operates on. Each task can operate on only one datasource.
   */
  public String getDataSource();

  /**
   * Returns fixed interval for this task, if any. Tasks without fixed intervals are not granted locks when started
   * and must explicitly request them.
   */
  public Optional<Interval> getFixedInterval();

  /**
   * Execute preflight checks for a task. This typically runs on the coordinator, and will be run while
   * holding a lock on our dataSource and interval. If this method throws an exception, the task should be
   * considered a failure.
   *
   * @param toolbox Toolbox for this task
   *
   * @return Some kind of status (runnable means continue on to a worker, non-runnable means we completed without
   *         using a worker).
   *
   * @throws Exception
   */
  public TaskStatus preflight(TaskToolbox toolbox) throws Exception;

  /**
   * Execute a task. This typically runs on a worker as determined by a TaskRunner, and will be run while
   * holding a lock on our dataSource and interval. If this method throws an exception, the task should be
   * considered a failure.
   *
   * @param toolbox Toolbox for this task
   *
   * @return Some kind of finished status (isRunnable must be false).
   *
   * @throws Exception
   */
  public TaskStatus run(TaskToolbox toolbox) throws Exception;
}
