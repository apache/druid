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

package com.metamx.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.metamx.druid.Query;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.TaskToolbox;
import com.metamx.druid.indexing.common.actions.TaskActionClient;
import com.metamx.druid.query.QueryRunner;
import org.joda.time.Interval;

/**
 * Represents a task that can run on a worker. The general contracts surrounding Tasks are:
 * <ul>
 *   <li>Tasks must operate on a single datasource.</li>
 *   <li>Tasks should be immutable, since the task ID is used as a proxy for the task in many locations.</li>
 *   <li>Task IDs must be unique. This can be done by naming them using UUIDs or the current timestamp.</li>
 *   <li>Tasks are each part of a "task group", which is a set of tasks that can share interval locks. These are
 *   useful for producing sharded segments.</li>
 *   <li>Tasks can optionally have an "implicit lock interval". Tasks with this property are guaranteed to have
 *   a lock on that interval during their {@link #preflight(com.metamx.druid.indexing.common.actions.TaskActionClient)}
 *   and {@link #run(com.metamx.druid.indexing.common.TaskToolbox)} methods.</li>
 *   <li>Tasks do not need to explicitly release locks; they are released upon task completion. Tasks may choose
 *   to release locks early if they desire.</li>
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "append", value = AppendTask.class),
    @JsonSubTypes.Type(name = "merge", value = MergeTask.class),
    @JsonSubTypes.Type(name = "delete", value = DeleteTask.class),
    @JsonSubTypes.Type(name = "kill", value = KillTask.class),
    @JsonSubTypes.Type(name = "index", value = IndexTask.class),
    @JsonSubTypes.Type(name = "index_partitions", value = IndexDeterminePartitionsTask.class),
    @JsonSubTypes.Type(name = "index_generator", value = IndexGeneratorTask.class),
    @JsonSubTypes.Type(name = "index_hadoop", value = HadoopIndexTask.class),
    @JsonSubTypes.Type(name = "index_realtime", value = RealtimeIndexTask.class),
    @JsonSubTypes.Type(name = "version_converter", value = VersionConverterTask.class),
    @JsonSubTypes.Type(name = "version_converter_sub", value = VersionConverterTask.SubTask.class)
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
   * Returns availability group ID of this task. Tasks the same availability group cannot be assigned to the same
   * worker. If tasks do not have this restriction, a common convention is to set the availability group ID to the
   * task ID.
   */
  public String getAvailabilityGroup();

  /**
   * Returns a descriptive label for this task type. Used for metrics emission and logging.
   */
  public String getType();

  /**
   * Get the nodeType for if/when this task publishes on zookeeper.
   *
   * @return the nodeType to use when publishing the server to zookeeper. null if the task doesn't expect to
   *         publish to zookeeper.
   */
  public String getNodeType();

  /**
   * Returns the datasource this task operates on. Each task can operate on only one datasource.
   */
  public String getDataSource();

  /**
   * Returns implicit lock interval for this task, if any. Tasks without implicit lock intervals are not granted locks
   * when started and must explicitly request them.
   */
  public Optional<Interval> getImplicitLockInterval();

  /**
   * Returns query runners for this task. If this task is not meant to answer queries over its datasource, this method
   * should return null.
   */
  public <T> QueryRunner<T> getQueryRunner(Query<T> query);

  /**
   * Execute preflight checks for a task. This typically runs on the coordinator, and will be run while
   * holding a lock on our dataSource and implicit lock interval (if any). If this method throws an exception, the
   * task should be considered a failure.
   *
   * @param taskActionClient action client for this task (not the full toolbox)
   *
   * @return Some kind of status (runnable means continue on to a worker, non-runnable means we completed without
   *         using a worker).
   *
   * @throws Exception
   */
  public TaskStatus preflight(TaskActionClient taskActionClient) throws Exception;

  /**
   * Execute a task. This typically runs on a worker as determined by a TaskRunner, and will be run while
   * holding a lock on our dataSource and implicit lock interval (if any). If this method throws an exception, the task
   * should be considered a failure.
   *
   * @param toolbox Toolbox for this task
   *
   * @return Some kind of finished status (isRunnable must be false).
   *
   * @throws Exception
   */
  public TaskStatus run(TaskToolbox toolbox) throws Exception;

  /**
   * Best-effort task cancellation. May or may not do anything. Calling this multiple times may have
   * a stronger effect.
   */
  public void shutdown();
}
