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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.Query;
import io.druid.query.QueryRunner;

/**
 * Represents a task that can run on a worker. The general contracts surrounding Tasks are:
 * <ul>
 * <li>Tasks must operate on a single datasource.</li>
 * <li>Tasks should be immutable, since the task ID is used as a proxy for the task in many locations.</li>
 * <li>Task IDs must be unique. This can be done by naming them using UUIDs or the current timestamp.</li>
 * <li>Tasks are each part of a "task group", which is a set of tasks that can share interval locks. These are
 * useful for producing sharded segments.</li>
 * <li>Tasks do not need to explicitly release locks; they are released upon task completion. Tasks may choose
 * to release locks early if they desire.</li>
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "append", value = AppendTask.class),
    @JsonSubTypes.Type(name = "merge", value = MergeTask.class),
    @JsonSubTypes.Type(name = "delete", value = DeleteTask.class),
    @JsonSubTypes.Type(name = "kill", value = KillTask.class),
    @JsonSubTypes.Type(name = "move", value = MoveTask.class),
    @JsonSubTypes.Type(name = "archive", value = ArchiveTask.class),
    @JsonSubTypes.Type(name = "restore", value = RestoreTask.class),
    @JsonSubTypes.Type(name = "index", value = IndexTask.class),
    @JsonSubTypes.Type(name = "index_hadoop", value = HadoopIndexTask.class),
    @JsonSubTypes.Type(name = "index_realtime", value = RealtimeIndexTask.class),
    @JsonSubTypes.Type(name = "noop", value = NoopTask.class),
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
   * Returns a {@link io.druid.indexing.common.task.TaskResource} for this task. Task resources define specific
   * worker requirements a task may require.
   */
  public TaskResource getTaskResource();

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
   * Returns query runners for this task. If this task is not meant to answer queries over its datasource, this method
   * should return null.
   */
  public <T> QueryRunner<T> getQueryRunner(Query<T> query);

  /**
   * Execute preflight actions for a task. This can be used to acquire locks, check preconditions, and so on. The
   * actions must be idempotent, since this method may be executed multiple times. This typically runs on the
   * coordinator. If this method throws an exception, the task should be considered a failure.
   *
   * This method must be idempotent, as it may be run multiple times per task.
   *
   * @param taskActionClient action client for this task (not the full toolbox)
   *
   * @return true if ready, false if not ready yet
   *
   * @throws Exception if the task should be considered a failure
   */
  public boolean isReady(TaskActionClient taskActionClient) throws Exception;

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
}
