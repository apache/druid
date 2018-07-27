/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.Query;
import io.druid.query.QueryRunner;

import java.util.Map;

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
    @JsonSubTypes.Type(name = "kill", value = KillTask.class),
    @JsonSubTypes.Type(name = "move", value = MoveTask.class),
    @JsonSubTypes.Type(name = "archive", value = ArchiveTask.class),
    @JsonSubTypes.Type(name = "restore", value = RestoreTask.class),
    @JsonSubTypes.Type(name = "index", value = IndexTask.class),
    @JsonSubTypes.Type(name = "index_hadoop", value = HadoopIndexTask.class),
    @JsonSubTypes.Type(name = "hadoop_convert_segment", value = HadoopConverterTask.class),
    @JsonSubTypes.Type(name = "hadoop_convert_segment_sub", value = HadoopConverterTask.ConverterSubTask.class),
    @JsonSubTypes.Type(name = "index_realtime", value = RealtimeIndexTask.class),
    @JsonSubTypes.Type(name = "noop", value = NoopTask.class),
    @JsonSubTypes.Type(name = "version_converter", value = ConvertSegmentBackwardsCompatibleTask.class), // Backwards compat - Deprecated
    @JsonSubTypes.Type(name = "version_converter_sub", value = ConvertSegmentBackwardsCompatibleTask.SubTask.class), // backwards compat - Deprecated
    @JsonSubTypes.Type(name = "convert_segment", value = ConvertSegmentTask.class),
    @JsonSubTypes.Type(name = "convert_segment_sub", value = ConvertSegmentTask.SubTask.class),
    @JsonSubTypes.Type(name = "same_interval_merge", value = SameIntervalMergeTask.class),
    @JsonSubTypes.Type(name = "compact", value = CompactionTask.class)
})
public interface Task
{
  /**
   * Returns ID of this task. Must be unique across all tasks ever created.
   *
   * @return task ID
   */
  String getId();

  /**
   * Returns group ID of this task. Tasks with the same group ID can share locks. If tasks do not need to share locks,
   * a common convention is to set group ID equal to task ID.
   *
   * @return task group ID
   */
  String getGroupId();

  /**
   * Returns task priority. The task priority is currently used only for prioritized locking, but, in the future, it can
   * be used for task scheduling, cluster resource management, etc.
   *
   * The task priority must be in taskContext if the task is submitted to the proper Overlord endpoint.
   *
   * It might not be in taskContext in rolling update. This returns {@link Tasks#DEFAULT_TASK_PRIORITY} in this case.
   *
   * @return task priority
   *
   * @see Tasks for default task priorities
   * @see io.druid.indexing.overlord.http.OverlordResource#taskPost
   */
  default int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_TASK_PRIORITY);
  }

  /**
   * Returns the default task priority. It can vary depending on the task type.
   */
  default int getDefaultPriority()
  {
    return Tasks.DEFAULT_TASK_PRIORITY;
  }

  /**
   * Returns a {@link TaskResource} for this task. Task resources define specific worker requirements a task may
   * require.
   *
   * @return {@link TaskResource} for this task
   */
  TaskResource getTaskResource();

  /**
   * Returns a descriptive label for this task type. Used for metrics emission and logging.
   *
   * @return task type label
   */
  String getType();

  /**
   * Get the nodeType for if/when this task publishes on zookeeper.
   *
   * @return the nodeType to use when publishing the server to zookeeper. null if the task doesn't expect to
   * publish to zookeeper.
   */
  String getNodeType();

  /**
   * Returns the datasource this task operates on. Each task can operate on only one datasource.
   */
  String getDataSource();

  /**
   * Returns query runners for this task. If this task is not meant to answer queries over its datasource, this method
   * should return null.
   *
   * @param <T> query result type
   *
   * @return query runners for this task
   */
  <T> QueryRunner<T> getQueryRunner(Query<T> query);

  /**
   * Returns an extra classpath that should be prepended to the default classpath when running this task. If no
   * extra classpath should be prepended, this should return null or the empty string.
   */
  String getClasspathPrefix();

  /**
   * Execute preflight actions for a task. This can be used to acquire locks, check preconditions, and so on. The
   * actions must be idempotent, since this method may be executed multiple times. This typically runs on the
   * coordinator. If this method throws an exception, the task should be considered a failure.
   * <p/>
   * This method must be idempotent, as it may be run multiple times per task.
   *
   * @param taskActionClient action client for this task (not the full toolbox)
   *
   * @return true if ready, false if not ready yet
   *
   * @throws Exception if the task should be considered a failure
   */
  boolean isReady(TaskActionClient taskActionClient) throws Exception;

  /**
   * Returns whether or not this task can restore its progress from its on-disk working directory. Restorable tasks
   * may be started with a non-empty working directory. Tasks that exit uncleanly may still have a chance to attempt
   * restores, meaning that restorable tasks should be able to deal with potentially partially written on-disk state.
   */
  boolean canRestore();

  /**
   * Asks a task to arrange for its "run" method to exit promptly. This method will only be called if
   * {@link #canRestore()} returns true. Tasks that take too long to stop gracefully will be terminated with
   * extreme prejudice.
   */
  void stopGracefully();

  /**
   * Execute a task. This typically runs on a worker as determined by a TaskRunner, and will be run while
   * holding a lock on our dataSource and implicit lock interval (if any). If this method throws an exception, the task
   * should be considered a failure.
   *
   * @param toolbox Toolbox for this task
   *
   * @return Some kind of finished status (isRunnable must be false).
   *
   * @throws Exception if this task failed
   */
  TaskStatus run(TaskToolbox toolbox) throws Exception;

  default Map<String, Object> addToContext(String key, Object val)
  {
    getContext().put(key, val);
    return getContext();
  }

  Map<String, Object> getContext();

  default <ContextValueType> ContextValueType getContextValue(String key)
  {
    return (ContextValueType) getContext().get(key);
  }

  default <ContextValueType> ContextValueType getContextValue(String key, ContextValueType defaultValue)
  {
    final ContextValueType value = getContextValue(key);
    return value == null ? defaultValue : value;
  }
}
