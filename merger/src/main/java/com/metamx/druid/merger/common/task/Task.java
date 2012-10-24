package com.metamx.druid.merger.common.task;

import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.merger.common.task.IndexDeterminePartitionsTask;
import com.metamx.druid.merger.common.task.IndexGeneratorTask;
import com.metamx.druid.merger.common.task.IndexTask;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Represents a task that can run on a worker. Immutable.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultMergeTask.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "append", value = AppendTask.class),
    @JsonSubTypes.Type(name = "delete", value = DeleteTask.class),
    @JsonSubTypes.Type(name = "index", value = IndexTask.class),
    @JsonSubTypes.Type(name = "index_partitions", value = IndexDeterminePartitionsTask.class),
    @JsonSubTypes.Type(name = "index_generator", value = IndexGeneratorTask.class)
})
public interface Task
{
  enum Type
  {
    INDEX,
    MERGE,
    APPEND,
    DELETE,
    TEST
  }

  public String getId();

  public String getGroupId();

  public Type getType();

  public String getDataSource();

  public Interval getInterval();

  /**
   * Execute preflight checks for a task. This typically runs on the coordinator, and will be run while
   * holding a lock on our dataSouce and interval. If this method throws an exception, the task should be
   * considered a failure.
   *
   * @param context Context for this task, gathered under indexer lock
   * @return Some kind of status (runnable means continue on to a worker, non-runnable means we completed without
   * using a worker).
   * @throws Exception
   */
  public TaskStatus preflight(TaskContext context) throws Exception;

  /**
   * Execute a task. This typically runs on a worker as determined by a TaskRunner, and will be run while
   * holding a lock on our dataSource and interval. If this method throws an exception, the task should be
   * considered a failure.
   *
   * @param context Context for this task, gathered under indexer lock
   * @param toolbox Toolbox for this task
   * @return Some kind of finished status (isRunnable must be false).
   * @throws Exception
   */
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception;
}
