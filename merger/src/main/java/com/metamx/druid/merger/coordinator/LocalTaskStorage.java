package com.metamx.druid.merger.coordinator;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Implements an in-heap TaskStorage facility, with no persistence across restarts. This class is not
 * thread safe.
 */
public class LocalTaskStorage implements TaskStorage
{
  private final Map<String, TaskStuff> tasks = Maps.newHashMap();

  private static final Logger log = new Logger(LocalTaskStorage.class);

  @Override
  public void insert(Task task, TaskStatus status)
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(
        task.getId().equals(status.getId()),
        "Task/Status ID mismatch[%s/%s]",
        task.getId(),
        status.getId()
    );
    Preconditions.checkState(!tasks.containsKey(task.getId()), "Task ID must not already be present: %s", task.getId());
    log.info("Inserting task %s with status: %s", task.getId(), status);
    tasks.put(task.getId(), new TaskStuff(task, status));
  }

  @Override
  public void setStatus(String taskid, TaskStatus status)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkState(tasks.containsKey(taskid), "Task ID must already be present: %s", taskid);
    log.info("Updating task %s to status: %s", taskid, status);
    tasks.put(taskid, tasks.get(taskid).withStatus(status));
  }

  @Override
  public Optional<TaskStatus> getStatus(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    if(tasks.containsKey(taskid)) {
      return Optional.of(tasks.get(taskid).status);
    } else {
      return Optional.absent();
    }
  }

  @Override
  public void setVersion(String taskid, String version)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(version, "status");
    Preconditions.checkState(tasks.containsKey(taskid), "Task ID must already be present: %s", taskid);
    log.info("Updating task %s to version: %s", taskid, version);
    tasks.put(taskid, tasks.get(taskid).withVersion(version));
  }

  @Override
  public Optional<String> getVersion(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    if(tasks.containsKey(taskid)) {
      return tasks.get(taskid).version;
    } else {
      return Optional.absent();
    }
  }

  @Override
  public List<Task> getRunningTasks()
  {
    final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
    for(final TaskStuff taskStuff : tasks.values()) {
      if(taskStuff.status.isRunnable()) {
        listBuilder.add(taskStuff.task);
      }
    }

    return listBuilder.build();
  }

  private static class TaskStuff
  {
    final Task task;
    final TaskStatus status;
    final Optional<String> version;

    private TaskStuff(Task task, TaskStatus status)
    {
      this(task, status, Optional.<String>absent());
    }

    private TaskStuff(Task task, TaskStatus status, Optional<String> version)
    {
      Preconditions.checkNotNull(task);
      Preconditions.checkNotNull(status);
      Preconditions.checkArgument(task.getId().equals(status.getId()));

      this.task = task;
      this.status = status;
      this.version = version;
    }

    private TaskStuff withStatus(TaskStatus _status)
    {
      return new TaskStuff(task, _status, version);
    }

    private TaskStuff withVersion(String _version)
    {
      return new TaskStuff(task, status, Optional.of(_version));
    }
  }
}
