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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.db.MetadataStorageConnector;
import io.druid.db.MetadataStorageTablesConfig;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.StatementException;

import java.util.List;
import java.util.Map;

public class MetadataTaskStorage implements TaskStorage
{
  private final ObjectMapper jsonMapper;
  private final MetadataStorageConnector metadataStorageConnector;
  private final MetadataStorageTablesConfig dbTables;
  private final TaskStorageConfig config;
  private final MetadataStorageActionHandler handler;

  private static final EmittingLogger log = new EmittingLogger(MetadataTaskStorage.class);

  @Inject
  public MetadataTaskStorage(
      final ObjectMapper jsonMapper,
      final MetadataStorageConnector metadataStorageConnector,
      final MetadataStorageTablesConfig dbTables,
      final TaskStorageConfig config,
      final MetadataStorageActionHandler handler
  )
  {
    this.jsonMapper = jsonMapper;
    this.metadataStorageConnector = metadataStorageConnector;
    this.dbTables = dbTables;
    this.config = config;
    this.handler = handler;
  }

  @LifecycleStart
  public void start()
  {
    metadataStorageConnector.createTaskTables();
  }

  @LifecycleStop
  public void stop()
  {
    // do nothing
  }

  @Override
  public void insert(final Task task, final TaskStatus status) throws TaskExistsException
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(
        task.getId().equals(status.getId()),
        "Task/Status ID mismatch[%s/%s]",
        task.getId(),
        status.getId()
    );

    log.info("Inserting task %s with status: %s", task.getId(), status);

    try {
      handler.insert(
          dbTables.getTasksTable(),
          task.getId(),
          new DateTime().toString(),
          task.getDataSource(),
          jsonMapper.writeValueAsBytes(task),
          status.isRunnable() ? 1 : 0,
          jsonMapper.writeValueAsBytes(status)
      );
    }
    catch (Exception e) {
      final boolean isStatementException =  e instanceof StatementException ||
                                            (e instanceof CallbackFailedException
                                             && e.getCause() instanceof StatementException);
      if (isStatementException && getTask(task.getId()).isPresent()) {
        throw new TaskExistsException(task.getId(), e);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void setStatus(final TaskStatus status)
  {
    Preconditions.checkNotNull(status, "status");

    log.info("Updating task %s to status: %s", status.getId(), status);

    try {
      int updated = handler.setStatus(
          dbTables.getTasksTable(),
          status.getId(),
          status.isRunnable() ? 1 : 0,
          jsonMapper.writeValueAsBytes(status)
      );
      if (updated != 1) {
        throw new IllegalStateException(String.format("Active task not found: %s", status.getId()));
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Optional<Task> getTask(final String taskid)
  {
    try {
      final List<Map<String, Object>> dbTasks = handler.getTask(dbTables.getTasksTable(), taskid);
      if (dbTasks.size() == 0) {
        return Optional.absent();
      } else {
        final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbTasks);
        return Optional.of(jsonMapper.readValue((byte[]) dbStatus.get("payload"), Task.class));
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Optional<TaskStatus> getStatus(final String taskid)
  {
    try {
      final List<Map<String, Object>> dbStatuses = handler.getTaskStatus(dbTables.getTasksTable(), taskid);
      if (dbStatuses.size() == 0) {
        return Optional.absent();
      } else {
        final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbStatuses);
        return Optional.of(jsonMapper.readValue((byte[]) dbStatus.get("status_payload"), TaskStatus.class));
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<Task> getActiveTasks()
  {
    final List<Map<String, Object>> dbTasks = handler.getActiveTasks(dbTables.getTasksTable());

    final ImmutableList.Builder<Task> tasks = ImmutableList.builder();
    for (final Map<String, Object> row : dbTasks) {
     final String id = row.get("id").toString();

     try {
       final Task task = jsonMapper.readValue((byte[]) row.get("payload"), Task.class);
       final TaskStatus status = jsonMapper.readValue((byte[]) row.get("status_payload"), TaskStatus.class);

       if (status.isRunnable()) {
         tasks.add(task);
       }
     }
     catch (Exception e) {
       log.makeAlert(e, "Failed to parse task payload").addData("task", id).emit();
     }
    }

    return tasks.build();
  }

  @Override
  public List<TaskStatus> getRecentlyFinishedTaskStatuses()
  {
    final DateTime recent = new DateTime().minus(config.getRecentlyFinishedThreshold());

    final List<Map<String, Object>> dbTasks = handler.getRecentlyFinishedTaskStatuses(dbTables.getTasksTable(), recent.toString());
    final ImmutableList.Builder<TaskStatus> statuses = ImmutableList.builder();
    for (final Map<String, Object> row : dbTasks) {
      final String id = row.get("id").toString();

      try {
        final TaskStatus status = jsonMapper.readValue((byte[]) row.get("status_payload"), TaskStatus.class);
        if (status.isComplete()) {
          statuses.add(status);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to parse status payload").addData("task", id).emit();
      }
    }

    return statuses.build();
  }

  @Override
  public void addLock(final String taskid, final TaskLock taskLock)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLock, "taskLock");

    log.info(
        "Adding lock on interval[%s] version[%s] for task: %s",
        taskLock.getInterval(),
        taskLock.getVersion(),
        taskid
    );

    try {
      handler.addLock(dbTables.getTaskLockTable(), taskid, jsonMapper.writeValueAsBytes(taskLock));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }


  }

  @Override
  public void removeLock(String taskid, TaskLock taskLockToRemove)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLockToRemove, "taskLockToRemove");

    final Map<Long, TaskLock> taskLocks = getLocksWithIds(taskid);

    for (final Map.Entry<Long, TaskLock> taskLockWithId : taskLocks.entrySet()) {
      final long id = taskLockWithId.getKey();
      final TaskLock taskLock = taskLockWithId.getValue();

      if (taskLock.equals(taskLockToRemove)) {
        log.info("Deleting TaskLock with id[%d]: %s", id, taskLock);
        handler.removeLock(dbTables.getTaskLockTable(), id);
      }
    }
  }

  @Override
  public List<TaskLock> getLocks(String taskid)
  {
    return ImmutableList.copyOf(
        Iterables.transform(
            getLocksWithIds(taskid).entrySet(), new Function<Map.Entry<Long, TaskLock>, TaskLock>()
            {
              @Override
              public TaskLock apply(Map.Entry<Long, TaskLock> e)
              {
                return e.getValue();
              }
            }
        )
    );
  }

  @Override
  public <T> void addAuditLog(final Task task, final TaskAction<T> taskAction)
  {
    Preconditions.checkNotNull(taskAction, "taskAction");

    log.info("Logging action for task[%s]: %s", task.getId(), taskAction);

    try {
      handler.addAuditLog(dbTables.getTaskLogTable(), task.getId(), jsonMapper.writeValueAsBytes(taskAction));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<TaskAction> getAuditLogs(final String taskid)
  {
    final List<Map<String, Object>> dbTaskLogs = handler.getTaskLogs(dbTables.getTaskLogTable(), taskid);
    final List<TaskAction> retList = Lists.newArrayList();
    for (final Map<String, Object> dbTaskLog : dbTaskLogs) {
      try {
        retList.add(jsonMapper.readValue((byte[]) dbTaskLog.get("log_payload"), TaskAction.class));
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to deserialize TaskLog")
           .addData("task", taskid)
           .addData("logPayload", dbTaskLog)
           .emit();
      }
    }
    return retList;
  }

  private Map<Long, TaskLock> getLocksWithIds(final String taskid)
  {
    final List<Map<String, Object>> dbTaskLocks = handler.getTaskLocks(dbTables.getTaskLockTable(), taskid);

    final Map<Long, TaskLock> retMap = Maps.newHashMap();
    for (final Map<String, Object> row : dbTaskLocks) {
      try {
        retMap.put(
            (Long) row.get("id"),
            jsonMapper.readValue((byte[]) row.get("lock_payload"), TaskLock.class)
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to deserialize TaskLock")
           .addData("task", taskid)
           .addData("lockPayload", row)
           .emit();
      }
    }
    return retMap;
  }
}