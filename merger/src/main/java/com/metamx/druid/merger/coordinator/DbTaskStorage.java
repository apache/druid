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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.actions.TaskAction;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.IndexerDbConnectorConfig;

import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class DbTaskStorage implements TaskStorage
{
  private final ObjectMapper jsonMapper;
  private final IndexerDbConnectorConfig dbConnectorConfig;
  private final DBI dbi;

  private static final Logger log = new Logger(DbTaskStorage.class);

  public DbTaskStorage(ObjectMapper jsonMapper, IndexerDbConnectorConfig dbConnectorConfig, DBI dbi)
  {
    this.jsonMapper = jsonMapper;
    this.dbConnectorConfig = dbConnectorConfig;
    this.dbi = dbi;
  }

  @Override
  public void insert(final Task task, final TaskStatus status)
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
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, created_date, payload, status_code, status_payload) VALUES (:id, :created_date, :payload, :status_code, :status_payload)",
                      dbConnectorConfig.getTaskTable()
                  )
              )
                    .bind("id", task.getId())
                    .bind("created_date", new DateTime().toString())
                    .bind("payload", jsonMapper.writeValueAsString(task))
                    .bind("status_code", status.getStatusCode().toString())
                    .bind("status_payload", jsonMapper.writeValueAsString(status))
                    .execute();

              return null;
            }
          }
      );
    } catch (StatementException e) {
      // Might be a duplicate task ID.
      if(getTask(task.getId()).isPresent()) {
        throw new TaskExistsException(task.getId(), e);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void setStatus(final TaskStatus status)
  {
    Preconditions.checkNotNull(status, "status");

    log.info("Updating task %s to status: %s", status.getId(), status);

    int updated = dbi.withHandle(
        new HandleCallback<Integer>()
        {
          @Override
          public Integer withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "UPDATE %s SET status_code = :status_code, status_payload = :status_payload WHERE id = :id AND status_code = :old_status_code",
                    dbConnectorConfig.getTaskTable()
                )
            )
                  .bind("id", status.getId())
                  .bind("status_code", status.getStatusCode().toString())
                  .bind("old_status_code", TaskStatus.Status.RUNNING.toString())
                  .bind("status_payload", jsonMapper.writeValueAsString(status))
                  .execute();
          }
        }
    );

    if(updated != 1) {
      throw new IllegalStateException(String.format("Running task not found: %s", status.getId()));
    }
  }

  @Override
  public Optional<Task> getTask(final String taskid)
  {
    return dbi.withHandle(
        new HandleCallback<Optional<Task>>()
        {
          @Override
          public Optional<Task> withHandle(Handle handle) throws Exception
          {
            final List<Map<String, Object>> dbTasks =
                handle.createQuery(
                    String.format(
                        "SELECT payload FROM %s WHERE id = :id",
                        dbConnectorConfig.getTaskTable()
                    )
                )
                      .bind("id", taskid)
                      .list();

            if(dbTasks.size() == 0) {
              return Optional.absent();
            } else {
              final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbTasks);
              return Optional.of(jsonMapper.readValue(dbStatus.get("payload").toString(), Task.class));
            }
          }
        }
    );
  }

  @Override
  public Optional<TaskStatus> getStatus(final String taskid)
  {
    return dbi.withHandle(
        new HandleCallback<Optional<TaskStatus>>()
        {
          @Override
          public Optional<TaskStatus> withHandle(Handle handle) throws Exception
          {
            final List<Map<String, Object>> dbStatuses =
                handle.createQuery(
                    String.format(
                        "SELECT status_payload FROM %s WHERE id = :id",
                        dbConnectorConfig.getTaskTable()
                    )
                )
                      .bind("id", taskid)
                      .list();

            if(dbStatuses.size() == 0) {
              return Optional.absent();
            } else {
              final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbStatuses);
              return Optional.of(jsonMapper.readValue(dbStatus.get("status_payload").toString(), TaskStatus.class));
            }
          }
        }
    );
  }

  @Override
  public List<Task> getRunningTasks()
  {
    return dbi.withHandle(
        new HandleCallback<List<Task>>()
        {
          @Override
          public List<Task> withHandle(Handle handle) throws Exception
          {
            final List<Map<String, Object>> dbTasks =
                handle.createQuery(
                    String.format(
                        "SELECT payload FROM %s WHERE status_code = :status_code",
                        dbConnectorConfig.getTaskTable()
                    )
                )
                      .bind("status_code", TaskStatus.Status.RUNNING.toString())
                      .list();

            return Lists.transform(
                dbTasks, new Function<Map<String, Object>, Task>()
            {
              @Override
              public Task apply(Map<String, Object> row)
              {
                try {
                  return jsonMapper.readValue(row.get("payload").toString(), Task.class);
                } catch(Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
            );
          }
        }
    );
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

    dbi.withHandle(
        new HandleCallback<Integer>()
        {
          @Override
          public Integer withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "INSERT INTO %s (task_id, lock_payload) VALUES (:task_id, :lock_payload)",
                    dbConnectorConfig.getTaskLockTable()
                )
            )
                         .bind("task_id", taskid)
                         .bind("lock_payload", jsonMapper.writeValueAsString(taskLock))
                         .execute();
          }
        }
    );
  }

  @Override
  public void removeLock(String taskid, TaskLock taskLockToRemove)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLockToRemove, "taskLockToRemove");

    final Map<Long, TaskLock> taskLocks = getLocksWithIds(taskid);

    for(final Map.Entry<Long, TaskLock> taskLockWithId : taskLocks.entrySet()) {
      final long id = taskLockWithId.getKey();
      final TaskLock taskLock = taskLockWithId.getValue();

      if(taskLock.equals(taskLockToRemove)) {
        log.info("Deleting TaskLock with id[%d]: %s", id, taskLock);

        dbi.withHandle(
            new HandleCallback<Integer>()
            {
              @Override
              public Integer withHandle(Handle handle) throws Exception
              {
                return handle.createStatement(
                    String.format(
                        "DELETE FROM %s WHERE id = :id",
                        dbConnectorConfig.getTaskLockTable()
                    )
                )
                             .bind("id", id)
                             .execute();
              }
            }
        );
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
  public <T> void addAuditLog(final TaskAction<T> taskAction)
  {
    Preconditions.checkNotNull(taskAction, "taskAction");

    log.info("Logging action for task[%s]: %s", taskAction.getTask().getId(), taskAction);

    dbi.withHandle(
        new HandleCallback<Integer>()
        {
          @Override
          public Integer withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "INSERT INTO %s (task_id, log_payload) VALUES (:task_id, :log_payload)",
                    dbConnectorConfig.getTaskLogTable()
                )
            )
                         .bind("task_id", taskAction.getTask().getId())
                         .bind("log_payload", jsonMapper.writeValueAsString(taskAction))
                         .execute();
          }
        }
    );
  }

  @Override
  public List<TaskAction> getAuditLogs(final String taskid)
  {
    return dbi.withHandle(
        new HandleCallback<List<TaskAction>>()
        {
          @Override
          public List<TaskAction> withHandle(Handle handle) throws Exception
          {
            final List<Map<String, Object>> dbTaskLogs =
                handle.createQuery(
                    String.format(
                        "SELECT log_payload FROM %s WHERE task_id = :task_id",
                        dbConnectorConfig.getTaskLogTable()
                    )
                )
                      .bind("task_id", taskid)
                      .list();

            return Lists.transform(
                dbTaskLogs, new Function<Map<String, Object>, TaskAction>()
            {
              @Override
              public TaskAction apply(Map<String, Object> row)
              {
                try {
                  return jsonMapper.readValue(row.get("payload").toString(), TaskAction.class);
                } catch(Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
            );
          }
        }
    );
  }

  private Map<Long, TaskLock> getLocksWithIds(final String taskid)
  {
    return dbi.withHandle(
        new HandleCallback<Map<Long, TaskLock>>()
        {
          @Override
          public Map<Long, TaskLock> withHandle(Handle handle) throws Exception
          {
            final List<Map<String, Object>> dbTaskLocks =
                handle.createQuery(
                    String.format(
                        "SELECT id, lock_payload FROM %s WHERE task_id = :task_id",
                        dbConnectorConfig.getTaskLockTable()
                    )
                )
                      .bind("task_id", taskid)
                      .list();

            final Map<Long, TaskLock> retMap = Maps.newHashMap();
            for(final Map<String, Object> row : dbTaskLocks) {
              retMap.put((Long)row.get("id"), jsonMapper.readValue(row.get("lock_payload").toString(), TaskLock.class));
            }
            return retMap;
          }
        }
    );
  }
}
