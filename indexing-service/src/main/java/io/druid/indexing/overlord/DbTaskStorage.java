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
import io.druid.db.DbConnector;
import io.druid.db.DbTablesConfig;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class DbTaskStorage implements TaskStorage
{
  private final ObjectMapper jsonMapper;
  private final DbConnector dbConnector;
  private final DbTablesConfig dbTables;
  private final IDBI dbi;

  private static final EmittingLogger log = new EmittingLogger(DbTaskStorage.class);

  @Inject
  public DbTaskStorage(ObjectMapper jsonMapper, DbConnector dbConnector, DbTablesConfig dbTables, IDBI dbi)
  {
    this.jsonMapper = jsonMapper;
    this.dbConnector = dbConnector;
    this.dbTables = dbTables;
    this.dbi = dbi;
  }

  @LifecycleStart
  public void start()
  {
    dbConnector.createTaskTables();
  }

  @LifecycleStop
  public void stop()
  {
    // do nothing
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
                      "INSERT INTO %s (id, created_date, datasource, payload, active, status_payload) VALUES (:id, :created_date, :datasource, :payload, :active, :status_payload)",
                      dbTables.getTasksTable()
                  )
              )
                    .bind("id", task.getId())
                    .bind("created_date", new DateTime().toString())
                    .bind("datasource", task.getDataSource())
                    .bind("payload", jsonMapper.writeValueAsBytes(task))
                    .bind("active", status.isRunnable() ? 1 : 0)
                    .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                    .execute();

              return null;
            }
          }
      );
    }
    catch (StatementException e) {
      // Might be a duplicate task ID.
      if (getTask(task.getId()).isPresent()) {
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
                    "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = 1",
                    dbTables.getTasksTable()
                )
            )
                         .bind("id", status.getId())
                         .bind("active", status.isRunnable() ? 1 : 0)
                         .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                         .execute();
          }
        }
    );

    if (updated != 1) {
      throw new IllegalStateException(String.format("Active task not found: %s", status.getId()));
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
                        dbTables.getTasksTable()
                    )
                )
                      .bind("id", taskid)
                      .list();

            if (dbTasks.size() == 0) {
              return Optional.absent();
            } else {
              final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbTasks);
              return Optional.of(jsonMapper.readValue((byte[]) dbStatus.get("payload"), Task.class));
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
                        dbTables.getTasksTable()
                    )
                )
                      .bind("id", taskid)
                      .list();

            if (dbStatuses.size() == 0) {
              return Optional.absent();
            } else {
              final Map<String, Object> dbStatus = Iterables.getOnlyElement(dbStatuses);
              return Optional.of(jsonMapper.readValue((byte[]) dbStatus.get("status_payload"), TaskStatus.class));
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
                        "SELECT id, payload, status_payload FROM %s WHERE active = 1",
                        dbTables.getTasksTable()
                    )
                )
                      .list();

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
                    dbTables.getTaskLockTable()
                )
            )
                         .bind("task_id", taskid)
                         .bind("lock_payload", jsonMapper.writeValueAsBytes(taskLock))
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

    for (final Map.Entry<Long, TaskLock> taskLockWithId : taskLocks.entrySet()) {
      final long id = taskLockWithId.getKey();
      final TaskLock taskLock = taskLockWithId.getValue();

      if (taskLock.equals(taskLockToRemove)) {
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
                        dbTables.getTaskLockTable()
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
  public <T> void addAuditLog(final Task task, final TaskAction<T> taskAction)
  {
    Preconditions.checkNotNull(taskAction, "taskAction");

    log.info("Logging action for task[%s]: %s", task.getId(), taskAction);

    dbi.withHandle(
        new HandleCallback<Integer>()
        {
          @Override
          public Integer withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "INSERT INTO %s (task_id, log_payload) VALUES (:task_id, :log_payload)",
                    dbTables.getTaskLogTable()
                )
            )
                         .bind("task_id", task.getId())
                         .bind("log_payload", jsonMapper.writeValueAsBytes(taskAction))
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
                        dbTables.getTaskLogTable()
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
                  return jsonMapper.readValue((byte[]) row.get("log_payload"), TaskAction.class);
                }
                catch (Exception e) {
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
                        dbTables.getTaskLockTable()
                    )
                )
                      .bind("task_id", taskid)
                      .list();

            final Map<Long, TaskLock> retMap = Maps.newHashMap();
            for (final Map<String, Object> row : dbTaskLocks) {
              retMap.put((Long) row.get("id"), jsonMapper.readValue((byte[]) row.get("lock_payload"), TaskLock.class));
            }
            return retMap;
          }
        }
    );
  }
}
