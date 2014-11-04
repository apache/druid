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

package io.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Maps;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import com.metamx.common.RetryUtils;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.MetadataStorageActionHandler;
import io.druid.indexing.overlord.MetadataStorageActionHandlerTypes;
import io.druid.indexing.overlord.TaskExistsException;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SQLMetadataStorageActionHandler<TaskType, TaskStatusType, TaskActionType, TaskLockType>
    implements MetadataStorageActionHandler<TaskType, TaskStatusType, TaskActionType, TaskLockType>
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataStorageActionHandler.class);

  private final IDBI dbi;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig config;
  private final ObjectMapper jsonMapper;
  private final TypeReference taskType;
  private final TypeReference taskStatusType;
  private final TypeReference taskActionType;
  private final TypeReference taskLockType;

  public SQLMetadataStorageActionHandler(
      final IDBI dbi,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig config,
      final ObjectMapper jsonMapper,
      final MetadataStorageActionHandlerTypes<TaskType, TaskStatusType, TaskActionType, TaskLockType> types
  )
  {
    this.dbi = dbi;
    this.connector = connector;
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.taskType = types.getTaskType();
    this.taskStatusType = types.getTaskStatusType();
    this.taskActionType = types.getTaskActionType();
    this.taskLockType = types.getTaskLockType();
  }

  /**
   * Insert stuff
   *
   */
  public void insert(
      final String id,
      final DateTime createdDate,
      final String dataSource,
      final TaskType task,
      final boolean active,
      final TaskStatusType status
  ) throws TaskExistsException
  {
    try {
      retryingHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, created_date, datasource, payload, active, status_payload) VALUES (:id, :created_date, :datasource, :payload, :active, :status_payload)",
                      config.getTasksTable()
                  )
              )
                    .bind("id", id)
                    .bind("created_date", createdDate.toString())
                    .bind("datasource", dataSource)
                    .bind("payload", jsonMapper.writeValueAsBytes(task))
                    .bind("active", active)
                    .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                    .execute();
              return null;
            }
          }
      );
    } catch(Exception e) {
      final boolean isStatementException = e instanceof StatementException ||
                                           (e instanceof CallbackFailedException
                                            && e.getCause() instanceof StatementException);
      if (isStatementException && getTask(id).isPresent()) {
        throw new TaskExistsException(id, e);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Update task status.
   *
   * @return true if status of the task with the given id has been updated successfully
   */
  public boolean setStatus(final String taskId, final boolean active, final TaskStatusType status)
  {
    return retryingHandle(
            new HandleCallback<Boolean>()
            {
              @Override
              public Boolean withHandle(Handle handle) throws Exception
              {
                return handle.createStatement(
                            String.format(
                                "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = 1",
                                config.getTasksTable()
                            )
                        )
                             .bind("id", taskId)
                             .bind("active", active)
                             .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                             .execute() == 1;
              }
            }
        );
  }

  /*  */

  /**
   * Retrieve a task with the given ID
   *
   * @param taskId task ID
   * @return task if it exists
   */
  public Optional<TaskType> getTask(final String taskId)
  {
    return retryingHandle(
        new HandleCallback<Optional<TaskType>>()
        {
          @Override
          public Optional<TaskType> withHandle(Handle handle) throws Exception
          {
            return Optional.fromNullable(
                jsonMapper.<TaskType>readValue(
                    handle.createQuery(
                        String.format("SELECT payload FROM %s WHERE id = :id", config.getTasksTable())
                    )
                          .bind("id", taskId)
                          .map(ByteArrayMapper.FIRST)
                          .first(),
                    taskType
                )
            );
          }
        }
    );

  }

  /* Retrieve a task status with the given ID */
  public Optional<TaskStatusType> getTaskStatus(final String taskId)
  {
    return retryingHandle(
        new HandleCallback<Optional<TaskStatusType>>()
        {
          @Override
          public Optional<TaskStatusType> withHandle(Handle handle) throws Exception
          {
            return Optional.fromNullable(
                jsonMapper.<TaskStatusType>readValue(
                    handle.createQuery(
                        String.format("SELECT status_payload FROM %s WHERE id = :id", config.getTasksTable())
                    )
                          .bind("id", taskId)
                          .map(ByteArrayMapper.FIRST)
                          .first(),
                    taskStatusType
                )
            );
          }
        }
    );
  }

  /* Retrieve active tasks */
  public List<Pair<TaskType, TaskStatusType>> getActiveTasksWithStatus()
  {
    return retryingHandle(
      new HandleCallback<List<Pair<TaskType, TaskStatusType>>>()
      {
        @Override
        public List<Pair<TaskType, TaskStatusType>> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT id, payload, status_payload FROM %s WHERE active = TRUE ORDER BY created_date",
                      config.getTasksTable()
                  )
              )
              .map(
                  new ResultSetMapper<Pair<TaskType, TaskStatusType>>()
                  {
                    @Override
                    public Pair<TaskType, TaskStatusType> map(int index, ResultSet r, StatementContext ctx)
                        throws SQLException
                    {
                      try {
                        return Pair.of(
                            jsonMapper.<TaskType>readValue(
                                r.getBytes("payload"),
                                taskType
                            ),
                            jsonMapper.<TaskStatusType>readValue(
                                r.getBytes("status_payload"),
                                taskStatusType
                            )
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to parse task payload").addData("task", r.getString("id")).emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              ).list();
        }
      }
    );

  }

  /* Retrieve task statuses that have been created sooner than the given time */
  public List<TaskStatusType> getRecentlyFinishedTaskStatuses(final DateTime start)
  {
    return retryingHandle(
      new HandleCallback<List<TaskStatusType>>()
      {
        @Override
        public List<TaskStatusType> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT id, status_payload FROM %s WHERE active = FALSE AND created_date >= :start ORDER BY created_date DESC",
                      config.getTasksTable()
                  )
              ).bind("start", start.toString())
              .map(
                  new ResultSetMapper<TaskStatusType>()
                  {
                    @Override
                    public TaskStatusType map(int index, ResultSet r, StatementContext ctx) throws SQLException
                    {
                      try {
                        return jsonMapper.readValue(
                            r.getBytes("status_payload"),
                            taskStatusType
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to parse status payload")
                           .addData("task", r.getString("id"))
                           .emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              ).list();
        }
      }
    );
  }

  /* Add lock to the task with given ID */
  public int addLock(final String taskId, final TaskLockType lock)
  {
    return retryingHandle(
      new HandleCallback<Integer>()
      {
        @Override
        public Integer withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "INSERT INTO %s (task_id, lock_payload) VALUES (:task_id, :lock_payload)",
                          config.getTaskLockTable()
                      )
                  )
                       .bind("task_id", taskId)
                       .bind("lock_payload", jsonMapper.writeValueAsBytes(lock))
                       .execute();
        }
      }
    );
  }

  /* Remove taskLock with given ID */
  public int removeLock(final long lockId)
  {
    return retryingHandle(
      new HandleCallback<Integer>()
      {
        @Override
        public Integer withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "DELETE FROM %s WHERE id = :id",
                          config.getTaskLockTable()
                      )
                  )
                       .bind("id", lockId)
                       .execute();
        }
      }
    );
  }

  public int addAuditLog(final String taskId, final TaskActionType taskAction)
  {
    return retryingHandle(
      new HandleCallback<Integer>()
      {
        @Override
        public Integer withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "INSERT INTO %s (task_id, log_payload) VALUES (:task_id, :log_payload)",
                          config.getTaskLogTable()
                      )
                  )
                       .bind("task_id", taskId)
                       .bind("log_payload", jsonMapper.writeValueAsBytes(taskAction))
                       .execute();
        }
      }
    );
  }

  /* Get logs for task with given ID */
  public List<TaskActionType> getTaskLogs(final String taskId)
  {
    return retryingHandle(
      new HandleCallback<List<TaskActionType>>()
      {
        @Override
        public List<TaskActionType> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT log_payload FROM %s WHERE task_id = :task_id",
                      config.getTaskLogTable()
                  )
              )
              .bind("task_id", taskId)
              .map(ByteArrayMapper.FIRST)
              .fold(
                  Lists.<TaskActionType>newLinkedList(),
                  new Folder3<List<TaskActionType>, byte[]>()
                  {
                    @Override
                    public List<TaskActionType> fold(
                        List<TaskActionType> list, byte[] bytes, FoldController control, StatementContext ctx
                    ) throws SQLException
                    {
                      try {
                        list.add(
                            jsonMapper.<TaskActionType>readValue(
                                bytes, taskActionType
                            )
                        );
                        return list;
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to deserialize TaskLog")
                           .addData("task", taskId)
                           .addData("logPayload", new String(bytes, Charsets.UTF_8))
                           .emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              );
        }
      }
    );
  }

  /* Get locks for task with given ID */
  public Map<Long, TaskLockType> getTaskLocks(final String taskId)
  {
    return retryingHandle(
      new HandleCallback<Map<Long, TaskLockType>>()
      {
        @Override
        public Map<Long, TaskLockType> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT id, lock_payload FROM %s WHERE task_id = :task_id",
                          config.getTaskLockTable()
                      )
                  )
                       .bind("task_id", taskId)
              .map(
                  new ResultSetMapper<Pair<Long, TaskLockType>>()
                  {
                    @Override
                    public Pair<Long, TaskLockType> map(int index, ResultSet r, StatementContext ctx)
                        throws SQLException
                    {
                      try {
                        return Pair.of(
                            r.getLong("id"),
                            jsonMapper.<TaskLockType>readValue(
                                r.getBytes("lock_payload"),
                                taskLockType
                            )
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to deserialize TaskLock")
                           .addData("task", r.getLong("id"))
                           .addData(
                               "lockPayload", new String(r.getBytes("lock_payload"), Charsets.UTF_8)
                           )
                            .emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              )
              .fold(
                  Maps.<Long, TaskLockType>newLinkedHashMap(),
                  new Folder3<Map<Long, TaskLockType>, Pair<Long, TaskLockType>>()
                  {
                    @Override
                    public Map<Long, TaskLockType> fold(
                        Map<Long, TaskLockType> accumulator,
                        Pair<Long, TaskLockType> lock,
                        FoldController control,
                        StatementContext ctx
                    ) throws SQLException
                    {
                      accumulator.put(lock.lhs, lock.rhs);
                      return accumulator;
                    }
                  }
              );
        }
      }
    );
  }

  private <T> T retryingHandle(final HandleCallback<T> callback)
  {
    final Callable<T> call = new Callable<T>()
    {
      @Override
      public T call() throws Exception
      {
        return dbi.withHandle(callback);
      }
    };
    final Predicate<Throwable> shouldRetry = new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        return shouldRetryException(e);
      }
    };
    final int maxTries = 10;
    try {
      return RetryUtils.retry(call, shouldRetry, maxTries);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected boolean shouldRetryException(final Throwable e)
  {
    return e != null && (e instanceof SQLTransientException
                         || connector.isTransientException(e)
                         || e instanceof SQLRecoverableException
                         || e instanceof UnableToObtainConnectionException
                         || (e instanceof SQLException && shouldRetryException(e.getCause()))
                         || (e instanceof DBIException && shouldRetryException(e.getCause())));
  }

}
