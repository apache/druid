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

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.RetryUtils;
import com.mysql.jdbc.exceptions.MySQLTransientException;
import io.druid.indexing.overlord.MetadataStorageActionHandler;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SQLMetadataStorageActionHandler implements MetadataStorageActionHandler
{
  private final IDBI dbi;

  @Inject
  public SQLMetadataStorageActionHandler(final IDBI dbi)
  {
    this.dbi = dbi;
  }

  /* Insert stuff. @returns number of entries inserted on success */
  public void insert(
      final String tableName,
      final String id,
      final String createdDate,
      final String dataSource,
      final byte[] payload,
      final int active,
      final byte[] statusPayload
  )
  {
    retryingHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(
                String.format(
                    "INSERT INTO %s (id, created_date, datasource, payload, active, status_payload) VALUES (:id, :created_date, :datasource, :payload, :active, :status_payload)",
                    tableName
                )
            )
                  .bind("id", id)
                  .bind("created_date", createdDate)
                  .bind("datasource", dataSource)
                  .bind("payload", payload)
                  .bind("active", active)
                  .bind("status_payload", statusPayload)
                  .execute();
            return null;
          }
        }
    );
  }

  /* Insert stuff. @returns 1 if status of the task with the given id has been updated successfully */
  public int setStatus(final String tableName, final String Id, final int active, final byte[] statusPayload)
  {
    return retryingHandle(
            new HandleCallback<Integer>()
            {
              @Override
              public Integer withHandle(Handle handle) throws Exception
              {
                return handle.createStatement(
                            String.format(
                                "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = 1",
                                tableName
                            )
                        )
                             .bind("id", Id)
                             .bind("active", active)
                             .bind("status_payload", statusPayload)
                             .execute();
              }
            }
        );
  }

  /* Retrieve a task with the given ID */
  public List<Map<String, Object>> getTask(final String tableName, final String Id)
  {
    return retryingHandle(
        new HandleCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                        String.format(
                            "SELECT payload FROM %s WHERE id = :id",
                            tableName
                        )
                    )
                         .bind("id", Id)
                         .list();
          }
        }
    );

  }

  /* Retrieve a task status with the given ID */
  public List<Map<String, Object>> getTaskStatus(final String tableName, final String Id)
  {
    return retryingHandle(
      new HandleCallback<List<Map<String, Object>>>()
      {
        @Override
        public List<Map<String, Object>> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT status_payload FROM %s WHERE id = :id",
                          tableName
                      )
                  )
                       .bind("id", Id)
                       .list();
        }
      }
    );
  }

  /* Retrieve active tasks */
  public List<Map<String, Object>> getActiveTasks(final String tableName)
  {
    return retryingHandle(
      new HandleCallback<List<Map<String, Object>>>()
      {
        @Override
        public List<Map<String, Object>> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT id, payload, status_payload FROM %s WHERE active = 1 ORDER BY created_date",
                          tableName
                      )
                  ).list();
        }
      }
    );

  }

  /* Retrieve task statuses that have been created sooner than the given time */
  public List<Map<String, Object>> getRecentlyFinishedTaskStatuses(final String tableName, final String recent)
  {
    return retryingHandle(
      new HandleCallback<List<Map<String, Object>>>()
      {
        @Override
        public List<Map<String, Object>> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT id, status_payload FROM %s WHERE active = 0 AND created_date >= :recent ORDER BY created_date DESC",
                          tableName
                      )
                  ).bind("recent", recent.toString()).list();
        }
      }
    );
  }

  /* Add lock to the task with given ID */
  public int addLock(final String tableName, final String Id, final byte[] lock)
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
                          tableName
                      )
                  )
                       .bind("task_id", Id)
                       .bind("lock_payload", lock)
                       .execute();
        }
      }
    );
  }

  /* Remove taskLock with given ID */
  public int removeLock(final String tableName, final long lockId)
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
                          tableName
                      )
                  )
                       .bind("id", lockId)
                       .execute();
        }
      }
    );
  }

  public int addAuditLog(final String tableName, final String Id, final byte[] taskAction)
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
                          tableName
                      )
                  )
                       .bind("task_id", Id)
                       .bind("log_payload", taskAction)
                       .execute();
        }
      }
    );
  }

  /* Get logs for task with given ID */
  public List<Map<String, Object>> getTaskLogs(final String tableName, final String Id)
  {
    return retryingHandle(
      new HandleCallback<List<Map<String, Object>>>()
      {
        @Override
        public List<Map<String, Object>> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT log_payload FROM %s WHERE task_id = :task_id",
                          tableName
                      )
                  )
                       .bind("task_id", Id)
                       .list();
        }
      }
    );
  }

  /* Get locks for task with given ID */
  public List<Map<String, Object>> getTaskLocks(final String tableName, final String Id)
  {
    return retryingHandle(
      new HandleCallback<List<Map<String, Object>>>()
      {
        @Override
        public List<Map<String, Object>> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT id, lock_payload FROM %s WHERE task_id = :task_id",
                          tableName
                      )
                  )
                       .bind("task_id", Id)
                       .list();
        }
      }
    );
  }

  //public abstract SQLMetadataConnector getConnector(Supplier<MetadataDbConnectorConfig> config, Supplier<MetadataTablesConfig> dbTables);

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

  private static boolean shouldRetryException(final Throwable e)
  {
    return e != null && (e instanceof SQLTransientException
                         || e instanceof MySQLTransientException
                         || e instanceof SQLRecoverableException
                         || e instanceof UnableToObtainConnectionException
                         || (e instanceof SQLException && ((SQLException) e).getErrorCode() == 1317)
                         || (e instanceof SQLException && shouldRetryException(e.getCause()))
                         || (e instanceof DBIException && shouldRetryException(e.getCause())));
  }

}
