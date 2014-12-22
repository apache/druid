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
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.RetryUtils;
import com.metamx.common.StringUtils;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
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

public class SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    implements MetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataStorageActionHandler.class);

  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final TypeReference entryType;
  private final TypeReference statusType;
  private final TypeReference logType;
  private final TypeReference lockType;

  private final String entryTypeName;
  private final String entryTable;
  private final String logTable;
  private final String lockTable;

  public SQLMetadataStorageActionHandler(
      final SQLMetadataConnector connector,
      final ObjectMapper jsonMapper,
      final MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      final String entryTypeName,
      final String entryTable,
      final String logTable,
      final String lockTable
  )
  {
    this.connector = connector;
    this.jsonMapper = jsonMapper;
    this.entryType = types.getEntryType();
    this.statusType = types.getStatusType();
    this.logType = types.getLogType();
    this.lockType = types.getLockType();
    this.entryTypeName = entryTypeName;
    this.entryTable = entryTable;
    this.logTable = logTable;
    this.lockTable = lockTable;
  }

  public void insert(
      final String id,
      final DateTime timestamp,
      final String dataSource,
      final EntryType entry,
      final boolean active,
      final StatusType status
  ) throws EntryExistsException
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
                      entryTable
                  )
              )
                    .bind("id", id)
                    .bind("created_date", timestamp.toString())
                    .bind("datasource", dataSource)
                    .bind("payload", jsonMapper.writeValueAsBytes(entry))
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
      if (isStatementException && getEntry(id).isPresent()) {
        throw new EntryExistsException(id, e);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  public boolean setStatus(final String entryId, final boolean active, final StatusType status)
  {
    return retryingHandle(
            new HandleCallback<Boolean>()
            {
              @Override
              public Boolean withHandle(Handle handle) throws Exception
              {
                return handle.createStatement(
                            String.format(
                                "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = TRUE",
                                entryTable
                            )
                        )
                             .bind("id", entryId)
                             .bind("active", active)
                             .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                             .execute() == 1;
              }
            }
        );
  }

  public Optional<EntryType> getEntry(final String entryId)
  {
    return retryingHandle(
        new HandleCallback<Optional<EntryType>>()
        {
          @Override
          public Optional<EntryType> withHandle(Handle handle) throws Exception
          {
            return Optional.fromNullable(
                jsonMapper.<EntryType>readValue(
                    handle.createQuery(
                        String.format("SELECT payload FROM %s WHERE id = :id", entryTable)
                    )
                          .bind("id", entryId)
                          .map(ByteArrayMapper.FIRST)
                          .first(),
                    entryType
                )
            );
          }
        }
    );

  }

  public Optional<StatusType> getStatus(final String entryId)
  {
    return retryingHandle(
        new HandleCallback<Optional<StatusType>>()
        {
          @Override
          public Optional<StatusType> withHandle(Handle handle) throws Exception
          {
            return Optional.fromNullable(
                jsonMapper.<StatusType>readValue(
                    handle.createQuery(
                        String.format("SELECT status_payload FROM %s WHERE id = :id", entryTable)
                    )
                          .bind("id", entryId)
                          .map(ByteArrayMapper.FIRST)
                          .first(),
                    statusType
                )
            );
          }
        }
    );
  }

  public List<Pair<EntryType, StatusType>> getActiveEntriesWithStatus()
  {
    return retryingHandle(
      new HandleCallback<List<Pair<EntryType, StatusType>>>()
      {
        @Override
        public List<Pair<EntryType, StatusType>> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT id, payload, status_payload FROM %s WHERE active = TRUE ORDER BY created_date",
                      entryTable
                  )
              )
              .map(
                  new ResultSetMapper<Pair<EntryType, StatusType>>()
                  {
                    @Override
                    public Pair<EntryType, StatusType> map(int index, ResultSet r, StatementContext ctx)
                        throws SQLException
                    {
                      try {
                        return Pair.of(
                            jsonMapper.<EntryType>readValue(
                                r.getBytes("payload"),
                                entryType
                            ),
                            jsonMapper.<StatusType>readValue(
                                r.getBytes("status_payload"),
                                statusType
                            )
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to parse entry payload").addData("entry", r.getString("id")).emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              ).list();
        }
      }
    );

  }

  public List<StatusType> getInactiveStatusesSince(final DateTime timestamp)
  {
    return retryingHandle(
      new HandleCallback<List<StatusType>>()
      {
        @Override
        public List<StatusType> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT id, status_payload FROM %s WHERE active = FALSE AND created_date >= :start ORDER BY created_date DESC",
                      entryTable
                  )
              ).bind("start", timestamp.toString())
              .map(
                  new ResultSetMapper<StatusType>()
                  {
                    @Override
                    public StatusType map(int index, ResultSet r, StatementContext ctx) throws SQLException
                    {
                      try {
                        return jsonMapper.readValue(
                            r.getBytes("status_payload"),
                            statusType
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to parse status payload")
                           .addData("entry", r.getString("id"))
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

  public boolean addLock(final String entryId, final LockType lock)
  {
    return retryingHandle(
      new HandleCallback<Boolean>()
      {
        @Override
        public Boolean withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "INSERT INTO %1$s (%2$s_id, lock_payload) VALUES (:entryId, :payload)",
                          lockTable, entryTypeName
                      )
                  )
                       .bind("entryId", entryId)
                       .bind("payload", jsonMapper.writeValueAsBytes(lock))
                       .execute() == 1;
        }
      }
    );
  }

  public boolean removeLock(final long lockId)
  {
    return retryingHandle(
      new HandleCallback<Boolean>()
      {
        @Override
        public Boolean withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "DELETE FROM %s WHERE id = :id",
                          lockTable
                      )
                  )
                       .bind("id", lockId)
                       .execute() == 1;
        }
      }
    );
  }

  public boolean addLog(final String entryId, final LogType log)
  {
    return retryingHandle(
      new HandleCallback<Boolean>()
      {
        @Override
        public Boolean withHandle(Handle handle) throws Exception
        {
          return handle.createStatement(
                      String.format(
                          "INSERT INTO %1$s (%2$s_id, log_payload) VALUES (:entryId, :payload)",
                          logTable, entryTypeName
                      )
                  )
                       .bind("entryId", entryId)
                       .bind("payload", jsonMapper.writeValueAsBytes(log))
                       .execute() == 1;
        }
      }
    );
  }

  public List<LogType> getLogs(final String entryId)
  {
    return retryingHandle(
      new HandleCallback<List<LogType>>()
      {
        @Override
        public List<LogType> withHandle(Handle handle) throws Exception
        {
          return handle
              .createQuery(
                  String.format(
                      "SELECT log_payload FROM %1$s WHERE %2$s_id = :entryId",
                      logTable, entryTypeName
                  )
              )
              .bind("entryId", entryId)
              .map(ByteArrayMapper.FIRST)
              .fold(
                  Lists.<LogType>newLinkedList(),
                  new Folder3<List<LogType>, byte[]>()
                  {
                    @Override
                    public List<LogType> fold(
                        List<LogType> list, byte[] bytes, FoldController control, StatementContext ctx
                    ) throws SQLException
                    {
                      try {
                        list.add(
                            jsonMapper.<LogType>readValue(
                                bytes, logType
                            )
                        );
                        return list;
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to deserialize log")
                           .addData("entryId", entryId)
                           .addData("payload", StringUtils.fromUtf8(bytes))
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

  public Map<Long, LockType> getLocks(final String entryId)
  {
    return retryingHandle(
      new HandleCallback<Map<Long, LockType>>()
      {
        @Override
        public Map<Long, LockType> withHandle(Handle handle) throws Exception
        {
          return handle.createQuery(
                      String.format(
                          "SELECT id, lock_payload FROM %1$s WHERE %2$s_id = :entryId",
                          lockTable, entryTypeName
                      )
                  )
                       .bind("entryId", entryId)
              .map(
                  new ResultSetMapper<Pair<Long, LockType>>()
                  {
                    @Override
                    public Pair<Long, LockType> map(int index, ResultSet r, StatementContext ctx)
                        throws SQLException
                    {
                      try {
                        return Pair.of(
                            r.getLong("id"),
                            jsonMapper.<LockType>readValue(
                                r.getBytes("lock_payload"),
                                lockType
                            )
                        );
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to deserialize " + lockType.getType())
                           .addData("id", r.getLong("id"))
                           .addData(
                               "lockPayload", StringUtils.fromUtf8(r.getBytes("lock_payload"))
                           )
                            .emit();
                        throw new SQLException(e);
                      }
                    }
                  }
              )
              .fold(
                  Maps.<Long, LockType>newLinkedHashMap(),
                  new Folder3<Map<Long, LockType>, Pair<Long, LockType>>()
                  {
                    @Override
                    public Map<Long, LockType> fold(
                        Map<Long, LockType> accumulator,
                        Pair<Long, LockType> lock,
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
        return connector.getDBI().withHandle(callback);
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
