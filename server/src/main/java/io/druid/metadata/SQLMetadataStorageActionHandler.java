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

package io.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
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

  protected SQLMetadataConnector getConnector()
  {
    return connector;
  }

  protected ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  protected TypeReference getStatusType()
  {
    return statusType;
  }

  protected String getEntryTable()
  {
    return entryTable;
  }

  @Override
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
      getConnector().retryWithHandle(
          (HandleCallback<Void>) handle -> {
            final String sql = StringUtils.format(
                "INSERT INTO %s (id, created_date, datasource, payload, active, status_payload) "
                + "VALUES (:id, :created_date, :datasource, :payload, :active, :status_payload)",
                getEntryTable()
            );
            handle.createStatement(sql)
                  .bind("id", id)
                  .bind("created_date", timestamp.toString())
                  .bind("datasource", dataSource)
                  .bind("payload", jsonMapper.writeValueAsBytes(entry))
                  .bind("active", active)
                  .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                  .execute();
            return null;
          },
          e -> getConnector().isTransientException(e) && !(isStatementException(e) && getEntry(id).isPresent())
      );
    }
    catch (Exception e) {
      if (isStatementException(e) && getEntry(id).isPresent()) {
        throw new EntryExistsException(id, e);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  protected static boolean isStatementException(Throwable e)
  {
    return e instanceof StatementException ||
           (e instanceof CallbackFailedException && e.getCause() instanceof StatementException);
  }

  @Override
  public boolean setStatus(final String entryId, final boolean active, final StatusType status)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                StringUtils.format(
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

  @Override
  public Optional<EntryType> getEntry(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Optional<EntryType>>()
        {
          @Override
          public Optional<EntryType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                StringUtils.format("SELECT payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.<EntryType>readValue(res, entryType)
            );
          }
        }
    );

  }

  @Override
  public Optional<StatusType> getStatus(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Optional<StatusType>>()
        {
          @Override
          public Optional<StatusType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                StringUtils.format("SELECT status_payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.<StatusType>readValue(res, statusType)
            );
          }
        }
    );
  }

  @Override
  public List<Pair<EntryType, StatusType>> getActiveEntriesWithStatus()
  {
    return connector.retryWithHandle(
        new HandleCallback<List<Pair<EntryType, StatusType>>>()
        {
          @Override
          public List<Pair<EntryType, StatusType>> withHandle(Handle handle) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format(
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

  @Override
  public List<StatusType> getInactiveStatusesSince(DateTime timestamp, @Nullable Integer maxNumStatuses)
  {
    return getConnector().retryWithHandle(
        handle -> {
          final Query<Map<String, Object>> query = createInactiveStatusesSinceQuery(handle, timestamp, maxNumStatuses);

          return query
              .map(
                  (ResultSetMapper<StatusType>) (index, r, ctx) -> {
                    try {
                      return getJsonMapper().readValue(
                          r.getBytes("status_payload"),
                          getStatusType()
                      );
                    }
                    catch (IOException e) {
                      log.makeAlert(e, "Failed to parse status payload")
                         .addData("entry", r.getString("id"))
                         .emit();
                      throw new SQLException(e);
                    }
                  }
              ).list();
        }
    );
  }

  protected abstract Query<Map<String, Object>> createInactiveStatusesSinceQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses
  );

  @Override
  @Nullable
  public Pair<DateTime, String> getCreatedDateAndDataSource(String entryId)
  {
    return connector.retryWithHandle(
        handle -> handle
        .createQuery(
            StringUtils.format(
                "SELECT created_date, datasource FROM %s WHERE id = :entryId",
                entryTable
            )
        )
        .bind("entryId", entryId)
        .map(
            (index, resultSet, ctx) -> Pair.of(
                DateTimes.of(resultSet.getString("created_date")), resultSet.getString("datasource")
            )
        )
        .first()
    );
  }

  @Override
  public boolean addLock(final String entryId, final LockType lock)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return addLock(handle, entryId, lock);
          }
        }
    );
  }

  private boolean addLock(Handle handle, String entryId, LockType lock) throws JsonProcessingException
  {
    final String statement = StringUtils.format(
        "INSERT INTO %1$s (%2$s_id, lock_payload) VALUES (:entryId, :payload)",
        lockTable, entryTypeName
    );
    return handle.createStatement(statement)
                 .bind("entryId", entryId)
                 .bind("payload", jsonMapper.writeValueAsBytes(lock))
                 .execute() == 1;
  }

  @Override
  public boolean replaceLock(final String entryId, final long oldLockId, final LockType newLock)
  {
    return connector.retryTransaction(
        (handle, transactionStatus) -> {
          int numDeletedRows = removeLock(handle, oldLockId);

          if (numDeletedRows != 1) {
            transactionStatus.setRollbackOnly();
            final String message = numDeletedRows == 0 ?
                                   StringUtils.format("Cannot find lock[%d]", oldLockId) :
                                   StringUtils.format("Found multiple locks for lockId[%d]", oldLockId);
            throw new RuntimeException(message);
          }

          return addLock(handle, entryId, newLock);
        },
        3,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  @Override
  public void removeLock(final long lockId)
  {
    connector.retryWithHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            removeLock(handle, lockId);

            return null;
          }
        }
    );
  }

  private int removeLock(Handle handle, long lockId)
  {
    return handle.createStatement(StringUtils.format("DELETE FROM %s WHERE id = :id", lockTable))
                 .bind("id", lockId)
                 .execute();
  }

  @Override
  public boolean addLog(final String entryId, final LogType log)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                StringUtils.format(
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

  @Override
  public List<LogType> getLogs(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<List<LogType>>()
        {
          @Override
          public List<LogType> withHandle(Handle handle) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format(
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

  @Override
  public Map<Long, LockType> getLocks(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Map<Long, LockType>>()
        {
          @Override
          public Map<Long, LockType> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                StringUtils.format(
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

  @Override
  @Nullable
  public Long getLockId(String entryId, LockType lock)
  {
    return getLocks(entryId).entrySet().stream()
                            .filter(entry -> entry.getValue().equals(lock))
                            .map(Entry::getKey)
                            .findAny()
                            .orElse(null);
  }
}
