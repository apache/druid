/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    implements MetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataStorageActionHandler.class);

  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final TypeReference<EntryType> entryType;
  private final TypeReference<StatusType> statusType;
  private final TypeReference<LogType> logType;
  private final TypeReference<LockType> lockType;

  private final String entryTypeName;
  private final String entryTable;
  private final String logTable;
  private final String lockTable;

  private final TaskInfoMapper<EntryType, StatusType> taskInfoMapper;
  private final TaskStatusPlusMapper taskStatusPlusMapper;

  @SuppressWarnings("PMD.UnnecessaryFullyQualifiedName")
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
    //fully qualified references required below due to identical package names across project modules.
    //noinspection UnnecessaryFullyQualifiedName
    this.jsonMapper = jsonMapper.copy().addMixIn(org.apache.druid.metadata.PasswordProvider.class,
            org.apache.druid.metadata.PasswordProviderRedactionMixIn.class);
    this.entryType = types.getEntryType();
    this.statusType = types.getStatusType();
    this.logType = types.getLogType();
    this.lockType = types.getLockType();
    this.entryTypeName = entryTypeName;
    this.entryTable = entryTable;
    this.logTable = logTable;
    this.lockTable = lockTable;
    this.taskInfoMapper = new TaskInfoMapper<>(jsonMapper, entryType, statusType);
    this.taskStatusPlusMapper = new TaskStatusPlusMapper(jsonMapper);
  }

  protected SQLMetadataConnector getConnector()
  {
    return connector;
  }

  protected ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  protected TypeReference<StatusType> getStatusType()
  {
    return statusType;
  }

  protected String getEntryTable()
  {
    return entryTable;
  }

  protected String getLogTable()
  {
    return logTable;
  }

  protected String getEntryTypeName()
  {
    return entryTypeName;
  }

  public TypeReference<EntryType> getEntryType()
  {
    return entryType;
  }

  @Override
  public void insert(
      final String id,
      final DateTime timestamp,
      final String dataSource,
      final EntryType entry,
      final boolean active,
      final StatusType status,
      final String type,
      final String groupId
  ) throws EntryExistsException
  {
    try {
      getConnector().retryWithHandle(
          (HandleCallback<Void>) handle -> {
            final String sql = StringUtils.format(
                "INSERT INTO %s (id, created_date, datasource, payload, type, group_id, active, status_payload) "
                + "VALUES (:id, :created_date, :datasource, :payload, :type, :group_id, :active, :status_payload)",
                getEntryTable()
            );
            handle.createStatement(sql)
                  .bind("id", id)
                  .bind("created_date", timestamp.toString())
                  .bind("datasource", dataSource)
                  .bind("payload", jsonMapper.writeValueAsBytes(entry))
                  .bind("type", type)
                  .bind("group_id", groupId)
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
        Throwables.propagateIfPossible(e);
        throw new RuntimeException(e);
      }
    }
  }

  public static boolean isStatementException(Throwable e)
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
                res == null ? null : jsonMapper.readValue(res, entryType)
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
                res == null ? null : jsonMapper.readValue(res, statusType)
            );
          }
        }
    );
  }

  @Override
  @Nullable
  public TaskInfo<EntryType, StatusType> getTaskInfo(String entryId)
  {
    return connector.retryWithHandle(handle -> {
      final String query = StringUtils.format(
          "SELECT id, status_payload, payload, datasource, created_date FROM %s WHERE id = :id",
          entryTable
      );
      return handle.createQuery(query)
                   .bind("id", entryId)
                   .map(taskInfoMapper)
                   .first();
    });
  }

  @Override
  public List<TaskInfo<EntryType, StatusType>> getTaskInfos(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource
  )
  {
    return getConnector().retryTransaction(
        (handle, status) -> {
          final List<TaskInfo<EntryType, StatusType>> tasks = new ArrayList<>();
          for (Entry<TaskLookupType, TaskLookup> entry : taskLookups.entrySet()) {
            final Query<Map<String, Object>> query;
            switch (entry.getKey()) {
              case ACTIVE:
                query = createActiveTaskStreamingQuery(
                    handle,
                    dataSource
                );
                tasks.addAll(query.map(taskInfoMapper).list());
                break;
              case COMPLETE:
                CompleteTaskLookup completeTaskLookup = (CompleteTaskLookup) entry.getValue();
                query = createCompletedTaskStreamingQuery(
                    handle,
                    completeTaskLookup.getTasksCreatedPriorTo(),
                    completeTaskLookup.getMaxTaskStatuses(),
                    dataSource
                );
                tasks.addAll(query.map(taskInfoMapper).list());
                break;
              default:
                throw new IAE("Unknown TaskLookupType: [%s]", entry.getKey());
            }
          }
          return tasks;
        },
        3,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  @Override
  public List<TaskStatusPlus> getTaskStatusPlusList(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource,
      boolean fetchPayload
  )
  {
    taskStatusPlusMapper.setUsePayload(fetchPayload);
    return getConnector().retryTransaction(
        (handle, status) -> {
          final List<TaskStatusPlus> taskStatusPlusList = new ArrayList<>();
          for (Entry<TaskLookupType, TaskLookup> entry : taskLookups.entrySet()) {
            final Query<Map<String, Object>> query;
            switch (entry.getKey()) {
              case ACTIVE:
                query = !fetchPayload
                        ? createActiveTaskSummaryStreamingQuery(handle, dataSource)
                        : createActiveTaskStreamingQuery(handle, dataSource);
                taskStatusPlusList.addAll(query.map(taskStatusPlusMapper).list());
                break;
              case COMPLETE:
                CompleteTaskLookup completeTaskLookup = (CompleteTaskLookup) entry.getValue();
                DateTime priorTo = completeTaskLookup.getTasksCreatedPriorTo();
                Integer limit = completeTaskLookup.getMaxTaskStatuses();
                query = !fetchPayload
                        ? createCompletedTaskSummaryStreamingQuery(handle, priorTo, limit, dataSource)
                        : createCompletedTaskStreamingQuery(handle, priorTo, limit, dataSource);
                taskStatusPlusList.addAll(query.map(taskStatusPlusMapper).list());
                break;
              default:
                throw new IAE("Unknown TaskLookupType: [%s]", entry.getKey());
            }
          }
          return taskStatusPlusList;
        },
        3,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  /**
   * Fetches the columns needed to build TaskStatusPlus for completed tasks
   * Please note that this requires completion of data migration to avoid empty values for task type and groupId
   * Recommended for GET /tasks API
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for TaskStatusPlus for completed tasks of interest
   */
  protected Query<Map<String, Object>> createCompletedTaskSummaryStreamingQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses,
      @Nullable String dataSource
  )
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  created_date, "
        + "  datasource, "
        + "  group_id, "
        + "  type, "
        + "  status_payload "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForInactiveStatusesSinceQuery(dataSource)
        + "ORDER BY created_date DESC",
        getEntryTable()
    );

    if (maxNumStatuses != null) {
      sql = decorateSqlWithLimit(sql);
    }
    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("start", timestamp.toString())
                                             .setFetchSize(connector.getStreamingFetchSize());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  /**
   * Fetches the columns needed to build a Task object with payload for completed tasks
   * This requires the task payload which can be large. Please use only when necessary.
   * For example for ingestion tasks view before migration of the new columns
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for completed TaskInfos of interest
   */
  protected Query<Map<String, Object>> createCompletedTaskStreamingQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses,
      @Nullable String dataSource
  )
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  created_date, "
        + "  datasource, "
        + "  payload "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForInactiveStatusesSinceQuery(dataSource)
        + "ORDER BY created_date DESC",
        getEntryTable()
    );

    if (maxNumStatuses != null) {
      sql = decorateSqlWithLimit(sql);
    }
    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("start", timestamp.toString())
                                             .setFetchSize(connector.getStreamingFetchSize());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  protected abstract String decorateSqlWithLimit(String sql);

  private String getWhereClauseForInactiveStatusesSinceQuery(@Nullable String datasource)
  {
    String sql = StringUtils.format("active = FALSE AND created_date >= :start ");
    if (datasource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  /**
   * Fetches the columns needed to build TaskStatusPlus for active tasks
   * Please note that this requires completion of data migration to avoid empty values for task type and groupId
   * Recommended for GET /tasks API
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for TaskStatusPlus for active tasks of interest
   */
  private Query<Map<String, Object>> createActiveTaskSummaryStreamingQuery(Handle handle, @Nullable String dataSource)
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  group_id, "
        + "  type, "
        + "  datasource, "
        + "  created_date "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForActiveStatusesQuery(dataSource)
        + "ORDER BY created_date",
        entryTable
    );

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .setFetchSize(connector.getStreamingFetchSize());
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  /**
   * Fetches the columns needed to build Task objects with payload for active tasks
   * This requires the task payload which can be large. Please use only when necessary.
   * For example for ingestion tasks view before migration of the new columns
   * Uses streaming SQL query to avoid fetching too many rows at once into memory
   * @param handle db handle
   * @param dataSource datasource to which the tasks belong. null if we don't want to filter
   * @return Query object for active TaskInfos of interest
   */
  private Query<Map<String, Object>> createActiveTaskStreamingQuery(Handle handle, @Nullable String dataSource)
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  payload, "
        + "  datasource, "
        + "  created_date "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForActiveStatusesQuery(dataSource)
        + "ORDER BY created_date",
        entryTable
    );

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .setFetchSize(connector.getStreamingFetchSize());
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  private String getWhereClauseForActiveStatusesQuery(String dataSource)
  {
    String sql = StringUtils.format("active = TRUE ");
    if (dataSource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  static class TaskStatusPlusMapper implements ResultSetMapper<TaskStatusPlus>
  {
    private final ObjectMapper objectMapper;
    private boolean usePayload;

    TaskStatusPlusMapper(ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
      this.usePayload = true;
    }

    void setUsePayload(boolean usePayload)
    {
      this.usePayload = usePayload;
    }

    @Override
    public TaskStatusPlus map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      String type;
      String groupId;
      if (usePayload) {
        try {
          ObjectNode payload = objectMapper.readValue(resultSet.getBytes("payload"), ObjectNode.class);
          type = payload.get("type").asText();
          groupId = payload.get("groupId").asText();
        }
        catch (IOException e) {
          log.error(e, "Encountered exception while deserializing task payload");
          throw new SQLException(e);
        }
      } else {
        type = resultSet.getString("type");
        groupId = resultSet.getString("group_id");
      }

      TaskState statusCode;
      Long duration;
      TaskLocation location;
      String errorMsg;
      try {
        ObjectNode status = objectMapper.readValue(resultSet.getBytes("status_payload"), ObjectNode.class);
        statusCode = objectMapper.convertValue(status.get("status"), TaskState.class);
        duration = status.get("duration").asLong();
        location = objectMapper.convertValue(status.get("location"), TaskLocation.class);
        errorMsg = status.get("errorMsg").asText();
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task status_payload");
        throw new SQLException(e);
      }

      return new TaskStatusPlus(
          resultSet.getString("id"),
          groupId,
          type,
          DateTimes.of(resultSet.getString("created_date")),
          DateTimes.EPOCH,
          statusCode,
          statusCode.isComplete() ? RunnerTaskState.NONE : RunnerTaskState.WAITING,
          duration,
          location,
          resultSet.getString("datasource"),
          errorMsg
      );
    }
  }

  static class TaskInfoMapper<EntryType, StatusType> implements ResultSetMapper<TaskInfo<EntryType, StatusType>>
  {
    private final ObjectMapper objectMapper;
    private final TypeReference<EntryType> entryType;
    private final TypeReference<StatusType> statusType;

    TaskInfoMapper(ObjectMapper objectMapper, TypeReference<EntryType> entryType, TypeReference<StatusType> statusType)
    {
      this.objectMapper = objectMapper;
      this.entryType = entryType;
      this.statusType = statusType;
    }

    @Override
    public TaskInfo<EntryType, StatusType> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      final TaskInfo<EntryType, StatusType> taskInfo;
      EntryType task;
      StatusType status;
      try {
        task = objectMapper.readValue(resultSet.getBytes("payload"), entryType);
      }
      catch (IOException e) {
        log.warn("Encountered exception[%s] while deserializing task payload, setting payload to null", e.getMessage());
        task = null;
      }
      try {
        status = objectMapper.readValue(resultSet.getBytes("status_payload"), statusType);
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task status_payload");
        throw new SQLException(e);
      }
      taskInfo = new TaskInfo<>(
          resultSet.getString("id"),
          DateTimes.of(resultSet.getString("created_date")),
          status,
          resultSet.getString("datasource"),
          task
      );
      return taskInfo;
    }
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
          public Void withHandle(Handle handle)
          {
            removeLock(handle, lockId);

            return null;
          }
        }
    );
  }

  @Override
  public void removeTasksOlderThan(final long timestamp)
  {
    DateTime dateTime = DateTimes.utc(timestamp);
    connector.retryWithHandle(
        (HandleCallback<Void>) handle -> {
          handle.createStatement(getSqlRemoveLogsOlderThan())
                .bind("date_time", dateTime.toString())
                .execute();
          handle.createStatement(
              StringUtils.format(
                  "DELETE FROM %s WHERE created_date < :date_time AND active = false",
                  entryTable
              )
          )
                .bind("date_time", dateTime.toString())
                .execute();

          return null;
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
          public List<LogType> withHandle(Handle handle)
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
                    new ArrayList<>(),
                    (List<LogType> list, byte[] bytes, FoldController control, StatementContext ctx) -> {
                      try {
                        list.add(jsonMapper.readValue(bytes, logType));
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
                );
          }
        }
    );
  }

  @Deprecated
  public String getSqlRemoveLogsOlderThan()
  {
    return StringUtils.format("DELETE a FROM %s a INNER JOIN %s b ON a.%s_id = b.id "
                              + "WHERE b.created_date < :date_time and b.active = false",
                              logTable, entryTable, entryTypeName
    );
  }

  @Override
  public Map<Long, LockType> getLocks(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Map<Long, LockType>>()
        {
          @Override
          public Map<Long, LockType> withHandle(Handle handle)
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
                                       jsonMapper.readValue(
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
                             Maps.newLinkedHashMap(),
                             new Folder3<Map<Long, LockType>, Pair<Long, LockType>>()
                             {
                               @Override
                               public Map<Long, LockType> fold(
                                   Map<Long, LockType> accumulator,
                                   Pair<Long, LockType> lock,
                                   FoldController control,
                                   StatementContext ctx
                               )
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

  @Override
  public boolean migrateTaskTable(String tableName)
  {
    log.info("Populate fields task and group_id of task entry table [%s] from payload", tableName);
    try {
      connector.retryWithHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws SQLException, IOException
            {
              ObjectMapper objectMapper = new ObjectMapper();
              Connection connection = handle.getConnection();
              Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
              boolean flag = true;
              while (flag) {
                // Should ideally use a cursor and sort by id for efficiency, but updates with ordering aren't allowed
                String sql = StringUtils.format(
                    "SELECT * FROM %1$s WHERE active = false AND type IS null %2$s",
                    tableName,
                    connector.limitClause(100)
                );
                ResultSet resultSet = statement.executeQuery(sql);
                flag = false;
                while (resultSet.next()) {
                  ObjectNode payload = objectMapper.readValue(resultSet.getBytes("payload"), ObjectNode.class);
                  resultSet.updateString("type", payload.get("type").asText());
                  resultSet.updateString("group_id", payload.get("groupId").asText());
                  resultSet.updateRow();
                  flag = true;
                }
              }
              statement.close();
              return null;
            }
          }
      );
      log.info("Migration of tasks complete for table[%s]", tableName);
      return true;
    }
    catch (Exception e) {
      log.warn(e, "Exception migrating task table [%s]", tableName);
      return false;
    }
  }
}
