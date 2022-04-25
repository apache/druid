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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskInfoLite;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class PostgreSQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    extends SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  private static final EmittingLogger log = new EmittingLogger(PostgreSQLMetadataStorageActionHandler.class);

  private final TaskInfoLiteMapper taskInfoMapper;

  public PostgreSQLMetadataStorageActionHandler(
      SQLMetadataConnector connector,
      ObjectMapper jsonMapper,
      MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      String entryTypeName,
      String entryTable,
      String logTable,
      String lockTable
  )
  {
    super(connector, jsonMapper, types, entryTypeName, entryTable, logTable, lockTable);
    this.taskInfoMapper = new TaskInfoLiteMapper(jsonMapper);
  }

  @Override
  protected Query<Map<String, Object>> createCompletedTaskInfoQuery(
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
      sql += " LIMIT :n";
    }

    Query<Map<String, Object>> query = handle.createQuery(sql).bind("start", timestamp.toString());

    if (maxNumStatuses != null) {
      query = query.bind("n", maxNumStatuses);
    }
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  private String getWhereClauseForInactiveStatusesSinceQuery(@Nullable String datasource)
  {
    String sql = StringUtils.format("active = FALSE AND created_date >= :start ");
    if (datasource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  @Override
  @Nullable
  public TaskInfoLite getTaskInfoLite(String entryId)
  {
    return getConnector().retryWithHandle(handle -> {
      final String query = StringUtils.format(
          "SELECT id, payload_json->>'groupId' AS group_id, payload_json->>'type' AS type, datasource, "
          + "status_payload_json->>'location' AS location, created_date, status_payload_json->>'status' AS status, "
          + "status_payload_json->>'duration' AS duration, status_payload_json->>'errorMsg' AS error_msg "
          + "FROM %s WHERE id = :id",
          getEntryTable()
      );
      return handle.createQuery(query)
                   .bind("id", entryId)
                   .map(taskInfoMapper)
                   .first();
    });
  }

  @Deprecated
  @Override
  public String getSqlRemoveLogsOlderThan()
  {
    return StringUtils.format("DELETE FROM %s USING %s "
                              + "WHERE %s_id = %s.id AND created_date < :date_time and active = false",
                              getLogTable(), getEntryTable(), getEntryTypeName(), getEntryTable());
  }

  static class TaskInfoLiteMapper implements ResultSetMapper<TaskInfoLite>
  {
    private final ObjectMapper objectMapper;

    TaskInfoLiteMapper(ObjectMapper objectMapper)
    {
      this.objectMapper = objectMapper;
    }

    @Override
    public TaskInfoLite map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      final TaskInfoLite taskInfo;
      TaskLocation location;
      try {
        location = objectMapper.readValue(resultSet.getString("location"), TaskLocation.class);
      }
      catch (IOException e) {
        log.warn("Encountered exception[%s] while deserializing location from task status, setting location to unknown", e.getMessage());
        location = TaskLocation.unknown();
      }
      taskInfo = new TaskInfoLite(
          resultSet.getString("id"),
          resultSet.getString("group_id"),
          resultSet.getString("type"),
          resultSet.getString("datasource"),
          location,
          DateTimes.of(resultSet.getString("created_date")),
          resultSet.getString("status"),
          resultSet.getLong("duration"),
          resultSet.getString("error_msg")
      );
      return taskInfo;
    }
  }
}
