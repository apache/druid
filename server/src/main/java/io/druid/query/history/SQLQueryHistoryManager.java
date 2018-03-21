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

package io.druid.query.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

@ManageLifecycle
public class SQLQueryHistoryManager implements QueryHistoryManager
{

  private final IDBI dbi;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ObjectMapper jsonMapper;
  private final QueryHistoryConfig queryHistoryConfig;

  @Inject
  public SQLQueryHistoryManager(
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables,
      @Json ObjectMapper jsonMapper,
      QueryHistoryConfig queryHistoryConfig
  )
  {
    this.dbi = connector.getDBI();
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.queryHistoryConfig = queryHistoryConfig;
  }

  private boolean isDisabled()
  {
    return !queryHistoryConfig.isEnabled();
  }

  @Override
  public void addEntry(QueryHistoryEntry entry)
  {
    if (isDisabled()) {
      return;
    }
    dbi.withHandle(new HandleCallback<Void>()
    {
      @Override
      public Void withHandle(Handle handle) throws Exception
      {
        handle.createStatement(
            StringUtils.format(
                "INSERT INTO %s (query_id, created_date, type, payload) VALUES (:query_id, :created_date, :type, :payload)",
                getQueryHistoryTable()
            )
        )
              .bind("query_id", entry.getQueryID())
              .bind("created_date", entry.getCreatedDate().toString())
              .bind("type", entry.getType())
              .bind("payload", jsonMapper.writeValueAsBytes(entry))
              .execute();
        return null;
      }
    });
  }

  @Override
  public List<String> fetchSqlQueryIDHistory()
  {
    if (isDisabled()) {
      return Collections.emptyList();
    }
    return dbi.withHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            return handle.createQuery(String.format(
                Locale.ENGLISH,
                "SELECT query_id, MAX(created_date) FROM %s WHERE type = :type GROUP BY query_id ORDER BY MAX(created_date) DESC",
                getQueryHistoryTable()
            ))
                         .bind("type", QueryHistoryEntry.TYPE_SQL_QUERY_TEXT)
                         .map(new ResultSetMapper<String>()
                         {
                           @Override
                           public String map(int i, ResultSet resultSet, StatementContext statementContext)
                               throws SQLException
                           {
                             return resultSet.getString("query_id");
                           }
                         })
                         .list();
          }
        }
    );
  }

  private String getQueryHistoryTable()
  {
    return dbTables.get().getQueryHistoryTable();
  }

  @Override
  public List<String> fetchQueryIDHistory()
  {
    if (isDisabled()) {
      return Collections.emptyList();
    }
    return dbi.withHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            return handle.createQuery(String.format(
                Locale.ENGLISH,
                "SELECT query_id, MAX(created_date) FROM %s GROUP BY query_id ORDER BY MAX(created_date) DESC",
                getQueryHistoryTable()
            ))
                         .map(new ResultSetMapper<String>()
                         {
                           @Override
                           public String map(int i, ResultSet resultSet, StatementContext statementContext)
                               throws SQLException
                           {
                             return resultSet.getString("query_id");
                           }
                         })
                         .list();
          }
        }
    );
  }

  @Override
  public List<QueryHistoryEntry> fetchQueryHistory()
  {
    if (isDisabled()) {
      return Collections.emptyList();
    }
    return dbi.withHandle(
        new HandleCallback<List<QueryHistoryEntry>>()
        {
          @Override
          public List<QueryHistoryEntry> withHandle(Handle handle)
          {
            return handle.createQuery(String.format(
                Locale.ENGLISH,
                "SELECT payload FROM %s ORDER BY created_date DESC",
                getQueryHistoryTable()
            ))
                         .map(new ResultSetMapper<QueryHistoryEntry>()
                         {
                           @Override
                           public QueryHistoryEntry map(int i, ResultSet resultSet, StatementContext statementContext)
                               throws SQLException
                           {
                             try {
                               return jsonMapper.readValue(resultSet.getBytes("payload"), QueryHistoryEntry.class);
                             }
                             catch (IOException e) {
                               throw new SQLException(e);
                             }
                           }
                         })
                         .list();
          }
        }
    );
  }

  @Override
  public List<QueryHistoryEntry> fetchQueryHistoryByQueryID(String queryID)
  {
    if (isDisabled()) {
      return Collections.emptyList();
    }
    return dbi.withHandle(
        new HandleCallback<List<QueryHistoryEntry>>()
        {
          @Override
          public List<QueryHistoryEntry> withHandle(Handle handle)
          {
            return handle.createQuery(String.format(
                Locale.ENGLISH,
                "SELECT payload FROM %s WHERE query_id = :query_id ORDER BY created_date DESC",
                getQueryHistoryTable()
            ))
                         .bind("query_id", queryID)
                         .map(new ResultSetMapper<QueryHistoryEntry>()
                         {
                           @Override
                           public QueryHistoryEntry map(int i, ResultSet resultSet, StatementContext statementContext)
                               throws SQLException
                           {
                             try {
                               return jsonMapper.readValue(resultSet.getBytes("payload"), QueryHistoryEntry.class);
                             }
                             catch (IOException e) {
                               throw new SQLException(e);
                             }
                           }
                         })
                         .list();
          }
        }
    );
  }
}
