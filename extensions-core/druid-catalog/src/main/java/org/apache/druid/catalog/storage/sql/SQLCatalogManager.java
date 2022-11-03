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

package org.apache.druid.catalog.storage.sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.storage.MetadataStorageManager;
import org.apache.druid.catalog.sync.CatalogUpdateListener;
import org.apache.druid.catalog.sync.UpdateEvent;
import org.apache.druid.catalog.sync.UpdateEvent.EventType;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

@ManageLifecycle
public class SQLCatalogManager implements CatalogManager
{
  public static final String TABLES_TABLE = "tableDefs";

  private static final String SCHEMA_NAME_COL = "schemaName";
  private static final String TABLE_NAME_COL = "name";
  private static final String CREATION_TIME_COL = "creationTime";
  private static final String UPDATE_TIME_COL = "updateTime";
  private static final String STATE_COL = "state";
  private static final String TABLE_TYPE_COL = "tableType";
  private static final String PROPERTIES_COL = "properties";
  private static final String COLUMNS_COL = "columns";

  private final MetadataStorageManager metastoreManager;
  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final IDBI dbi;
  private final String tableName;
  private final Deque<CatalogUpdateListener> listeners = new ConcurrentLinkedDeque<>();

  @Inject
  public SQLCatalogManager(MetadataStorageManager metastoreManager)
  {
    if (!metastoreManager.isSql()) {
      throw new ISE("SQLCatalogManager only works with SQL based metadata store at this time");
    }
    this.metastoreManager = metastoreManager;
    this.connector = metastoreManager.sqlConnector();
    this.dbi = connector.getDBI();
    this.jsonMapper = metastoreManager.jsonMapper();
    this.tableName = getTableDefnTable();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    createTableDefnTable();
  }

  public static final String CREATE_TABLE =
      "CREATE TABLE %s (\n" +
      "  schemaName VARCHAR(255) NOT NULL,\n" +
      "  name VARCHAR(255) NOT NULL,\n" +
      "  creationTime BIGINT NOT NULL,\n" +
      "  updateTime BIGINT NOT NULL,\n" +
      "  state CHAR(1) NOT NULL,\n" +
      "  tableType VARCHAR(20) NOT NULL,\n" +
      "  properties %s,\n" +
      "  columns %s,\n" +
      "  PRIMARY KEY(schemaName, name)\n" +
      ")";

  // TODO: Move to SqlMetadataConnector
  public void createTableDefnTable()
  {
    if (!metastoreManager.config().isCreateTables()) {
      return;
    }
    connector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                CREATE_TABLE,
                tableName,
                connector.getPayloadType(),
                connector.getPayloadType()
            )
        )
    );
  }

  private static final String INSERT_TABLE =
      "INSERT INTO %s\n" +
      "  (schemaName, name, creationTime, updateTime, state,\n" +
      "   tableType, properties, columns)\n" +
      "  VALUES(:schemaName, :name, :creationTime, :updateTime, :state,\n" +
      "         :tableType, :properties, :columns)";

  @Override
  public long create(TableMetadata table) throws DuplicateKeyException
  {
    try {
      return dbi.withHandle(
          new HandleCallback<Long>()
          {
            @Override
            public Long withHandle(Handle handle) throws DuplicateKeyException
            {
              final TableSpec spec = table.spec();
              final long updateTime = System.currentTimeMillis();
              final Update stmt = handle
                  .createStatement(statement(INSERT_TABLE))
                  .bind(SCHEMA_NAME_COL, table.id().schema())
                  .bind(TABLE_NAME_COL, table.id().name())
                  .bind(CREATION_TIME_COL, updateTime)
                  .bind(UPDATE_TIME_COL, updateTime)
                  .bind(STATE_COL, TableMetadata.TableState.ACTIVE.code())
                  .bind(TABLE_TYPE_COL, spec.type())
                  .bind(PROPERTIES_COL, JacksonUtils.toBytes(jsonMapper, spec.properties()))
                  .bind(COLUMNS_COL, JacksonUtils.toBytes(jsonMapper, spec.columns()));
              try {
                stmt.execute();
              }
              catch (UnableToExecuteStatementException e) {
                if (DbUtils.isDuplicateRecordException(e)) {
                  throw new DuplicateKeyException(
                        "Tried to insert a duplicate table: %s",
                        table.sqlName()
                  );
                } else {
                  throw e;
                }
              }
              sendAddition(table, updateTime);
              return updateTime;
            }
          }
      );
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof DuplicateKeyException) {
        throw (DuplicateKeyException) e.getCause();
      }
      throw e;
    }
  }

  private static final String SELECT_TABLE =
      "SELECT creationTime, updateTime, state, tableType, properties, columns\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  @Override
  public TableMetadata read(TableId id) throws NotFoundException
  {
    try {
      return dbi.withHandle(
          new HandleCallback<TableMetadata>()
          {
            @Override
            public TableMetadata withHandle(Handle handle) throws NotFoundException
            {
              final Query<Map<String, Object>> query = handle
                  .createQuery(statement(SELECT_TABLE))
                  .setFetchSize(connector.getStreamingFetchSize())
                  .bind(SCHEMA_NAME_COL, id.schema())
                  .bind(TABLE_NAME_COL, id.name());
              final ResultIterator<TableMetadata> resultIterator =
                  query.map((index, r, ctx) ->
                    new TableMetadata(
                        id,
                        r.getLong(1),
                        r.getLong(2),
                        TableMetadata.TableState.fromCode(r.getString(3)),
                        tableSpecFromBytes(jsonMapper, r.getString(4), r.getBytes(5), r.getBytes(6))
                    ))
                  .iterator();
              if (resultIterator.hasNext()) {
                return resultIterator.next();
              }
              throw tableNotFound(id);
            }
          }
      );
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw e;
    }
  }

  private static final String REPLACE_SPEC_STMT =
      "UPDATE %s\n SET\n" +
      "  tableType = :tableType,\n" +
      "  properties = :properties,\n" +
      "  columns = :columns,\n" +
      "  updateTime = :updateTime\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n" +
      "  AND state = 'A'";

  @Override
  public long replace(TableMetadata table) throws NotFoundException
  {
    try {
      final TableMetadata revised = dbi.withHandle(
          new HandleCallback<TableMetadata>()
          {
            @Override
            public TableMetadata withHandle(Handle handle) throws NotFoundException
            {
              final TableId id = table.id();
              final TableSpec spec = table.spec();
              final long updateTime = System.currentTimeMillis();
              final int updateCount = handle
                  .createStatement(statement(REPLACE_SPEC_STMT))
                  .bind(SCHEMA_NAME_COL, id.schema())
                  .bind(TABLE_NAME_COL, id.name())
                  .bind(TABLE_TYPE_COL, spec.type())
                  .bind(PROPERTIES_COL, JacksonUtils.toBytes(jsonMapper, spec.properties()))
                  .bind(COLUMNS_COL, JacksonUtils.toBytes(jsonMapper, spec.columns()))
                  .bind(UPDATE_TIME_COL, updateTime)
                  .execute();
              if (updateCount == 0) {
                throw tableNotFound(id);
              }
              return table.asUpdate(updateTime);
            }
          }
      );
      sendUpdate(EventType.UPDATE, revised);
      return revised.updateTime();
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw e;
    }
  }

  private static final String OLD_VERSION_PARAM = "oldVersion";
  private static final String UPDATE_SPEC_STMT =
      REPLACE_SPEC_STMT +
      "  AND updateTime = :oldVersion";

  @Override
  public long update(TableMetadata table, long oldVersion) throws NotFoundException
  {
    try {
      final TableMetadata revised = dbi.withHandle(
          new HandleCallback<TableMetadata>()
          {
            @Override
            public TableMetadata withHandle(Handle handle) throws NotFoundException
            {
              final TableId id = table.id();
              final TableSpec spec = table.spec();
              final long updateTime = System.currentTimeMillis();
              final int updateCount = handle
                  .createStatement(statement(UPDATE_SPEC_STMT))
                  .bind(SCHEMA_NAME_COL, id.schema())
                  .bind(TABLE_NAME_COL, id.name())
                  .bind(TABLE_TYPE_COL, spec.type())
                  .bind(PROPERTIES_COL, JacksonUtils.toBytes(jsonMapper, spec.properties()))
                  .bind(COLUMNS_COL, JacksonUtils.toBytes(jsonMapper, spec.columns()))
                  .bind(UPDATE_TIME_COL, updateTime)
                  .bind(OLD_VERSION_PARAM, oldVersion)
                  .execute();
              if (updateCount == 0) {
                throw new NotFoundException(
                    "Table %s: not found, is being deleted or update version does not match DB version",
                    id.sqlName()
                );
              }
              return table.asUpdate(updateTime);
            }
          }
      );
      sendUpdate(EventType.UPDATE, revised);
      return revised.updateTime();
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw e;
    }
  }

  private static final String SELECT_PROPERTIES_STMT =
      "SELECT tableType, properties\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n" +
      "  AND state = 'A'";

  private static final String UPDATE_PROPERTIES_STMT =
      "UPDATE %s\n SET\n" +
      "  properties = :properties,\n" +
      "  updateTime = :updateTime\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  @Override
  public long updateProperties(
      final TableId id,
      final TableTransform transform
  ) throws CatalogException
  {
    try {
      final TableMetadata result = dbi.withHandle(
          new HandleCallback<TableMetadata>()
          {
            @Override
            public TableMetadata withHandle(Handle handle) throws CatalogException
            {
              handle.begin();
              try {
                final Query<Map<String, Object>> query = handle
                    .createQuery(statement(SELECT_PROPERTIES_STMT))
                    .setFetchSize(connector.getStreamingFetchSize())
                    .bind(SCHEMA_NAME_COL, id.schema())
                    .bind(TABLE_NAME_COL, id.name());

                final ResultIterator<TableSpec> resultIterator = query
                      .map((index, r, ctx) ->
                          tableSpecFromBytes(
                              jsonMapper,
                              r.getString(1),
                              r.getBytes(2),
                              null
                          )
                       )
                      .iterator();
                final TableSpec tableSpec;
                if (resultIterator.hasNext()) {
                  tableSpec = resultIterator.next();
                } else {
                  throw tableNotFound(id);
                }
                final TableSpec revised = transform.apply(TableMetadata.of(id, tableSpec));
                if (revised == null) {
                  handle.rollback();
                  return null;
                }
                final long updateTime = System.currentTimeMillis();
                final int updateCount = handle
                    .createStatement(statement(UPDATE_PROPERTIES_STMT))
                    .bind(SCHEMA_NAME_COL, id.schema())
                    .bind(TABLE_NAME_COL, id.name())
                    .bind(PROPERTIES_COL, JacksonUtils.toBytes(jsonMapper, revised.properties()))
                    .bind(UPDATE_TIME_COL, updateTime)
                    .execute();
                if (updateCount == 0) {
                  // Should never occur because we're holding a lock.
                  throw new ISE("Table %s: not found", id.sqlName());
                }
                handle.commit();
                return TableMetadata.forUpdate(id, updateTime, revised);
              }
              catch (Exception e) {
                handle.rollback();
                throw e;
              }
            }
          }
      );
      if (result == null) {
        return 0;
      }
      sendUpdate(EventType.PROPERTY_UPDATE, result);
      return result.updateTime();
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof CatalogException) {
        throw (CatalogException) e.getCause();
      }
      throw e;
    }
  }

  private static final String SELECT_COLUMNS_STMT =
      "SELECT tableType, columns\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n" +
      "  AND state = 'A'";

  private static final String UPDATE_COLUMNS_STMT =
      "UPDATE %s\n SET\n" +
      "  columns = :columns,\n" +
      "  updateTime = :updateTime\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  @Override
  public long updateColumns(
      final TableId id,
      final TableTransform transform
  ) throws CatalogException
  {
    try {
      final TableMetadata result = dbi.withHandle(
          new HandleCallback<TableMetadata>()
          {
            @Override
            public TableMetadata withHandle(Handle handle) throws CatalogException
            {
              handle.begin();
              try {
                final Query<Map<String, Object>> query = handle
                    .createQuery(statement(SELECT_COLUMNS_STMT))
                    .setFetchSize(connector.getStreamingFetchSize())
                    .bind(SCHEMA_NAME_COL, id.schema())
                    .bind(TABLE_NAME_COL, id.name());

                final ResultIterator<TableSpec> resultIterator = query
                      .map((index, r, ctx) ->
                          tableSpecFromBytes(
                              jsonMapper,
                              r.getString(1),
                              null,
                              r.getBytes(2)
                          )
                       )
                      .iterator();
                final TableSpec tableSpec;
                if (resultIterator.hasNext()) {
                  tableSpec = resultIterator.next();
                } else {
                  throw tableNotFound(id);
                }
                final TableSpec revised = transform.apply(TableMetadata.of(id, tableSpec));
                if (revised == null) {
                  handle.rollback();
                  return null;
                }
                final long updateTime = System.currentTimeMillis();
                final int updateCount = handle
                    .createStatement(statement(UPDATE_COLUMNS_STMT))
                    .bind(SCHEMA_NAME_COL, id.schema())
                    .bind(TABLE_NAME_COL, id.name())
                    .bind(COLUMNS_COL, JacksonUtils.toBytes(jsonMapper, revised.columns()))
                    .bind(UPDATE_TIME_COL, updateTime)
                    .execute();
                if (updateCount == 0) {
                  // Should never occur because we're holding a lock.
                  throw new ISE("Table %s: not found", id.sqlName());
                }
                handle.commit();
                return TableMetadata.forUpdate(id, updateTime, revised);
              }
              catch (Exception e) {
                handle.rollback();
                throw e;
              }
            }
          }
      );
      if (result == null) {
        return 0;
      }
      sendUpdate(EventType.COLUMNS_UPDATE, result);
      return result.updateTime();
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof CatalogException) {
        throw (CatalogException) e.getCause();
      }
      throw e;
    }
  }

  private static final String MARK_DELETING_STMT =
      "UPDATE %s\n SET\n" +
      "  state = 'D',\n" +
      "  updateTime = :updateTime\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  @Override
  public long markDeleting(TableId id)
  {
    return dbi.withHandle(
        new HandleCallback<Long>()
        {
          @Override
          public Long withHandle(Handle handle)
          {
            long updateTime = System.currentTimeMillis();
            int updateCount = handle
                .createStatement(statement(MARK_DELETING_STMT))
                .bind(SCHEMA_NAME_COL, id.schema())
                .bind(TABLE_NAME_COL, id.name())
                .bind(UPDATE_TIME_COL, updateTime)
                .execute();
            sendDeletion(id);
            return updateCount == 1 ? updateTime : 0;
          }
        }
    );
  }

  private static final String DELETE_TABLE_STMT =
      "DELETE FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  @Override
  public void delete(TableId id) throws NotFoundException
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws NotFoundException
            {
              int updateCount = handle
                  .createStatement(statement(DELETE_TABLE_STMT))
                  .bind(SCHEMA_NAME_COL, id.schema())
                  .bind(TABLE_NAME_COL, id.name())
                  .execute();
              if (updateCount == 0) {
                throw tableNotFound(id);
              } else {
                sendDeletion(id);
                return null;
              }
            }
          }
      );
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof NotFoundException) {
        throw (NotFoundException) e.getCause();
      }
      throw e;
    }
  }

  private static final String SELECT_ALL_TABLE_PATHS_STMT =
      "SELECT schemaName, name\n" +
      "FROM %s\n" +
      "ORDER BY schemaName, name";

  @Override
  public List<TableId> allTablePaths()
  {
    return dbi.withHandle(
        new HandleCallback<List<TableId>>()
        {
          @Override
          public List<TableId> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle
                .createQuery(statement(SELECT_ALL_TABLE_PATHS_STMT))
                .setFetchSize(connector.getStreamingFetchSize());
            final ResultIterator<TableId> resultIterator =
                query.map((index, r, ctx) ->
                    new TableId(r.getString(1), r.getString(2)))
                .iterator();
            return Lists.newArrayList(resultIterator);
          }
        }
    );
  }

  private static final String SELECT_TABLE_NAMES_IN_SCHEMA_STMT =
      "SELECT name\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "ORDER BY name";

  @Override
  public List<String> tableNamesInSchema(String dbSchema)
  {
    return dbi.withHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle
                .createQuery(statement(SELECT_TABLE_NAMES_IN_SCHEMA_STMT))
                .bind(SCHEMA_NAME_COL, dbSchema)
                .setFetchSize(connector.getStreamingFetchSize());
            final ResultIterator<String> resultIterator =
                query.map((index, r, ctx) ->
                    r.getString(1))
                .iterator();
            return Lists.newArrayList(resultIterator);
          }
        }
    );
  }

  private static final String SELECT_TABLES_IN_SCHEMA_STMT =
      "SELECT name, creationTime, updateTime, state, tableType, properties, columns\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "ORDER BY name";

  @Override
  public List<TableMetadata> tablesInSchema(String dbSchema)
  {
    return dbi.withHandle(
        new HandleCallback<List<TableMetadata>>()
        {
          @Override
          public List<TableMetadata> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle
                .createQuery(statement(SELECT_TABLES_IN_SCHEMA_STMT))
                .bind(SCHEMA_NAME_COL, dbSchema)
                .setFetchSize(connector.getStreamingFetchSize());
            final ResultIterator<TableMetadata> resultIterator =
                query.map((index, r, ctx) ->
                    new TableMetadata(
                        TableId.of(dbSchema, r.getString(1)),
                        r.getLong(2),
                        r.getLong(3),
                        TableMetadata.TableState.fromCode(r.getString(4)),
                        tableSpecFromBytes(jsonMapper, r.getString(5), r.getBytes(6), r.getBytes(7))
                    )
                 )
                .iterator();
            return Lists.newArrayList(resultIterator);
          }
        }
    );
  }

  @Override
  public synchronized void register(CatalogUpdateListener listener)
  {
    listeners.add(listener);
  }

  protected synchronized void sendAddition(TableMetadata table, long updateTime)
  {
    if (listeners.isEmpty()) {
      return;
    }
    sendEvent(new UpdateEvent(EventType.CREATE, table.fromInsert(updateTime)));
  }

  protected synchronized void sendUpdate(EventType eventType, TableMetadata table)
  {
    if (listeners.isEmpty()) {
      return;
    }
    sendEvent(new UpdateEvent(eventType, table));
  }

  protected void sendDeletion(TableId id)
  {
    sendEvent(new UpdateEvent(EventType.DELETE, TableMetadata.empty(id)));
  }

  protected synchronized void sendEvent(UpdateEvent event)
  {
    for (CatalogUpdateListener listener : listeners) {
      listener.updated(event);
    }
  }

  // Mimics what MetadataStorageTablesConfig should do.
  public String getTableDefnTable()
  {
    final String base = metastoreManager.tablesConfig().getBase();
    if (Strings.isNullOrEmpty(base)) {
      return TABLES_TABLE;
    } else {
      return StringUtils.format("%s_%s", base, TABLES_TABLE);
    }
  }

  private String statement(String baseStmt)
  {
    return StringUtils.format(baseStmt, tableName);
  }

  private NotFoundException tableNotFound(TableId id)
  {
    return new NotFoundException(
        "Table %s: not found",
        id.sqlName()
    );
  }

  /**
   * Deserialize an object from an array of bytes. Use when the object is
   * known deserializable so that the Jackson exception can be suppressed.
   */
  private static <T> T fromBytes(ObjectMapper jsonMapper, byte[] bytes, TypeReference<T> typeRef)
  {
    try {
      return jsonMapper.readValue(bytes, typeRef);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to deserialize a DB object");
    }
  }

  private static TableSpec tableSpecFromBytes(
      final ObjectMapper jsonMapper,
      final String type,
      final byte[] properties,
      final byte[] columns
  )
  {
    return new TableSpec(
        type,
        properties == null ? null : propertiesFromBytes(jsonMapper, properties),
        columns == null ? null : columnsFromBytes(jsonMapper, columns)
    );
  }

  private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
      new TypeReference<Map<String, Object>>() { };

  private static Map<String, Object> propertiesFromBytes(
      final ObjectMapper jsonMapper,
      final byte[] properties
  )
  {
    return fromBytes(jsonMapper, properties, PROPERTIES_TYPE_REF);
  }

  private static final TypeReference<List<ColumnSpec>> COLUMNS_TYPE_REF =
      new TypeReference<List<ColumnSpec>>() { };

  private static List<ColumnSpec> columnsFromBytes(
      final ObjectMapper jsonMapper,
      final byte[] properties
  )
  {
    return fromBytes(jsonMapper, properties, COLUMNS_TYPE_REF);
  }
}
