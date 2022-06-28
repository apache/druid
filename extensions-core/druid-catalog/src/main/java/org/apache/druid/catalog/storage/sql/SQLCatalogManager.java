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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.storage.MetastoreManager;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;

@ManageLifecycle
public class SQLCatalogManager implements CatalogManager
{
  public static final String TABLES_TABLE = "tableDefs";

  private static final String INSERT_TABLE =
      "INSERT INTO %s\n" +
      "  (schemaName, name, creationTime, updateTime, state, payload)\n" +
      "  VALUES(:schemaName, :name, :creationTime, :updateTime, :state, :payload)";

  private static final String UPDATE_HEAD =
      "UPDATE %s\n SET\n";

  private static final String WHERE_TABLE_ID =
      "WHERE schemaName = :schemaName\n" +
      "  AND name = :name\n";

  private static final String SAFETY_CHECK =
      "  AND updateTime = :oldVersion";

  private static final String UPDATE_DEFN_UNSAFE =
      UPDATE_HEAD +
      "  payload = :payload,\n" +
      "  updateTime = :updateTime\n" +
      WHERE_TABLE_ID;

  private static final String UPDATE_DEFN_SAFE =
      UPDATE_DEFN_UNSAFE +
      SAFETY_CHECK;

  private static final String UPDATE_STATE =
      UPDATE_HEAD +
      "  state = :state,\n" +
      "  updateTime = :updateTime\n" +
      WHERE_TABLE_ID;

  private static final String SELECT_TABLE =
      "SELECT creationTime, updateTime, state, payload\n" +
      "FROM %s\n" +
      WHERE_TABLE_ID;

  private static final String SELECT_PAYLOAD =
      "SELECT state, payload\n" +
      "FROM %s\n" +
      WHERE_TABLE_ID;

  private static final String SELECT_ALL_TABLES =
      "SELECT schemaName, name\n" +
      "FROM %s\n" +
      "ORDER BY schemaName, name";

  private static final String SELECT_TABLES_IN_SCHEMA =
      "SELECT name\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "ORDER BY name";

  private static final String SELECT_TABLE_DETAILS_IN_SCHEMA =
      "SELECT name, creationTime, updateTime, state, payload\n" +
      "FROM %s\n" +
      "WHERE schemaName = :schemaName\n" +
      "ORDER BY name";

  private static final String DELETE_TABLE =
      "DELETE FROM %s\n" +
      WHERE_TABLE_ID;

  private final MetastoreManager metastoreManager;
  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final IDBI dbi;
  private final String tableName;
  private final Deque<Listener> listeners = new ConcurrentLinkedDeque<>();

  @Inject
  public SQLCatalogManager(MetastoreManager metastoreManager)
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

  @Override
  public void stop()
  {
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

  // TODO: Move to SqlMetadataConnector
  @Override
  public void createTableDefnTable()
  {
    if (!metastoreManager.createTables()) {
      return;
    }
    connector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %s (\n"
                + "  schemaName VARCHAR(255) NOT NULL,\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  creationTime BIGINT NOT NULL,\n"
                + "  updateTime BIGINT NOT NULL,\n"
                + "  state CHAR(1) NOT NULL,\n"
                + "  payload %s,\n"
                + "  PRIMARY KEY(schemaName, name)\n"
                + ")",
                tableName,
                connector.getPayloadType())));
  }

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
              long updateTime = System.currentTimeMillis();
              Update stmt = handle.createStatement(
                  StringUtils.format(INSERT_TABLE, tableName)
              )
                  .bind("schemaName", table.id().schema())
                  .bind("name", table.id().name())
                  .bind("creationTime", updateTime)
                  .bind("updateTime", updateTime)
                  .bind("state", TableMetadata.TableState.ACTIVE.code())
                  .bind("payload", table.spec().toBytes(jsonMapper));
              try {
                stmt.execute();
              }
              catch (UnableToExecuteStatementException e) {
                if (DbUtils.isDuplicateRecordException(e)) {
                  throw new DuplicateKeyException(
                        "Tried to insert a duplicate table: " + table.sqlName(),
                        e);
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

  @Override
  public TableMetadata read(TableId id)
  {
    return dbi.withHandle(
        new HandleCallback<TableMetadata>()
        {
          @Override
          public TableMetadata withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle.createQuery(
                StringUtils.format(SELECT_TABLE, tableName)
            )
                .setFetchSize(connector.getStreamingFetchSize())
                .bind("schemaName", id.schema())
                .bind("name", id.name());
            final ResultIterator<TableMetadata> resultIterator =
                query.map((index, r, ctx) ->
                  new TableMetadata(
                      id,
                      r.getLong(1),
                      r.getLong(2),
                      TableMetadata.TableState.fromCode(r.getString(3)),
                      TableSpec.fromBytes(jsonMapper, r.getBytes(4))
                  ))
                .iterator();
            if (resultIterator.hasNext()) {
              return resultIterator.next();
            }
            return null;
          }
        }
    );
  }

  @Override
  public long update(TableMetadata table, long oldVersion) throws OutOfDateException, NotFoundException
  {
    if (oldVersion == 0) {
      return updateUnsafe(table.id(), table.spec());
    } else {
      return updateSafe(table.id(), table.spec(), oldVersion);
    }
  }

  private long updateSafe(TableId id, TableSpec defn, long oldVersion) throws OutOfDateException
  {
    try {
      return dbi.withHandle(
          new HandleCallback<Long>()
          {
            @Override
            public Long withHandle(Handle handle) throws OutOfDateException
            {
              long updateTime = System.currentTimeMillis();
              int updateCount = handle.createStatement(
                  StringUtils.format(UPDATE_DEFN_SAFE, tableName))
                  .bind("schemaName", id.schema())
                  .bind("name", id.name())
                  .bind("payload", defn.toBytes(jsonMapper))
                  .bind("updateTime", updateTime)
                  .bind("oldVersion", oldVersion)
                  .execute();
              if (updateCount == 0) {
                throw new OutOfDateException(
                    StringUtils.format(
                        "Table %s: not found or update version does not match DB version",
                        id.sqlName()));
              }
              sendUpdate(id);
              return updateTime;
            }
          }
      );
    }
    catch (CallbackFailedException e) {
      if (e.getCause() instanceof OutOfDateException) {
        throw (OutOfDateException) e.getCause();
      }
      throw e;
    }
  }

  private long updateUnsafe(TableId id, TableSpec defn) throws NotFoundException
  {
    try {
      return dbi.withHandle(
          new HandleCallback<Long>()
          {
            @Override
            public Long withHandle(Handle handle) throws NotFoundException
            {
              long updateTime = System.currentTimeMillis();
              int updateCount = handle.createStatement(
                  StringUtils.format(UPDATE_DEFN_UNSAFE, tableName))
                  .bind("schemaName", id.schema())
                  .bind("name", id.name())
                  .bind("payload", defn.toBytes(jsonMapper))
                  .bind("updateTime", updateTime)
                  .execute();
              if (updateCount == 0) {
                throw new NotFoundException(
                    StringUtils.format("Table %s: not found", id.sqlName())
                );
              }
              sendUpdate(id);
              return updateTime;
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

  @Override
  public long updatePayload(TableId id, Function<TableSpec, TableSpec> transform) throws NotFoundException
  {
    try {
      return dbi.withHandle(
          new HandleCallback<Long>()
          {
            @Override
            public Long withHandle(Handle handle) throws NotFoundException
            {
              handle.begin();
              try {
                Query<Map<String, Object>> query = handle.createQuery(
                    StringUtils.format(SELECT_PAYLOAD, tableName)
                )
                    .setFetchSize(connector.getStreamingFetchSize())
                    .bind("schemaName", id.schema())
                    .bind("name", id.name());

                final ResultIterator<TableMetadata> resultIterator =
                    query.map((index, r, ctx) ->
                      new TableMetadata(
                          id,
                          0,
                          0,
                          TableMetadata.TableState.fromCode(r.getString(1)),
                          TableSpec.fromBytes(jsonMapper, r.getBytes(2))
                      ))
                    .iterator();
                TableMetadata table;
                if (resultIterator.hasNext()) {
                  table = resultIterator.next();
                } else {
                  handle.rollback();
                  throw new NotFoundException(
                      StringUtils.format("Table %s: not found", id.sqlName())
                  );
                }
                if (table.state() != TableMetadata.TableState.ACTIVE) {
                  throw new ISE("Table is in state [%s] and cannot be updated", table.state());
                }
                TableSpec revised = transform.apply(table.spec());
                long updateTime = System.currentTimeMillis();
                int updateCount = handle.createStatement(
                    StringUtils.format(UPDATE_DEFN_UNSAFE, tableName))
                    .bind("schemaName", id.schema())
                    .bind("name", id.name())
                    .bind("payload", revised.toBytes(jsonMapper))
                    .bind("updateTime", updateTime)
                    .execute();
                if (updateCount == 0) {
                  // Should never occur because we're holding a lock.
                  throw new ISE("Table %s: not found", id.sqlName());
                }
                handle.commit();
                sendUpdate(id);
                return updateTime;
              }
              catch (RuntimeException e) {
                handle.rollback();
                throw e;
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
            int updateCount = handle.createStatement(
                StringUtils.format(UPDATE_STATE, tableName))
                .bind("schemaName", id.schema())
                .bind("name", id.name())
                .bind("updateTime", updateTime)
                .bind("state", TableMetadata.TableState.DELETING.code())
                .execute();
            sendDeletion(id);
            return updateCount == 1 ? updateTime : 0;
          }
        }
    );
  }

  @Override
  public boolean delete(TableId id)
  {
    return dbi.withHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle)
          {
            int updateCount = handle.createStatement(
                StringUtils.format(DELETE_TABLE, tableName))
                .bind("schemaName", id.schema())
                .bind("name", id.name())
                .execute();
            sendDeletion(id);
            return updateCount > 0;
          }
        }
    );
  }

  @Override
  public List<TableId> list()
  {
    return dbi.withHandle(
        new HandleCallback<List<TableId>>()
        {
          @Override
          public List<TableId> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle.createQuery(
                StringUtils.format(SELECT_ALL_TABLES, tableName)
            )
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

  @Override
  public List<String> list(String dbSchema)
  {
    return dbi.withHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle.createQuery(
                StringUtils.format(SELECT_TABLES_IN_SCHEMA, tableName)
            )
                .bind("schemaName", dbSchema)
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

  @Override
  public List<TableMetadata> listDetails(String dbSchema)
  {
    return dbi.withHandle(
        new HandleCallback<List<TableMetadata>>()
        {
          @Override
          public List<TableMetadata> withHandle(Handle handle)
          {
            Query<Map<String, Object>> query = handle.createQuery(
                StringUtils.format(SELECT_TABLE_DETAILS_IN_SCHEMA, tableName)
            )
                .bind("schemaName", dbSchema)
                .setFetchSize(connector.getStreamingFetchSize());
            final ResultIterator<TableMetadata> resultIterator =
                query.map((index, r, ctx) ->
                  new TableMetadata(
                      TableId.of(dbSchema, r.getString(1)),
                      r.getLong(2),
                      r.getLong(3),
                      TableMetadata.TableState.fromCode(r.getString(4)),
                      TableSpec.fromBytes(jsonMapper, r.getBytes(5))))
                .iterator();
            return Lists.newArrayList(resultIterator);
          }
        }
    );
  }

  @Override
  public synchronized void register(Listener listener)
  {
    listeners.add(listener);
  }

  protected synchronized void sendAddition(TableMetadata table, long updateTime)
  {
    if (listeners.isEmpty()) {
      return;
    }
    TableMetadata newTable = table.fromInsert(updateTime);
    for (Listener listener : listeners) {
      listener.added(newTable);
    }
  }

  protected synchronized void sendUpdate(TableId id)
  {
    if (listeners.isEmpty()) {
      return;
    }
    TableMetadata updatedTable = read(id);
    for (Listener listener : listeners) {
      listener.updated(updatedTable);
    }
  }

  protected synchronized void sendDeletion(TableId id)
  {
    for (Listener listener : listeners) {
      listener.deleted(id);
    }
  }
}
