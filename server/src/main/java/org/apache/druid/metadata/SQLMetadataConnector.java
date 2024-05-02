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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class SQLMetadataConnector implements MetadataStorageConnector
{
  private static final Logger log = new Logger(SQLMetadataConnector.class);
  private static final String PAYLOAD_TYPE = "BLOB";
  private static final String COLLATION = "";

  static final int DEFAULT_MAX_TRIES = 10;

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final Supplier<MetadataStorageTablesConfig> tablesConfigSupplier;
  private final Predicate<Throwable> shouldRetry;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public SQLMetadataConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfigSupplier,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.config = config;
    this.tablesConfigSupplier = tablesConfigSupplier;
    this.shouldRetry = this::isTransientException;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
  }

  /**
   * SQL type to use for payload data (e.g. JSON blobs).
   * Must be a binary type, which values can be accessed using ResultSet.getBytes()
   * <p/>
   * The resulting string will be interpolated into the table creation statement, e.g.
   * <code>CREATE TABLE druid_table ( payload <type> NOT NULL, ... )</code>
   *
   * @return String representing the SQL type
   */
  public String getPayloadType()
  {
    return PAYLOAD_TYPE;
  }

  /**
   * The character set and collation for case-sensitive nonbinary string comparison
   *
   * @return the collation for the character set
   */
  public String getCollation()
  {
    return COLLATION;
  }

  /**
   * Auto-incrementing integer SQL type to use for IDs.
   * The returned string is interpolated into the table creation statement as follows:
   * <pre>
   * CREATE TABLE druid_table (
   *   id &lt;serial-type&gt; NOT NULL,
   *   col_2 VARCHAR(255) NOT NULL,
   *   col_3 VARCHAR(255) NOT NULL
   *   ...
   * )
   * </pre>
   *
   * @return String representing auto-incrementing SQL integer type to use for IDs.
   */
  public abstract String getSerialType();

  /**
   * Returns the value that should be passed to statement.setFetchSize to ensure results
   * are streamed back from the database instead of fetching the entire result set in memory.
   *
   * @return optimal fetch size to stream results back
   */
  public abstract int getStreamingFetchSize();

  /**
   * @return the string that should be used to quote string fields
   */
  public abstract String getQuoteString();

  public String getValidationQuery()
  {
    return "SELECT 1";
  }

  public abstract boolean tableExists(Handle handle, String tableName);

  public abstract String limitClause(int limit);

  public <T> T retryWithHandle(
      final HandleCallback<T> callback,
      final Predicate<Throwable> myShouldRetry
  )
  {
    try {
      return RetryUtils.retry(() -> getDBI().withHandle(callback), myShouldRetry, DEFAULT_MAX_TRIES);
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  public <T> T retryWithHandle(final HandleCallback<T> callback)
  {
    return retryWithHandle(callback, shouldRetry);
  }

  public <T> T retryTransaction(final TransactionCallback<T> callback, final int quietTries, final int maxTries)
  {
    try {
      return RetryUtils.retry(() -> getDBI().inTransaction(TransactionIsolationLevel.READ_COMMITTED, callback), shouldRetry, quietTries, maxTries);
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  public final boolean isTransientException(Throwable e)
  {
    return e != null && (e instanceof RetryTransactionException
                         || e instanceof SQLTransientException
                         || e instanceof SQLRecoverableException
                         || e instanceof UnableToObtainConnectionException
                         || (e instanceof UnableToExecuteStatementException && isTransientException(e.getCause()))
                         || connectorIsTransientException(e)
                         || (e instanceof SQLException && isTransientException(e.getCause()))
                         || (e instanceof DBIException && isTransientException(e.getCause())));
  }

  /**
   * Vendor specific errors that are not covered by {@link #isTransientException(Throwable)}
   */
  protected boolean connectorIsTransientException(Throwable e)
  {
    return false;
  }

  /**
   * Checks if the root cause of the given exception is a PacketTooBigException.
   *
   * @return false by default. Specific implementations should override this method
   * to correctly classify their packet exceptions.
   */
  protected boolean isRootCausePacketTooBigException(Throwable t)
  {
    return false;
  }

  /**
   * Creates the given table and indexes if the table doesn't already exist.
   */
  public void createTable(final String tableName, final Iterable<String> sql)
  {
    try {
      retryWithHandle(handle -> {
        if (tableExists(handle, tableName)) {
          log.info("Table[%s] already exists", tableName);
        } else {
          log.info("Creating table[%s]", tableName);
          final Batch batch = handle.createBatch();
          for (String s : sql) {
            batch.add(s);
          }
          batch.execute();
        }
        return null;
      });
    }
    catch (Exception e) {
      log.warn(e, "Exception creating table");
    }
  }

  /**
   * Execute the desired ALTER statement on the desired table
   *
   * @param tableName The name of the table being altered
   * @param sql ALTER statment to be executed
   */
  private void alterTable(final String tableName, final Iterable<String> sql)
  {
    try {
      retryWithHandle(handle -> {
        if (tableExists(handle, tableName)) {
          final Batch batch = handle.createBatch();
          for (String s : sql) {
            log.info("Altering table[%s], with command: %s", tableName, s);
            batch.add(s);
          }
          batch.execute();
        } else {
          log.info("Table[%s] doesn't exist.", tableName);
        }
        return null;
      });
    }
    catch (Exception e) {
      log.warn(e, "Exception Altering table[%s]", tableName);
    }
  }

  public void createPendingSegmentsTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  dataSource VARCHAR(255) %4$s NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  start VARCHAR(255) NOT NULL,\n"
                + "  %3$send%3$s VARCHAR(255) NOT NULL,\n"
                + "  sequence_name VARCHAR(255) NOT NULL,\n"
                + "  sequence_prev_id VARCHAR(255) NOT NULL,\n"
                + "  sequence_name_prev_id_sha1 VARCHAR(255) NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY (id),\n"
                + "  UNIQUE (sequence_name_prev_id_sha1)\n"
                + ")",
                tableName, getPayloadType(), getQuoteString(), getCollation()
            ),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_datasource_end ON %1$s(dataSource, %2$send%2$s)",
                tableName,
                getQuoteString()
            ),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_datasource_sequence ON %1$s(dataSource, sequence_name)",
                tableName
            )
        )
    );
    alterPendingSegmentsTable(tableName);
  }

  public void createDataSourceTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  dataSource VARCHAR(255) %3$s NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  commit_metadata_payload %2$s NOT NULL,\n"
                + "  commit_metadata_sha1 VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (dataSource)\n"
                + ")",
                tableName, getPayloadType(), getCollation()
            )
        )
    );
  }

  public void createSegmentTable(final String tableName)
  {
    List<String> columns = new ArrayList<>();
    columns.add("id VARCHAR(255) NOT NULL");
    columns.add("dataSource VARCHAR(255) %4$s NOT NULL");
    columns.add("created_date VARCHAR(255) NOT NULL");
    columns.add("start VARCHAR(255) NOT NULL");
    columns.add("%3$send%3$s VARCHAR(255) NOT NULL");
    columns.add("partitioned BOOLEAN NOT NULL");
    columns.add("version VARCHAR(255) NOT NULL");
    columns.add("used BOOLEAN NOT NULL");
    columns.add("payload %2$s NOT NULL");
    columns.add("used_status_last_updated VARCHAR(255) NOT NULL");

    if (centralizedDatasourceSchemaConfig.isEnabled()) {
      columns.add("schema_fingerprint VARCHAR(255)");
      columns.add("num_rows BIGINT");
    }

    StringBuilder createStatementBuilder = new StringBuilder("CREATE TABLE %1$s (");

    for (String column : columns) {
      createStatementBuilder.append(column);
      createStatementBuilder.append(",");
    }

    createStatementBuilder.append("PRIMARY KEY (id))");

    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                createStatementBuilder.toString(),
                tableName, getPayloadType(), getQuoteString(), getCollation()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_used ON %1$s(used)", tableName),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_datasource_used_end_start ON %1$s(dataSource, used, %2$send%2$s, start)",
                tableName,
                getQuoteString()
            )
        )
    );
  }

  private void createUpgradeSegmentsTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  task_id VARCHAR(255) NOT NULL,\n"
                + "  segment_id VARCHAR(255) NOT NULL,\n"
                + "  lock_version VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType()
            ),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_task ON %1$s(task_id)",
                tableName
            )
        )
    );
  }

  public void createRulesTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  dataSource VARCHAR(255) %3$s NOT NULL,\n"
                + "  version VARCHAR(255) NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType(), getCollation()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName)
        )
    );
  }

  public void createConfigTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY(name)\n"
                + ")",
                tableName, getPayloadType()
            )
        )
    );
  }

  public void prepareTaskEntryTable(final String tableName)
  {
    createEntryTable(tableName);
    alterEntryTableAddTypeAndGroupId(tableName);
  }

  public void createEntryTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  datasource VARCHAR(255) %3$s NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  status_payload %2$s NOT NULL,\n"
                + "  active BOOLEAN NOT NULL DEFAULT FALSE,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType(), getCollation()
            )
        )
    );
    final Set<String> createdIndexSet = getIndexOnTable(tableName);
    createIndex(
        tableName,
        StringUtils.format("idx_%1$s_active_created_date", tableName),
        ImmutableList.of("active", "created_date"),
        createdIndexSet
    );
    createIndex(
        tableName,
        StringUtils.format("idx_%1$s_datasource_active", tableName),
        ImmutableList.of("datasource", "active"),
        createdIndexSet
    );
  }

  private void alterEntryTableAddTypeAndGroupId(final String tableName)
  {
    List<String> statements = new ArrayList<>();
    if (tableHasColumn(tableName, "type")) {
      log.info("Table[%s] already has column[type].", tableName);
    } else {
      log.info("Adding column[type] to table[%s].", tableName);
      statements.add(StringUtils.format("ALTER TABLE %1$s ADD COLUMN type VARCHAR(255)", tableName));
    }
    if (tableHasColumn(tableName, "group_id")) {
      log.info("Table[%s] already has column[group_id].", tableName);
    } else {
      log.info("Adding column[group_id] to table[%s].", tableName);
      statements.add(StringUtils.format("ALTER TABLE %1$s ADD COLUMN group_id VARCHAR(255)", tableName));
    }
    if (!statements.isEmpty()) {
      alterTable(tableName, statements);
    }
  }

  /**
   * Adds the following columns to the pending segments table to clean up unused records,
   * and to faciliatate concurrent append and replace.
   * 1) task_allocator_id -> The task id / task group id / task replica group id of the task that allocated it.
   * 2) upgraded_from_segment_id -> The id of the segment from which the entry was upgraded upon concurrent replace.
   *
   * Also, adds an index on (dataSource, task_allocator_id)
   * @param tableName name of the pending segments table
   */
  private void alterPendingSegmentsTable(final String tableName)
  {
    List<String> statements = new ArrayList<>();
    if (tableHasColumn(tableName, "upgraded_from_segment_id")) {
      log.info("Table[%s] already has column[upgraded_from_segment_id].", tableName);
    } else {
      log.info("Adding column[upgraded_from_segment_id] to table[%s].", tableName);
      statements.add(StringUtils.format("ALTER TABLE %1$s ADD COLUMN upgraded_from_segment_id VARCHAR(255)", tableName));
    }
    if (tableHasColumn(tableName, "task_allocator_id")) {
      log.info("Table[%s] already has column[task_allocator_id].", tableName);
    } else {
      log.info("Adding column[task_allocator_id] to table[%s].", tableName);
      statements.add(StringUtils.format("ALTER TABLE %1$s ADD COLUMN task_allocator_id VARCHAR(255)", tableName));
    }
    if (!statements.isEmpty()) {
      alterTable(tableName, statements);
    }

    final Set<String> createdIndexSet = getIndexOnTable(tableName);
    createIndex(
        tableName,
        StringUtils.format("idx_%1$s_datasource_task_allocator_id", tableName),
        ImmutableList.of("dataSource", "task_allocator_id"),
        createdIndexSet
    );
  }

  public void createLogTable(final String tableName, final String entryTypeName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  %4$s_id VARCHAR(255) DEFAULT NULL,\n"
                + "  log_payload %3$s,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType(), entryTypeName
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
        )
    );
  }

  public void createLockTable(final String tableName, final String entryTypeName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  %4$s_id VARCHAR(255) DEFAULT NULL,\n"
                + "  lock_payload %3$s,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType(), entryTypeName
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
        )
    );
  }

  public void createSupervisorsTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  spec_id VARCHAR(255) NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  payload %3$s NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_spec_id ON %1$s(spec_id)", tableName)
        )
    );
  }

  /**
   * Adds new columns (used_status_last_updated) to the "segments" table.
   * Conditionally, add schema_fingerprint, num_rows columns.
   */
  protected void alterSegmentTable()
  {
    final String tableName = tablesConfigSupplier.get().getSegmentsTable();

    Map<String, String> columnNameTypes = new HashMap<>();
    columnNameTypes.put("used_status_last_updated", "VARCHAR(255)");

    if (centralizedDatasourceSchemaConfig.isEnabled()) {
      columnNameTypes.put("schema_fingerprint", "VARCHAR(255)");
      columnNameTypes.put("num_rows", "BIGINT");
    }

    Set<String> columnsToAdd = new HashSet<>();

    for (String columnName : columnNameTypes.keySet()) {
      if (tableHasColumn(tableName, columnName)) {
        log.info("Table[%s] already has column[%s].", tableName, columnName);
      } else {
        columnsToAdd.add(columnName);
      }
    }

    List<String> alterCommands = new ArrayList<>();
    if (!columnsToAdd.isEmpty()) {
      for (String columnName : columnsToAdd) {
        alterCommands.add(
            StringUtils.format(
                "ALTER TABLE %1$s ADD %2$s %3$s",
                tableName,
                columnName,
                columnNameTypes.get(columnName)
            )
        );
      }

      log.info("Adding columns %s to table[%s].", columnsToAdd, tableName);
    }

    alterTable(
        tableName,
        alterCommands
    );
  }

  @Override
  public Void insertOrUpdate(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value
  )
  {
    return getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus)
          {
            int count = handle
                .createQuery(
                    StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", tableName, keyColumn)
                )
                .bind("key", key)
                .map(IntegerMapper.FIRST)
                .first();
            if (count == 0) {
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value)",
                      tableName, keyColumn, valueColumn
                  )
              )
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            } else {
              handle.createStatement(
                  StringUtils.format(
                      "UPDATE %1$s SET %3$s=:value WHERE %2$s=:key",
                      tableName, keyColumn, valueColumn
                  )
              )
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            }
            return null;
          }
        }
    );
  }

  @Override
  public boolean compareAndSwap(
      List<MetadataCASUpdate> updates
  )
  {
    return getDBI().inTransaction(
        TransactionIsolationLevel.REPEATABLE_READ,
        new TransactionCallback<Boolean>()
        {
          @Override
          public Boolean inTransaction(Handle handle, TransactionStatus transactionStatus)
          {
            List<byte[]> currentValues = new ArrayList<byte[]>();

            // Compare
            for (MetadataCASUpdate update : updates) {
              byte[] currentValue = handle
                  .createQuery(
                      StringUtils.format(
                          "SELECT %1$s FROM %2$s WHERE %3$s = :key FOR UPDATE",
                          update.getValueColumn(),
                          update.getTableName(),
                          update.getKeyColumn()
                      )
                  )
                  .bind("key", update.getKey())
                  .map(ByteArrayMapper.FIRST)
                  .first();

              if (!Arrays.equals(currentValue, update.getOldValue())) {
                return false;
              }
              currentValues.add(currentValue);
            }

            // Swap
            for (int i = 0; i < updates.size(); i++) {
              MetadataCASUpdate update = updates.get(i);
              byte[] currentValue = currentValues.get(i);

              if (currentValue == null) {
                handle.createStatement(
                    StringUtils.format(
                        "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value)",
                        update.getTableName(),
                        update.getKeyColumn(),
                        update.getValueColumn()
                    )
                )
                      .bind("key", update.getKey())
                      .bind("value", update.getNewValue())
                      .execute();
              } else {
                handle.createStatement(
                    StringUtils.format(
                        "UPDATE %1$s SET %3$s=:value WHERE %2$s=:key",
                        update.getTableName(),
                        update.getKeyColumn(),
                        update.getValueColumn()
                    )
                )
                      .bind("key", update.getKey())
                      .bind("value", update.getNewValue())
                      .execute();
              }
            }

            return true;
          }
        }
    );
  }

  public abstract DBI getDBI();

  @Override
  public void createDataSourceTable()
  {
    if (config.get().isCreateTables()) {
      createDataSourceTable(tablesConfigSupplier.get().getDataSourceTable());
    }
  }

  @Override
  public void createPendingSegmentsTable()
  {
    if (config.get().isCreateTables()) {
      createPendingSegmentsTable(tablesConfigSupplier.get().getPendingSegmentsTable());
    }
  }

  @Override
  public void createSegmentTable()
  {
    if (config.get().isCreateTables()) {
      createSegmentTable(tablesConfigSupplier.get().getSegmentsTable());
      alterSegmentTable();
    }
    // Called outside of the above conditional because we want to validate the table
    // regardless of cluster configuration for creating tables.
    validateSegmentsTable();
  }

  @Override
  public void createUpgradeSegmentsTable()
  {
    if (config.get().isCreateTables()) {
      createUpgradeSegmentsTable(tablesConfigSupplier.get().getUpgradeSegmentsTable());
    }
  }

  @Override
  public void createRulesTable()
  {
    if (config.get().isCreateTables()) {
      createRulesTable(tablesConfigSupplier.get().getRulesTable());
    }
  }

  @Override
  public void createConfigTable()
  {
    if (config.get().isCreateTables()) {
      createConfigTable(tablesConfigSupplier.get().getConfigTable());
    }
  }

  @Override
  public void createTaskTables()
  {
    if (config.get().isCreateTables()) {
      final MetadataStorageTablesConfig tablesConfig = tablesConfigSupplier.get();
      final String entryType = tablesConfig.getTaskEntryType();
      prepareTaskEntryTable(tablesConfig.getEntryTable(entryType));
      createLogTable(tablesConfig.getLogTable(entryType), entryType);
      createLockTable(tablesConfig.getLockTable(entryType), entryType);
    }
  }

  @Override
  public void createSupervisorsTable()
  {
    if (config.get().isCreateTables()) {
      createSupervisorsTable(tablesConfigSupplier.get().getSupervisorTable());
    }
  }

  @Override
  public @Nullable byte[] lookup(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key
  )
  {
    return getDBI().withHandle(
        handle -> lookupWithHandle(handle, tableName, keyColumn, valueColumn, key)
    );
  }

  public @Nullable byte[] lookupWithHandle(
      final Handle handle,
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key
  )
  {
    final String selectStatement = StringUtils.format(
        "SELECT %s FROM %s WHERE %s = :key", valueColumn,
        tableName, keyColumn
    );

    List<byte[]> matched = handle.createQuery(selectStatement)
                                 .bind("key", key)
                                 .map(ByteArrayMapper.FIRST)
                                 .list();

    if (matched.isEmpty()) {
      return null;
    }

    if (matched.size() > 1) {
      throw new ISE("Error! More than one matching entry[%d] found for [%s]?!", matched.size(), key);
    }

    return matched.get(0);
  }

  public MetadataStorageConnectorConfig getConfig()
  {
    return config.get();
  }

  protected static BasicDataSource makeDatasource(MetadataStorageConnectorConfig connectorConfig, String validationQuery)
  {
    BasicDataSource dataSource;

    try {
      Properties dbcpProperties = connectorConfig.getDbcpProperties();
      if (dbcpProperties != null) {
        dataSource = BasicDataSourceFactory.createDataSource(dbcpProperties);
      } else {
        dataSource = new BasicDataSourceExt(connectorConfig);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    dataSource.setValidationQuery(validationQuery);
    dataSource.setTestOnBorrow(true);

    return dataSource;
  }

  protected BasicDataSource getDatasource()
  {
    return makeDatasource(getConfig(), getValidationQuery());
  }

  public final <T> T inReadOnlyTransaction(
      final TransactionCallback<T> callback
  )
  {
    return getDBI().withHandle(
        new HandleCallback<T>()
        {
          @Override
          public T withHandle(Handle handle) throws Exception
          {
            final Connection connection = handle.getConnection();
            final boolean readOnly = connection.isReadOnly();
            connection.setReadOnly(true);
            try {
              return handle.inTransaction(callback);
            }
            finally {
              try {
                connection.setReadOnly(readOnly);
              }
              catch (SQLException e) {
                // at least try to log it so we don't swallow exceptions
                log.error(e, "Unable to reset connection read-only state");
              }
            }
          }
        }
    );
  }

  private void createAuditTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  audit_key VARCHAR(255) NOT NULL,\n"
                + "  type VARCHAR(255) NOT NULL,\n"
                + "  author VARCHAR(255) NOT NULL,\n"
                + "  comment VARCHAR(2048) NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  payload %3$s NOT NULL,\n"
                + "  PRIMARY KEY(id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_key_time ON %1$s(audit_key, created_date)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_type_time ON %1$s(type, created_date)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_audit_time ON %1$s(created_date)", tableName)
        )
    );
  }

  @Override
  public void createAuditTable()
  {
    if (config.get().isCreateTables()) {
      createAuditTable(tablesConfigSupplier.get().getAuditTable());
    }
  }

  @Override
  public void deleteAllRecords(final String tableName)
  {
    try {
      retryWithHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              if (tableExists(handle, tableName)) {
                log.info("Deleting all records from table[%s]", tableName);
                final Batch batch = handle.createBatch();
                batch.add("DELETE FROM " + tableName);
                batch.execute();
              } else {
                log.info("Table[%s] does not exit.", tableName);
              }
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.warn(e, "Exception while deleting records from table");
    }
  }

  public void createSegmentSchemaTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  datasource VARCHAR(255) NOT NULL,\n"
                + "  fingerprint VARCHAR(255) NOT NULL,\n"
                + "  payload %3$s NOT NULL,\n"
                + "  used BOOLEAN NOT NULL,\n"
                + "  used_status_last_updated VARCHAR(255) NOT NULL,\n"
                + "  version INTEGER NOT NULL,\n"
                + "  PRIMARY KEY (id),\n"
                + "  UNIQUE (fingerprint) \n"
                + ")",
                tableName, getSerialType(), getPayloadType()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_fingerprint ON %1$s(fingerprint)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_used ON %1$s(used)", tableName)
        )
    );
  }

  @Override
  public void createSegmentSchemasTable()
  {
    if (config.get().isCreateTables() && centralizedDatasourceSchemaConfig.isEnabled()) {
      createSegmentSchemaTable(tablesConfigSupplier.get().getSegmentSchemasTable());
    }
  }

  /**
   * Get the Set of the index on given table
   *
   * @param tableName name of the table to fetch the index map
   * @return Set of the uppercase index names, returns empty set if table does not exist
   */
  public Set<String> getIndexOnTable(String tableName)
  {
    Set<String> res = new HashSet<>();
    try {
      retryWithHandle(new HandleCallback<Void>()
      {
        @Override
        public Void withHandle(Handle handle) throws Exception
        {
          DatabaseMetaData databaseMetaData = handle.getConnection().getMetaData();
          // Fetch the index for given table
          ResultSet resultSet = getIndexInfo(databaseMetaData, tableName);
          while (resultSet.next()) {
            String indexName = resultSet.getString("INDEX_NAME");
            if (org.apache.commons.lang.StringUtils.isNotBlank(indexName)) {
              res.add(StringUtils.toUpperCase(indexName));
            }
          }
          return null;
        }
      });
    }
    catch (Exception e) {
      log.error(e, "Exception while listing the index on table %s ", tableName);
    }
    return ImmutableSet.copyOf(res);
  }

  /**
   * Get the ResultSet for indexInfo for given table
   *
   * @param databaseMetaData DatabaseMetaData
   * @param tableName        Name of table
   * @return ResultSet with index info
   */
  public ResultSet getIndexInfo(DatabaseMetaData databaseMetaData, String tableName) throws SQLException
  {
    return databaseMetaData.getIndexInfo(
        null,
        null,
        tableName,  // tableName is case-sensitive in mysql default setting
        false,
        false
    );
  }

  /**
   * create index on the table with retry if not already exist, to be called after createTable
   *
   * @param tableName       Name of the table to create index on
   * @param indexName       case-insensitive string index name, it helps to check the existing index on table
   * @param indexCols       List of columns to be indexed on
   * @param createdIndexSet
   */
  public void createIndex(
      final String tableName,
      final String indexName,
      final List<String> indexCols,
      final Set<String> createdIndexSet
  )
  {
    try {
      retryWithHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              if (!createdIndexSet.contains(StringUtils.toUpperCase(indexName))) {
                String indexSQL = StringUtils.format(
                    "CREATE INDEX %1$s ON %2$s(%3$s)",
                    indexName,
                    tableName,
                    Joiner.on(",").join(indexCols)
                );
                log.info("Creating Index on Table [%s], sql: [%s] ", tableName, indexSQL);
                handle.execute(indexSQL);
              } else {
                log.info("Index [%s] on Table [%s] already exists", indexName, tableName);
              }
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, StringUtils.format("Exception while creating index on table [%s]", tableName));
    }
  }

  /**
   * Checks table metadata to determine if the given column exists in the table.
   *
   * @return true if the column exists in the table, false otherwise
   */
  protected boolean tableHasColumn(String tableName, String columnName)
  {
    return getDBI().withHandle(handle -> {
      try {
        if (tableExists(handle, tableName)) {
          DatabaseMetaData dbMetaData = handle.getConnection().getMetaData();
          ResultSet columns = dbMetaData.getColumns(null, null, tableName, columnName);
          return columns.next();
        } else {
          return false;
        }
      }
      catch (SQLException e) {
        return false;
      }
    });
  }

  /**
   * Ensures that the "segments" table has a schema compatible with the current version of Druid.
   *
   * @throws RuntimeException if the "segments" table has an incompatible schema.
   *                          There is no recovering from an invalid schema, the program should crash.
   * @see <a href="https://druid.apache.org/docs/latest/operations/metadata-migration/">Metadata migration</a> for info
   * on manually preparing the "segments" table.
   */
  private void validateSegmentsTable()
  {
    String segmentsTables = tablesConfigSupplier.get().getSegmentsTable();

    boolean schemaPersistenceRequirementMet =
        !centralizedDatasourceSchemaConfig.isEnabled() ||
        (tableHasColumn(segmentsTables, "schema_fingerprint")
         && tableHasColumn(segmentsTables, "num_rows"));

    if (tableHasColumn(segmentsTables, "used_status_last_updated") && schemaPersistenceRequirementMet) {
      // do nothing
    } else {
      throw new ISE(
          "Cannot start Druid as table[%s] has an incompatible schema."
          + " Reason: One or all of these columns [used_status_last_updated, schema_fingerprint, num_rows] does not exist in table."
          + " See https://druid.apache.org/docs/latest/operations/upgrade-prep.html for more info on remediation.",
          tablesConfigSupplier.get().getSegmentsTable()
      );
    }
  }
}
