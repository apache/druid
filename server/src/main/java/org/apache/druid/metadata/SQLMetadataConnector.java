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

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
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
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class SQLMetadataConnector implements MetadataStorageConnector
{
  private static final Logger log = new Logger(SQLMetadataConnector.class);
  private static final String PAYLOAD_TYPE = "BLOB";
  private static final String COLLATION = "";

  static final int DEFAULT_MAX_TRIES = 10;

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final Supplier<MetadataStorageTablesConfig> tablesConfigSupplier;
  private final Predicate<Throwable> shouldRetry;

  public SQLMetadataConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfigSupplier
  )
  {
    this.config = config;
    this.tablesConfigSupplier = tablesConfigSupplier;
    this.shouldRetry = this::isTransientException;
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
   * Auto-incrementing SQL type to use for IDs
   * Must be an integer type, which values will be automatically set by the database
   * <p/>
   * The resulting string will be interpolated into the table creation statement, e.g.
   * <code>CREATE TABLE druid_table ( id <type> NOT NULL, ... )</code>
   *
   * @return String representing the SQL type and auto-increment statement
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
                         || e instanceof UnableToExecuteStatementException
                         || connectorIsTransientException(e)
                         || (e instanceof SQLException && isTransientException(e.getCause()))
                         || (e instanceof DBIException && isTransientException(e.getCause())));
  }

  protected boolean connectorIsTransientException(Throwable e)
  {
    return false;
  }

  public void createTable(final String tableName, final Iterable<String> sql)
  {
    try {
      retryWithHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle)
            {
              if (!tableExists(handle, tableName)) {
                log.info("Creating table[%s]", tableName);
                final Batch batch = handle.createBatch();
                for (String s : sql) {
                  batch.add(s);
                }
                batch.execute();
              } else {
                log.info("Table[%s] already exists", tableName);
              }
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.warn(e, "Exception creating table");
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
                + "  partitioned BOOLEAN NOT NULL,\n"
                + "  version VARCHAR(255) NOT NULL,\n"
                + "  used BOOLEAN NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType(), getQuoteString(), getCollation()
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_used ON %1$s(used)", tableName),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_datasource_used_end ON %1$s(dataSource, used, %2$send%2$s)",
                tableName,
                getQuoteString()
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
            ),
            StringUtils.format("CREATE INDEX idx_%1$s_active_created_date ON %1$s(active, created_date)", tableName)
        )
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
      createEntryTable(tablesConfig.getEntryTable(entryType));
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
        new HandleCallback<byte[]>()
        {
          @Override
          public byte[] withHandle(Handle handle)
          {
            return lookupWithHandle(handle, tableName, keyColumn, valueColumn, key);
          }
        }
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

  protected BasicDataSource getDatasource()
  {
    MetadataStorageConnectorConfig connectorConfig = getConfig();

    BasicDataSource dataSource;

    try {
      Properties dbcpProperties = connectorConfig.getDbcpProperties();
      if (dbcpProperties != null) {
        dataSource = BasicDataSourceFactory.createDataSource(dbcpProperties);
      } else {
        dataSource = new BasicDataSource();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    dataSource.setValidationQuery(getValidationQuery());
    dataSource.setTestOnBorrow(true);

    return dataSource;
  }

  protected final <T> T inReadOnlyTransaction(
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
}
