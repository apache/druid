/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.metadata;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.util.List;

public abstract class SQLMetadataConnector implements MetadataStorageConnector
{
  private static final Logger log = new Logger(SQLMetadataConnector.class);
  private static final String PAYLOAD_TYPE = "BLOB";

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final Supplier<MetadataStorageTablesConfig> tablesConfigSupplier;

  public SQLMetadataConnector(Supplier<MetadataStorageConnectorConfig> config,
                              Supplier<MetadataStorageTablesConfig> tablesConfigSupplier
  )
  {
    this.config = config;
    this.tablesConfigSupplier = tablesConfigSupplier;
  }

  /**
   * SQL type to use for payload data (e.g. JSON blobs).
   * Must be a binary type, which values can be accessed using ResultSet.getBytes()
   *
   * The resulting string will be interpolated into the table creation statement, e.g.
   * <code>CREATE TABLE druid_table ( payload <type> NOT NULL, ... )</code>
   *
   * @return String representing the SQL type
   */
  protected String getPayloadType() {
    return PAYLOAD_TYPE;
  }

  /**
   * Auto-incrementing SQL type to use for IDs
   * Must be an integer type, which values will be automatically set by the database
   *
   * The resulting string will be interpolated into the table creation statement, e.g.
   * <code>CREATE TABLE druid_table ( id <type> NOT NULL, ... )</code>
   *
   * @return String representing the SQL type and auto-increment statement
   */
  protected abstract String getSerialType();

  public String getValidationQuery() { return "SELECT 1"; }

  public abstract boolean tableExists(Handle handle, final String tableName);

  protected boolean isTransientException(Throwable e) {
    return false;
  }

  public void createTable(final IDBI dbi, final String tableName, final Iterable<String> sql)
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              if (!tableExists(handle, tableName)) {
                log.info("Creating table[%s]", tableName);
                final Batch batch = handle.createBatch();
                for(String s : sql) {
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

  public void createSegmentTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  dataSource VARCHAR(255) NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  start VARCHAR(255) NOT NULL,\n"
                + "  \"end\" VARCHAR(255) NOT NULL,\n"
                + "  partitioned BOOLEAN NOT NULL,\n"
                + "  version VARCHAR(255) NOT NULL,\n"
                + "  used BOOLEAN NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType()
            ),
            String.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName),
            String.format("CREATE INDEX idx_%1$s_used ON %1$s(used)", tableName)
        )
    );
  }

  public void createRulesTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  dataSource VARCHAR(255) NOT NULL,\n"
                + "  version VARCHAR(255) NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType()
            ),
            String.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName)
        )
    );
  }

  public void createConfigTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
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

  public void createEntryTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
                "CREATE TABLE %1$s (\n"
                + "  id VARCHAR(255) NOT NULL,\n"
                + "  created_date VARCHAR(255) NOT NULL,\n"
                + "  datasource VARCHAR(255) NOT NULL,\n"
                + "  payload %2$s NOT NULL,\n"
                + "  status_payload %2$s NOT NULL,\n"
                + "  active BOOLEAN NOT NULL DEFAULT FALSE,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getPayloadType()
            ),
            String.format("CREATE INDEX idx_%1$s_active_created_date ON %1$s(active, created_date)", tableName)
        )
    );
  }

  public void createLogTable(final IDBI dbi, final String tableName, final String entryTypeName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  %4$s_id VARCHAR(255) DEFAULT NULL,\n"
                + "  log_payload %3$s,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType(), entryTypeName
            ),
            String.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
        )
    );
  }

  public void createLockTable(final IDBI dbi, final String tableName, final String entryTypeName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
                "CREATE TABLE %1$s (\n"
                + "  id %2$s NOT NULL,\n"
                + "  %4$s_id VARCHAR(255) DEFAULT NULL,\n"
                + "  lock_payload %3$s,\n"
                + "  PRIMARY KEY (id)\n"
                + ")",
                tableName, getSerialType(), getPayloadType(), entryTypeName
            ),
            String.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
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
  ) throws Exception
  {
    return getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = handle
                .createQuery(
                    String.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", tableName, keyColumn)
                )
                .bind("key", key)
                .map(IntegerMapper.FIRST)
                .first();
            if (count == 0) {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value)",
                      tableName, keyColumn, valueColumn
                  )
              )
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            } else {
              handle.createStatement(
                  String.format(
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

  public abstract DBI getDBI();

  @Override
  public void createSegmentTable() {
    if (config.get().isCreateTables()) {
      createSegmentTable(getDBI(), tablesConfigSupplier.get().getSegmentsTable());
    }
  }

  @Override
  public void createRulesTable() {
    if (config.get().isCreateTables()) {
      createRulesTable(getDBI(), tablesConfigSupplier.get().getRulesTable());
    }
  }

  @Override
  public void createConfigTable() {
    if (config.get().isCreateTables()) {
      createConfigTable(getDBI(), tablesConfigSupplier.get().getConfigTable());
    }
  }

  @Override
  public void createTaskTables() {
    if (config.get().isCreateTables()) {
      final MetadataStorageTablesConfig tablesConfig = tablesConfigSupplier.get();
      final String entryType = tablesConfig.getTaskEntryType();
      createEntryTable(getDBI(), tablesConfig.getEntryTable(entryType));
      createLogTable(getDBI(), tablesConfig.getLogTable(entryType), entryType);
      createLockTable(getDBI(), tablesConfig.getLockTable(entryType), entryType);
    }
  }

  @Override
  public byte[] lookup(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key
  )
  {
    final String selectStatement = String.format("SELECT %s FROM %s WHERE %s = :key", valueColumn,
                                                 tableName, keyColumn);

    return getDBI().withHandle(
        new HandleCallback<byte[]>()
        {
          @Override
          public byte[] withHandle(Handle handle) throws Exception
          {
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
        }
    );
  }

  public MetadataStorageConnectorConfig getConfig() { return config.get(); }

  protected BasicDataSource getDatasource()
  {
    MetadataStorageConnectorConfig connectorConfig = getConfig();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    dataSource.setValidationQuery(getValidationQuery());
    dataSource.setTestOnBorrow(true);

    return dataSource;
  }

  private void createAuditTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        ImmutableList.of(
            String.format(
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
            String.format("CREATE INDEX idx_%1$s_key_time ON %1$s(audit_key, created_date)", tableName),
            String.format("CREATE INDEX idx_%1$s_type_time ON %1$s(audit_key, created_date)", tableName),
            String.format("CREATE INDEX idx_%1$s_audit_time ON %1$s(created_date)", tableName)
        )
    );
  }
  @Override
  public void createAuditTable() {
    if (config.get().isCreateTables()) {
      createAuditTable(getDBI(), tablesConfigSupplier.get().getAuditTable());
    }
  }

}
