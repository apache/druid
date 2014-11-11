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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.sql.Connection;
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
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            Connection conn = getDBI().open().getConnection();
            handle.begin();
            conn.setAutoCommit(false);
            int count = handle
                .createQuery(
                    String.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", tableName, keyColumn)
                )
                .bind("key", key)
                .map(IntegerMapper.FIRST)
                .first();
            if (count == 0) {
              handle.createStatement(String.format("INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value)",
                                                   tableName, keyColumn, valueColumn))
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            } else {
              handle.createStatement(String.format("UPDATE %1$s SET %3$s=:value WHERE %2$s=:key",
                                                   tableName, keyColumn, valueColumn))
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            }
            conn.setAutoCommit(true);
            handle.commit();
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

}
