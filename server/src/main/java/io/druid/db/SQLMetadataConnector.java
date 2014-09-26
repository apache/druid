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

package io.druid.db;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import org.apache.commons.dbcp.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public abstract class SQLMetadataConnector implements MetadataDbConnector
{
  private final Supplier<MetadataDbConnectorConfig> config;
  private final Supplier<MetadataTablesConfig> dbTables;

  protected SQLMetadataConnector(Supplier<MetadataDbConnectorConfig> config, Supplier<MetadataTablesConfig> dbTables)
  {
    this.config = config;
    this.dbTables = dbTables;

  }

  public abstract void createTable(final IDBI dbi, final String tableName, final String sql);

  public abstract void createSegmentTable(final IDBI dbi, final String tableName);

  public abstract void createRulesTable(final IDBI dbi, final String tableName);

  public abstract void createConfigTable(final IDBI dbi, final String tableName);

  public abstract void createTaskTable(final IDBI dbi, final String tableName);

  public abstract void createTaskLogTable(final IDBI dbi, final String taskLogsTableName);

  public abstract void createTaskLockTable(final IDBI dbi, final String taskLocksTableName);

  public abstract String insertOrUpdateStatement(final String tableName, final String keyColumn, final String valueColumn);

  public abstract DBI getDBI();

  @Override
  public void createSegmentTable() {
    if (config.get().isCreateTables()) {
      createSegmentTable(getDBI(), dbTables.get().getSegmentsTable());
    }
  }

  @Override
  public void createRulesTable() {
    if (config.get().isCreateTables()) {
      createRulesTable(getDBI(), dbTables.get().getRulesTable());
    }
  }

  @Override
  public void createConfigTable() {
    if (config.get().isCreateTables()) {
      createConfigTable(getDBI(), dbTables.get().getConfigTable());
    }
  }

  @Override
  public void createTaskTables() {
    if (config.get().isCreateTables()) {
      final MetadataTablesConfig metadataTablesConfig = dbTables.get();
      createTaskTable(getDBI(), metadataTablesConfig.getTasksTable());
      createTaskLogTable(getDBI(), metadataTablesConfig.getTaskLogTable());
      createTaskLockTable(getDBI(), metadataTablesConfig.getTaskLockTable());
    }
  }

  @Override
  public Void insertOrUpdate(
      final String storageName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value
  ) throws Exception
  {
    final String insertOrUpdateStatement = insertOrUpdateStatement(storageName, keyColumn, valueColumn);

    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(insertOrUpdateStatement)
                  .bind("key", key)
                  .bind("value", value)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public byte[] lookup(
      final String storageName,
      final String keyColumn,
      final String valueColumn,
      final String key
  )
  {
    final String selectStatement = String.format("SELECT %s FROM %s WHERE %s = :key", valueColumn,
                                                 storageName, keyColumn);

    return getDBI().withHandle(
        new HandleCallback<byte[]>()
        {
          @Override
          public byte[] withHandle(Handle handle) throws Exception
          {
            List<byte[]> matched = handle.createQuery(selectStatement)
                                         .bind("key", key)
                                         .map(
                                             new ResultSetMapper<byte[]>()
                                             {
                                               @Override
                                               public byte[] map(int index, ResultSet r, StatementContext ctx)
                                                   throws SQLException
                                               {
                                                 return r.getBytes(valueColumn);
                                               }
                                             }
                                         ).list();

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

  public MetadataDbConnectorConfig getConfig() { return config.get(); }

  protected DataSource getDatasource()
  {
    MetadataDbConnectorConfig connectorConfig = config.get();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    if (connectorConfig.isUseValidationQuery()) {
      dataSource.setValidationQuery(connectorConfig.getValidationQuery());
      dataSource.setTestOnBorrow(true);
    }

    return dataSource;
  }
}
