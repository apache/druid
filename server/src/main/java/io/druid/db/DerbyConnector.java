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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import org.apache.derby.drda.NetworkServerControl;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.net.InetAddress;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class DerbyConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(DerbyConnector.class);
  private final DBI dbi;

  @Inject
  public DerbyConnector(Supplier<MetadataDbConnectorConfig> config, Supplier<MetadataTablesConfig> dbTables)
  {
    super(config, dbTables);
    this.dbi = new DBI(getConnectionFactory("druidDerbyDb"));
  }

  public void createTable(final IDBI dbi, final String tableName, final String sql)
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              List<Map<String, Object>> tables = handle.select("select * from SYS.SYSTABLES");
              boolean tableExists = false;
              String existingTableName = "";
              String tableNameUpper = tableName.toUpperCase();
              for (Map<String, Object> t : tables) {
                if (t.get("tablename").equals(tableNameUpper)) {
                  tableExists = true;
                  existingTableName = (String)t.get("tablename");
                }
              }
              if (!tableExists) {
                log.info("Creating table[%s]", tableName);
                handle.createStatement(sql).execute();
              } else {
                log.info("Table[%s] existed: [%s]", tableName, existingTableName);
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

  public void createIndex(final IDBI dbi, final String tableName, final String indexName, final String columnName) {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("CREATE INDEX %1$s ON %2$s(%3$s)", indexName, tableName, columnName)).execute();
            return null;
          }
        }
    );
  }

  public void createSegmentTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format(
            "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date VARCHAR(255) NOT NULL, "
            + "start VARCHAR(255) NOT NULL, \"end\" VARCHAR(255) NOT NULL, partitioned SMALLINT NOT NULL, version VARCHAR(255) NOT NULL, "
            + "used BOOLEAN NOT NULL, payload CLOB NOT NULL, PRIMARY KEY (id))",
            tableName
        )
    );
    createIndex(dbi, tableName, "segment_dataSource", "dataSource");
    createIndex(dbi, tableName, "segment_used", "used");
  }

  public void createRulesTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format(
            "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version VARCHAR(255) NOT NULL, payload CLOB NOT NULL, PRIMARY KEY (id))",
            tableName
        )
    );
    createIndex(dbi, tableName, "rules_dataSource", "dataSource");
  }

  public void createConfigTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format(
            "CREATE table %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(name))",
            tableName
        )
    );
  }

  public void createTaskTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format("CREATE TABLE %s (id VARCHAR(255) NOT NULL, created_date VARCHAR(255) NOT NULL, "
                      + "datasource VARCHAR(255) NOT NULL, payload CLOB NOT NULL, status_payload CLOB NOT NULL, "
                      + "active SMALLINT NOT NULL DEFAULT 0, PRIMARY KEY (id))",
            tableName
        )
    );
    createIndex(dbi, tableName, "task_active_created_date", "active, created_date");
  }

  public void createTaskLogTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format(
            "CREATE TABLE %s (id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + "task_id VARCHAR(255) DEFAULT NULL, log_payload CLOB, PRIMARY KEY (id)); "
            + "CREATE INDEX task_log_task_id ON %1$s(task_id)",
            tableName
        )
    );
    createIndex(dbi, tableName, "task_log_task_id", "task_id");
  }

  public void createTaskLockTable(final IDBI dbi, final String tableName)
  {
    createTable(
        dbi,
        tableName,
        String.format(
            "CREATE TABLE %s (id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
            + "task_id VARCHAR(255) DEFAULT NULL, lock_payload CLOB, PRIMARY KEY (id))",
            tableName
        )
    );
    createIndex(dbi, tableName, "task_lock_task_id", "task_id");
  }

  public String insertOrUpdateStatement(final String tableName, final String keyColumn, final String valueColumn)
  {
    return null;
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
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            Connection conn = getDBI().open().getConnection();
            handle.begin();
            conn.setAutoCommit(false);
            List<Map<String, Object>> entry = handle.createQuery(
                String.format("SELECT * FROM %1$s WHERE %2$s=:key", storageName, keyColumn)
            ).list();
            if (entry == null || entry.isEmpty()) {
              handle.createStatement(String.format("INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value)", storageName, keyColumn, valueColumn))
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            } else {
              handle.createStatement(String.format("UPDATE %1$s SET %3$s=:value WHERE %2$s=:key", storageName, keyColumn, valueColumn))
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

  public DBI getDBI() { return dbi; }

  protected ConnectionFactory getConnectionFactory(String dbName)
  {
    try {
      NetworkServerControl server = new NetworkServerControl
          (InetAddress.getByName("localhost"),1527);
      server.start(null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new DerbyConnectionFactory(dbName);
  }
}
