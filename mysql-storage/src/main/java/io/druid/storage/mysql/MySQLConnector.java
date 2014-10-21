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

package io.druid.storage.mysql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.db.MetadataStorageConnectorConfig;
import io.druid.db.MetadataStorageTablesConfig;
import io.druid.db.SQLMetadataConnector;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class MySQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(MySQLConnector.class);
  private final DBI dbi;

  @Inject
  public MySQLConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);
    this.dbi = new DBI(getDatasource());
    dbi.withHandle(new HandleCallback<Void>()
                   {
                     @Override
                     public Void withHandle(Handle handle) throws Exception
                     {
                       handle.createStatement("SET sql_mode='ANSI_QUOTES'").execute();
                       return null;
                     }
                   });
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
            List<Map<String, Object>> table = handle.select(String.format("SHOW tables LIKE '%s'", tableName));
            if (table.isEmpty()) {
              log.info("Creating table[%s]", tableName);
              handle.createStatement(sql).execute();
            } else {
              log.info("Table[%s] existed: [%s]", tableName, table);
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
      String.format(
        "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, "
        + "start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, "
        + "used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id))",
        tableName
      )
    );
  }

  public void createRulesTable(final IDBI dbi, final String tableName)
  {
    createTable(
      dbi,
      tableName,
      String.format(
        "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TINYTEXT NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), PRIMARY KEY (id))",
        tableName
      )
    );
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
      String.format(
        "CREATE TABLE `%s` (\n"
        + "  `id` varchar(255) NOT NULL,\n"
        + "  `created_date` tinytext NOT NULL,\n"
        + "  `datasource` varchar(255) NOT NULL,\n"
        + "  `payload` longblob NOT NULL,\n"
        + "  `status_payload` longblob NOT NULL,\n"
        + "  `active` tinyint(1) NOT NULL DEFAULT '0',\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY (active, created_date(100))\n"
        + ")",
        tableName
      )
    );
  }

  public void createTaskLogTable(final IDBI dbi, final String tableName)
  {
    createTable(
      dbi,
      tableName,
      String.format(
        "CREATE TABLE `%s` (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `task_id` varchar(255) DEFAULT NULL,\n"
        + "  `log_payload` longblob,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `task_id` (`task_id`)\n"
        + ")",
        tableName
      )
    );
  }

  public void createTaskLockTable(final IDBI dbi, final String tableName)
  {
    createTable(
      dbi,
      tableName,
      String.format(
        "CREATE TABLE `%s` (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `task_id` varchar(255) DEFAULT NULL,\n"
        + "  `lock_payload` longblob,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `task_id` (`task_id`)\n"
        + ")",
        tableName
      )
    );
  }

  public String insertOrUpdateStatement(final String tableName, final String keyColumn, final String valueColumn)
  {
    return String.format(
        "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value) ON DUPLICATE KEY UPDATE %3$s = :value",
        tableName, keyColumn, valueColumn
    );
  }

  public DBI getDBI() { return dbi; }

}
