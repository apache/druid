/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.db;

import com.metamx.common.logger.Logger;
import org.apache.commons.dbcp.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 */
public class DbConnector
{
  private static final Logger log = new Logger(DbConnector.class);

  public static void createSegmentTable(final DBI dbi, final String segmentTableName)
  {
    createTable(
        dbi,
        segmentTableName,
        String.format(
            "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id))",
            segmentTableName
        )
    );
  }

  public static void createRuleTable(final DBI dbi, final String ruleTableName)
  {
    createTable(
        dbi,
        ruleTableName,
        String.format(
            "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TINYTEXT NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), PRIMARY KEY (id))",
            ruleTableName
        )
    );
  }

  public static void createConfigTable(final DBI dbi, final String configTableName)
  {
    createTable(
        dbi,
        configTableName,
        String.format(
            "CREATE table %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(name))",
            configTableName
        )
    );
  }

  public static void createTaskTable(final DBI dbi, final String taskTableName)
  {
    createTable(
        dbi,
        taskTableName,
        String.format(
            "CREATE TABLE `%s` (\n"
            + "  `id` varchar(255) NOT NULL,\n"
            + "  `created_date` tinytext NOT NULL,\n"
            + "  `datasource` varchar(255) NOT NULL,\n"
            + "  `payload` longblob NOT NULL,\n"
            + "  `status_payload` longblob NOT NULL,\n"
            + "  `active` tinyint(1) NOT NULL DEFAULT '0',\n"
            + "  PRIMARY KEY (`id`)\n"
            + ")",
            taskTableName
        )
    );
  }

  public static void createTaskLogTable(final DBI dbi, final String taskLogsTableName)
  {
    createTable(
        dbi,
        taskLogsTableName,
        String.format(
            "CREATE TABLE `%s` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `task_id` varchar(255) DEFAULT NULL,\n"
            + "  `log_payload` longblob,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `task_id` (`task_id`)\n"
            + ")",
            taskLogsTableName
        )
    );
  }

  public static void createTaskLockTable(final DBI dbi, final String taskLocksTableName)
  {
    createTable(
        dbi,
        taskLocksTableName,
        String.format(
            "CREATE TABLE `%s` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `task_id` varchar(255) DEFAULT NULL,\n"
            + "  `lock_payload` longblob,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `task_id` (`task_id`)\n"
            + ")",
            taskLocksTableName
        )
    );
  }

  public static void createTable(
      final DBI dbi,
      final String tableName,
      final String sql
  )
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              if ( !handle.getConnection().getMetaData().getDatabaseProductName().contains("PostgreSQL") ) {
                List<Map<String, Object>> table = handle.select(String.format("SHOW tables LIKE '%s'", tableName));

                if (table.isEmpty()) {
                  log.info("Creating table[%s]", tableName);
                  handle.createStatement(sql).execute();
                } else {
                  log.info("Table[%s] existed: [%s]", tableName, table);
                }
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

  private final DbConnectorConfig config;
  private final DBI dbi;

  public DbConnector(DbConnectorConfig config)
  {
    this.config = config;

    this.dbi = new DBI(getDatasource());
  }

  public DBI getDBI()
  {
    return dbi;
  }

  private DataSource getDatasource()
  {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(config.getDatabaseUser());
    dataSource.setPassword(config.getDatabasePassword());
    dataSource.setUrl(config.getDatabaseConnectURI());

    if (config.useValidationQuery()) {
      dataSource.setValidationQuery(config.getValidationQuery());
      dataSource.setTestOnBorrow(true);
    }

    return dataSource;
  }
}
