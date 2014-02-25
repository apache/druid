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
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import org.apache.commons.dbcp.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 */
public class DbConnector
{
  private static final Logger log = new Logger(DbConnector.class);

  public static void createSegmentTable(final IDBI dbi, final String segmentTableName)
  {
    createTable(
        dbi,
        segmentTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TEXT NOT NULL, start TEXT NOT NULL, \"end\" TEXT NOT NULL, partitioned SMALLINT NOT NULL, version TEXT NOT NULL, used BOOLEAN NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));" +
                "CREATE INDEX ON %1$s(dataSource);"+
                "CREATE INDEX ON %1$s(used);":
                "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id))",
            segmentTableName
        )
    );
  }

  public static void createRuleTable(final IDBI dbi, final String ruleTableName)
  {
    createTable(
        dbi,
        ruleTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TEXT NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));"+
                "CREATE INDEX ON %1$s(dataSource);":
                "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TINYTEXT NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), PRIMARY KEY (id))",
            ruleTableName
        )
    );
  }

  public static void createConfigTable(final IDBI dbi, final String configTableName)
  {
    createTable(
        dbi,
        configTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload bytea NOT NULL, PRIMARY KEY(name))":
                "CREATE table %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(name))",
            configTableName
        )
    );
  }

  public static void createTaskTable(final IDBI dbi, final String taskTableName)
  {
    createTable(
        dbi,
        taskTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %1$s (\n"
                + "  id varchar(255) NOT NULL,\n"
                + "  created_date TEXT NOT NULL,\n"
                + "  datasource varchar(255) NOT NULL,\n"
                + "  payload bytea NOT NULL,\n"
                + "  status_payload bytea NOT NULL,\n"
                + "  active SMALLINT NOT NULL DEFAULT '0',\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n" +
                "CREATE INDEX ON %1$s(active, created_date);":
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
            taskTableName
        )
    );
  }

  public static void createTaskLogTable(final IDBI dbi, final String taskLogsTableName)
  {
    createTable(
        dbi,
        taskLogsTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %1$s (\n"
                + "  id bigserial NOT NULL,\n"
                + "  task_id varchar(255) DEFAULT NULL,\n"
                + "  log_payload bytea,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"+
                "CREATE INDEX ON %1$s(task_id);":
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

  public static void createTaskLockTable(final IDBI dbi, final String taskLocksTableName)
  {
    createTable(
        dbi,
        taskLocksTableName,
        String.format(
            isPostgreSQL(dbi) ?
                "CREATE TABLE %1$s (\n"
                + "  id bigserial NOT NULL,\n"
                + "  task_id varchar(255) DEFAULT NULL,\n"
                + "  lock_payload bytea,\n"
                + "  PRIMARY KEY (id)\n"
                + ");\n"+
                "CREATE INDEX ON %1$s(task_id);":
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
      final IDBI dbi,
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
              List<Map<String, Object>> table;
              if ( isPostgreSQL(dbi) ) {
                table = handle.select(String.format("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '%s'", tableName));
              } else {
                table = handle.select(String.format("SHOW tables LIKE '%s'", tableName));
              }

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

  public static Boolean isPostgreSQL(final IDBI dbi)
  {
    return dbi.withHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return isPostgreSQL(handle);
          }
        }
    );
  }

  public static Boolean isPostgreSQL(final Handle handle) throws SQLException
  {
    return handle.getConnection().getMetaData().getDatabaseProductName().contains("PostgreSQL");
  }

  private final Supplier<DbConnectorConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final DBI dbi;

  @Inject
  public DbConnector(Supplier<DbConnectorConfig> config, Supplier<DbTablesConfig> dbTables)
  {
    this.config = config;
    this.dbTables = dbTables;

    this.dbi = new DBI(getDatasource());
  }

  public DBI getDBI()
  {
    return dbi;
  }

  private DataSource getDatasource()
  {
    DbConnectorConfig connectorConfig = config.get();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    dataSource.setUrl(connectorConfig.getConnectURI());

    if (connectorConfig.isUseValidationQuery()) {
      dataSource.setValidationQuery(connectorConfig.getValidationQuery());
      dataSource.setTestOnBorrow(true);
    }

    return dataSource;
  }

  public void createSegmentTable()
  {
    if (config.get().isCreateTables()) {
      createSegmentTable(dbi, dbTables.get().getSegmentsTable());
    }
  }

  public void createRulesTable()
  {
    if (config.get().isCreateTables()) {
      createRuleTable(dbi, dbTables.get().getRulesTable());
    }
  }

  public void createConfigTable()
  {
    if (config.get().isCreateTables()) {
      createConfigTable(dbi, dbTables.get().getConfigTable());
    }
  }

  public void createTaskTables()
  {
    if (config.get().isCreateTables()) {
      final DbTablesConfig dbTablesConfig = dbTables.get();
      createTaskTable(dbi, dbTablesConfig.getTasksTable());
      createTaskLogTable(dbi, dbTablesConfig.getTaskLogTable());
      createTaskLockTable(dbi, dbTablesConfig.getTaskLockTable());
    }
  }
}
