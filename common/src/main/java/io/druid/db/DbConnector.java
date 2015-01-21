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
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
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
import java.util.Map;

/**
 */
public class DbConnector
{
  private static final Logger log = new Logger(DbConnector.class);

  public static void createSegmentTable(final IDBI dbi, final String segmentTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        segmentTableName,
        String.format(
            isPostgreSQL ?
                "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TEXT NOT NULL, start TEXT NOT NULL, \"end\" TEXT NOT NULL, partitioned SMALLINT NOT NULL, version TEXT NOT NULL, used BOOLEAN NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));" +
                "CREATE INDEX ON %1$s(dataSource);"+
                "CREATE INDEX ON %1$s(used);":
                "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TINYTEXT NOT NULL, start TINYTEXT NOT NULL, end TINYTEXT NOT NULL, partitioned BOOLEAN NOT NULL, version TINYTEXT NOT NULL, used BOOLEAN NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), INDEX(used), PRIMARY KEY (id)) DEFAULT CHARACTER SET utf8",
            segmentTableName
        ),
        isPostgreSQL
    );
  }

  public static void createRuleTable(final IDBI dbi, final String ruleTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        ruleTableName,
        String.format(
            isPostgreSQL ?
                "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TEXT NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));"+
                "CREATE INDEX ON %1$s(dataSource);":
                "CREATE table %s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TINYTEXT NOT NULL, payload LONGTEXT NOT NULL, INDEX(dataSource), PRIMARY KEY (id)) DEFAULT CHARACTER SET utf8",
            ruleTableName
        ),
        isPostgreSQL
    );
  }

  public static void createConfigTable(final IDBI dbi, final String configTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        configTableName,
        String.format(
            isPostgreSQL ?
                "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload bytea NOT NULL, PRIMARY KEY(name))":
                "CREATE table %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(name)) DEFAULT CHARACTER SET utf8",
            configTableName
        ),
        isPostgreSQL
    );
  }

  public static void createTaskTable(final IDBI dbi, final String taskTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        taskTableName,
        String.format(
            isPostgreSQL ?
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
                + ") DEFAULT CHARACTER SET utf8",
            taskTableName
        ),
        isPostgreSQL
    );
  }

  public static void createTaskLogTable(final IDBI dbi, final String taskLogsTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        taskLogsTableName,
        String.format(
            isPostgreSQL ?
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
                + ") DEFAULT CHARACTER SET utf8",
            taskLogsTableName
        ),
        isPostgreSQL
    );
  }

  public static void createTaskLockTable(final IDBI dbi, final String taskLocksTableName, boolean isPostgreSQL)
  {
    createTable(
        dbi,
        taskLocksTableName,
        String.format(
            isPostgreSQL ?
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
                + ") DEFAULT CHARACTER SET utf8",
            taskLocksTableName
        ),
        isPostgreSQL
    );
  }

  public static void createTable(
      final IDBI dbi,
      final String tableName,
      final String sql,
      final boolean isPostgreSQL
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
              if ( isPostgreSQL ) {
                table = handle.select(String.format("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '%s'", tableName));
              } else {
                // ensure database defaults to utf8, otherwise bail
                boolean isUtf8 =
                    handle.createQuery("SHOW VARIABLES where variable_name = 'character_set_database' and value = 'utf8'")
                          .list()
                          .size() == 1;

                if(!isUtf8) {
                  throw new ISE(
                      "Database default character set is not UTF-8." + System.lineSeparator()
                    + "  Druid requires its MySQL database to be created using UTF-8 as default character set."
                    + " If you are upgrading from previous version of Druid, please make all tables have been converted to utf8 and change the database default."
                    + " For more information on how to convert and set the default, please refer to section on updating in the Druid 0.6.171 release notes."
                    );
                }
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

  public Void insertOrUpdate(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value
  ) throws SQLException
  {
    final String insertOrUpdateStatement = String.format(
        isPostgreSQL ?
        "BEGIN;\n" +
        "LOCK TABLE %1$s IN SHARE ROW EXCLUSIVE MODE;\n" +
        "WITH upsert AS (UPDATE %1$s SET %3$s=:value WHERE %2$s=:key RETURNING *)\n" +
        "    INSERT INTO %1$s (%2$s, %3$s) SELECT :key, :value WHERE NOT EXISTS (SELECT * FROM upsert)\n;" +
        "COMMIT;" :
        "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value) ON DUPLICATE KEY UPDATE %3$s = :value",
        tableName, keyColumn, valueColumn
    );

    return dbi.withHandle(
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

  public byte[] lookup(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key
  )
  {
    final String selectStatement = String.format("SELECT %s FROM %s WHERE %s = :key", valueColumn, tableName, keyColumn);

    return dbi.withHandle(
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

  protected static Boolean isPostgreSQL(final Handle handle) throws SQLException
  {
    return handle.getConnection().getMetaData().getDatabaseProductName().contains("PostgreSQL");
  }

  private final Supplier<DbConnectorConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final DBI dbi;
  private boolean isPostgreSQL = false;

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

  public boolean isPostgreSQL()
  {
    return isPostgreSQL;
  }

  private DataSource getDatasource()
  {
    DbConnectorConfig connectorConfig = config.get();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    isPostgreSQL = uri.startsWith("jdbc:postgresql");
    dataSource.setUrl(uri);

    if (connectorConfig.isUseValidationQuery()) {
      dataSource.setValidationQuery(connectorConfig.getValidationQuery());
      dataSource.setTestOnBorrow(true);
    }

    return dataSource;
  }

  public void createSegmentTable()
  {
    if (config.get().isCreateTables()) {
      createSegmentTable(dbi, dbTables.get().getSegmentsTable(), isPostgreSQL);
    }
  }

  public void createRulesTable()
  {
    if (config.get().isCreateTables()) {
      createRuleTable(dbi, dbTables.get().getRulesTable(), isPostgreSQL);
    }
  }

  public void createConfigTable()
  {
    if (config.get().isCreateTables()) {
      createConfigTable(dbi, dbTables.get().getConfigTable(), isPostgreSQL);
    }
  }

  public void createTaskTables()
  {
    if (config.get().isCreateTables()) {
      final DbTablesConfig dbTablesConfig = dbTables.get();
      createTaskTable(dbi, dbTablesConfig.getTasksTable(), isPostgreSQL);
      createTaskLogTable(dbi, dbTablesConfig.getTaskLogTable(), isPostgreSQL);
      createTaskLockTable(dbi, dbTablesConfig.getTaskLockTable(), isPostgreSQL);
    }
  }
}
