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

package io.druid.storage.jdbc.postgresql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.db.MetadataDbConnectorConfig;
import io.druid.db.MetadataTablesConfig;
import io.druid.db.SQLMetadataConnector;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class PostgreSQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(PostgreSQLConnector.class);
  private final DBI dbi;

  @Inject
  public PostgreSQLConnector(Supplier<MetadataDbConnectorConfig> config, Supplier<MetadataTablesConfig> dbTables)
  {
    super(config, dbTables);
    this.dbi = new DBI(getDatasource());

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
              List<Map<String, Object>> table = handle.select(String.format("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE '%s'", tableName));
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
            "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, created_date TEXT NOT NULL, "
            + "start TEXT NOT NULL, \"end\" TEXT NOT NULL, partitioned SMALLINT NOT NULL, version TEXT NOT NULL, "
            + "used BOOLEAN NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));" +
            "CREATE INDEX ON %1$s(dataSource);"+
            "CREATE INDEX ON %1$s(used);",
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
            "CREATE TABLE %1$s (id VARCHAR(255) NOT NULL, dataSource VARCHAR(255) NOT NULL, version TEXT NOT NULL, payload bytea NOT NULL, PRIMARY KEY (id));"+
            "CREATE INDEX ON %1$s(dataSource);",
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
            "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload bytea NOT NULL, PRIMARY KEY(name))",
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
          "CREATE TABLE %1$s (\n"
          + "  id varchar(255) NOT NULL,\n"
          + "  created_date TEXT NOT NULL,\n"
          + "  datasource varchar(255) NOT NULL,\n"
          + "  payload bytea NOT NULL,\n"
          + "  status_payload bytea NOT NULL,\n"
          + "  active SMALLINT NOT NULL DEFAULT '0',\n"
          + "  PRIMARY KEY (id)\n"
          + ");\n" +
          "CREATE INDEX ON %1$s(active, created_date);",
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
            "CREATE TABLE %1$s (\n"
            + "  id bigserial NOT NULL,\n"
            + "  task_id varchar(255) DEFAULT NULL,\n"
            + "  log_payload bytea,\n"
            + "  PRIMARY KEY (id)\n"
            + ");\n"+
            "CREATE INDEX ON %1$s(task_id);",
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
            "CREATE TABLE %1$s (\n"
            + "  id bigserial NOT NULL,\n"
            + "  task_id varchar(255) DEFAULT NULL,\n"
            + "  lock_payload bytea,\n"
            + "  PRIMARY KEY (id)\n"
            + ");\n"+
            "CREATE INDEX ON %1$s(task_id);",
            tableName
        )
    );
  }

  public String insertOrUpdateStatement(final String tableName, final String keyColumn, final String valueColumn)
  {
    return String.format(
      "BEGIN;\n" +
      "LOCK TABLE %1$s IN SHARE ROW EXCLUSIVE MODE;\n" +
      "WITH upsert AS (UPDATE %1$s SET %3$s=:value WHERE %2$s=:key RETURNING *)\n" +
      "    INSERT INTO %1$s (%2$s, %3$s) SELECT :key, :value WHERE NOT EXISTS (SELECT * FROM upsert)\n;" +
      "COMMIT;",
      tableName, keyColumn, valueColumn
    );
  }

  public DBI getDBI() { return dbi; }
}
