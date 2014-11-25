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

package io.druid.metadata.storage.postgresql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

public class PostgreSQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(PostgreSQLConnector.class);
  private static final String PAYLOAD_TYPE = "BYTEA";
  private static final String SERIAL_TYPE = "BIGSERIAL";

  private final DBI dbi;

  @Inject
  public PostgreSQLConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    // PostgreSQL driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.postgresql.Driver");

    this.dbi = new DBI(datasource);
  }

  @Override
  protected String getPayloadType() {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename LIKE :tableName"
    )
                 .bind("tableName", tableName)
                 .map(StringMapper.FIRST)
                 .list()
                 .isEmpty();
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
            handle.createStatement(
                String.format(
                    "BEGIN;\n" +
                    "LOCK TABLE %1$s IN SHARE ROW EXCLUSIVE MODE;\n" +
                    "WITH upsert AS (UPDATE %1$s SET %3$s=:value WHERE %2$s=:key RETURNING *)\n" +
                    "    INSERT INTO %1$s (%2$s, %3$s) SELECT :key, :value WHERE NOT EXISTS (SELECT * FROM upsert)\n;" +
                    "COMMIT;",
                    tableName,
                    keyColumn,
                    valueColumn
                )
            )
                  .bind("key", key)
                  .bind("value", value)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public DBI getDBI() { return dbi; }
}
