/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata.storage.postgresql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class PostgreSQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(PostgreSQLConnector.class);
  private static final String PAYLOAD_TYPE = "BYTEA";
  private static final String SERIAL_TYPE = "BIGSERIAL";
  public static final int DEFAULT_STREAMING_RESULT_SIZE = 100;

  private final DBI dbi;

  private volatile Boolean canUpsert;

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

    log.info("Configured PostgreSQL as metadata storage");
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
  protected int getStreamingFetchSize()
  {
    return DEFAULT_STREAMING_RESULT_SIZE;
  }

  protected boolean canUpsert(Handle handle) throws SQLException
  {
    if (canUpsert == null) {
      DatabaseMetaData metaData = handle.getConnection().getMetaData();
      canUpsert = metaData.getDatabaseMajorVersion() > 9 || (
          metaData.getDatabaseMajorVersion() == 9 &&
          metaData.getDatabaseMinorVersion() >= 5
      );
    }
    return canUpsert;
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename ILIKE :tableName"
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
            if (canUpsert(handle)) {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value) ON CONFLICT (%2$s) DO UPDATE SET %3$s = EXCLUDED.%3$s",
                      tableName,
                      keyColumn,
                      valueColumn
                  )
              )
                    .bind("key", key)
                    .bind("value", value)
                    .execute();
            } else {
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
            }
            return null;
          }
        }
    );
  }

  @Override
  public DBI getDBI() { return dbi; }

  @Override
  protected boolean connectorIsTransientException(Throwable e)
  {
    if(e instanceof SQLException) {
      final String sqlState = ((SQLException) e).getSQLState();
      // limited to errors that are likely to be resolved within a few retries
      // retry on connection errors and insufficient resources
      // see http://www.postgresql.org/docs/current/static/errcodes-appendix.html for details
      return sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("53"));
    }
    return false;
  }
}
