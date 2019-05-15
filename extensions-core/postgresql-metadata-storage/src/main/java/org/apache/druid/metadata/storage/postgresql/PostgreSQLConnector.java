/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.metadata.storage.postgresql;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

public class PostgreSQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(PostgreSQLConnector.class);
  private static final String PAYLOAD_TYPE = "BYTEA";
  private static final String SERIAL_TYPE = "BIGSERIAL";
  private static final String QUOTE_STRING = "\\\"";
  private static final String PSQL_SERIALIZATION_FAILURE_MSG =
      "ERROR: could not serialize access due to concurrent update";
  private static final String PSQL_SERIALIZATION_FAILURE_SQL_STATE = "40001";
  public static final int DEFAULT_STREAMING_RESULT_SIZE = 100;

  private final DBI dbi;

  private volatile Boolean canUpsert;

  private final String dbTableSchema;
  
  @Inject
  public PostgreSQLConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      PostgreSQLConnectorConfig connectorConfig,
      PostgreSQLTablesConfig tablesConfig
  )
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    // PostgreSQL driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.postgresql.Driver");

    // SSL Configuration
    if (connectorConfig.isUseSSL()) {
      log.info("SSL is enabled on this PostgreSQL connection.");
      datasource.addConnectionProperty(PGProperty.SSL.getName(), String.valueOf(connectorConfig.isUseSSL()));

      if (connectorConfig.getPassword() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_PASSWORD.getName(), connectorConfig.getPassword());
      }
      if (connectorConfig.getSslFactory() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_FACTORY.getName(), connectorConfig.getSslFactory());
      }
      if (connectorConfig.getSslFactoryArg() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_FACTORY_ARG.getName(), connectorConfig.getSslFactoryArg());
      }
      if (connectorConfig.getSslMode() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_MODE.getName(), connectorConfig.getSslMode());
      }
      if (connectorConfig.getSslCert() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_CERT.getName(), connectorConfig.getSslCert());
      }
      if (connectorConfig.getSslKey() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_KEY.getName(), connectorConfig.getSslKey());
      }
      if (connectorConfig.getSslRootCert() != null) {
        datasource.addConnectionProperty(PGProperty.SSL_ROOT_CERT.getName(), connectorConfig.getSslRootCert());
      }
      if (connectorConfig.getSslHostNameVerifier() != null) {
        datasource.addConnectionProperty(
            PGProperty.SSL_HOSTNAME_VERIFIER.getName(),
            connectorConfig.getSslHostNameVerifier()
        );
      }
      if (connectorConfig.getSslPasswordCallback() != null) {
        datasource.addConnectionProperty(
            PGProperty.SSL_PASSWORD_CALLBACK.getName(),
            connectorConfig.getSslPasswordCallback()
        );
      }
    }

    this.dbi = new DBI(datasource);
    this.dbTableSchema = tablesConfig.getDbTableSchema();
    
    log.info("Configured PostgreSQL as metadata storage");
  }

  @Override
  protected String getPayloadType()
  {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public String getQuoteString()
  {
    return QUOTE_STRING;
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
      canUpsert = metaData.getDatabaseMajorVersion() > 9 ||
                  (metaData.getDatabaseMajorVersion() == 9 && metaData.getDatabaseMinorVersion() >= 5);
    }
    return canUpsert;
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = :dbTableSchema AND tablename ILIKE :tableName"
    )
                  .bind("dbTableSchema", dbTableSchema)
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
  )
  {
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            if (canUpsert(handle)) {
              handle.createStatement(
                  StringUtils.format(
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
                  StringUtils.format(
                      "BEGIN;\n"
                      +
                      "LOCK TABLE %1$s IN SHARE ROW EXCLUSIVE MODE;\n"
                      +
                      "WITH upsert AS (UPDATE %1$s SET %3$s=:value WHERE %2$s=:key RETURNING *)\n"
                      +
                      "    INSERT INTO %1$s (%2$s, %3$s) SELECT :key, :value WHERE NOT EXISTS (SELECT * FROM upsert)\n;"
                      +
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
  public boolean compareAndSwap(List<MetadataCASUpdate> updates)
  {
    try {
      return super.compareAndSwap(updates);
    }
    catch (CallbackFailedException cfe) {
      Throwable root = Throwables.getRootCause(cfe);
      if (checkRootCauseForPSQLSerializationFailure(root)) {
        return false;
      } else {
        throw cfe;
      }
    }
  }

  /**
   * Used by compareAndSwap to check if the transaction was terminated because of concurrent updates.
   *
   * The parent implementation's compareAndSwap transaction has isolation level REPEATABLE_READ.
   * In Postgres, such transactions will be canceled when another transaction commits a conflicting update:
   * https://www.postgresql.org/docs/10/transaction-iso.html#XACT-REPEATABLE-READ
   *
   * When this occurs, we need to retry the transaction from the beginning: by returning false in compareAndSwap,
   * the calling code will attempt retries.
   */
  private boolean checkRootCauseForPSQLSerializationFailure(
      Throwable root
  )
  {
    if (root instanceof PSQLException) {
      PSQLException psqlException = (PSQLException) root;
      return PSQL_SERIALIZATION_FAILURE_SQL_STATE.equals(psqlException.getSQLState()) &&
             PSQL_SERIALIZATION_FAILURE_MSG.equals(psqlException.getMessage());
    } else {
      return false;
    }
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  protected boolean connectorIsTransientException(Throwable e)
  {
    if (e instanceof SQLException) {
      final String sqlState = ((SQLException) e).getSQLState();
      // limited to errors that are likely to be resolved within a few retries
      // retry on connection errors and insufficient resources
      // see http://www.postgresql.org/docs/current/static/errcodes-appendix.html for details
      return sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("53"));
    }
    return false;
  }
}
